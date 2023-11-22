import logging
import traceback
import time
import asyncio
import uvloop
import pprint
import random
import tracemalloc
from concurrent.futures import ALL_COMPLETED

from Orchestrator import db, settings, utils
from Orchestrator import trade_engine
from Orchestrator.data_agent import data_agent, data_agent_gen


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
logger = logging.getLogger(__name__)

DO_MEM_CHECKING = False

SQL_ACTIVE_SUBS = ''
SQL_ACTIVE_SUBS += '('
SQL_ACTIVE_SUBS += '  (users.subscription_name in ({})) OR '
SQL_ACTIVE_SUBS += '  (users.subscription_name in ({}) AND '
SQL_ACTIVE_SUBS += '   users.credit_gwei > 0) OR '
SQL_ACTIVE_SUBS += '  (users.subscription_name in ({}) AND '
SQL_ACTIVE_SUBS += '   users.subscription_expiry_date_utc > {}) '
SQL_ACTIVE_SUBS += ') '

# This is the number of seconds after which the
# async processing of user_id/exchange is
# halted to update user data and shared data etc.
# Changed to 30 seconds in December 2019 to have
# the lasts updated more-often.
TIME_OUT_WAIT_SECS = 30
CACHE_REFRESH_MARKET_DATA_SECS = 90


class CoroutineTracker:

    """
    This class maintains the running coroutines in the
    event loop (below, see "run" function).

    It provides an interface to query the user id
    for a coroutine. This is useful if a coroutine
    does not finish properly, there is no other way
    (that I know of) to find back for which user id
    the coroutine was running.

    It also provides an interface to allows it to query
    whether a coroutine should be run for a user
    or not (yet).

    The data structure that tracks this has
    the following format, where the keys are
    the user ids:

    {
      10: {
             ts_last_added: 1519644277.7618508
             ts_last_returned: 151964428.7618508
          },
      ...
    }

    Also the following dict is maintained for
    the reverse lookup. Key is the coroutine hash
    and the value is the user id:
    {
      8764921962748: 10
    }
    """

    _user_timing_dict = dict()
    _coro_hash_user_dict = dict()

    def get_user_id_for_coro(self, coro):
        """
        Returns the user id for given coroutine.
        This is necessary for when coroutines ended
        in exceptions and the return value is not know.
        """

        return self._coro_hash_user_dict.get(hash(coro), None)

    def notarise_coro_added(self, coro, user_id):
        """
        This function records that for user_id
        the coroutine coro is added to the pending
        coroutines and assumed to start soon.
        """

        try:
            d = self._user_timing_dict[user_id]
        except KeyError:
            d = dict()
            self._user_timing_dict[user_id] = d

        d['ts_last_added'] = time.time()

        self._coro_hash_user_dict[hash(coro)] = user_id

        logger.debug(
            'notarised coro added for user_id={}: {}'.format(
                user_id,
                d))

    def notarise_coro_returned_get_user_id(self, coro):
        """
        This function marks down the time stamp of
        a given coroutine.

        If it cannot find the coroutine in its bookkeeping,
        then a warning is issued to go fix that coding
        error.

        Returns the user_id or None if that cannot be found.
        """

        user_id = self._coro_hash_user_dict.pop(hash(coro), None)

        logger.debug(
            'coro returned, got user_id={}'.format(user_id))

        if user_id is None:

            msg = 'Cannot find user id for returned '
            msg += 'coroutine: {}. '.format(coro)
            msg += 'This is a coding error and should be fixed. '
            msg += 'Notify bloctite please.'
            logger.warning(msg)

        else:

            d = self._user_timing_dict.get(user_id, None)

            if d is None:
                msg = 'Cannot find user id {} for returned '.format(user_id)
                msg += 'coroutine: {}. '.format(coro)
                msg += 'This is a coding error and should be fixed. '
                msg += 'Notify bloctite please.'
                logger.warning(msg)

            else:
                d['ts_last_returned'] = time.time()
                logger.debug(
                    'Updated _user_timing_dict to: {}'.format(
                        d))

        return user_id

    def user_is_new_or_due(self, user_id):
        """
        This function return True if the user_id is
        new or due to run, otherwise False.
        """

        new_or_due = False

        d = self._user_timing_dict.get(user_id, None)

        if d is None:

            # User is new
            new_or_due = True

        else:
            ts_last_added = d.get('ts_last_added', None)
            if ts_last_added is None:

                msg = 'Unable to find ts_last_added from timing '
                msg += 'dict for user {}. '.format(user_id)
                msg += 'This is a coding error, because '
                msg += 'if the dictionary exists, it should contain '
                msg += 'a last-added timestamp. '
                msg += 'Notify bloctite please. Setting ts_last_added '
                msg += 'to 0 for now.'
                logger.warning(msg)

                ts_last_added = 0

            ts_last_returned = d.get('ts_last_returned', 0)
            if ts_last_added > ts_last_returned:

                logger.debug(
                    'Trade engine still running for user {}'.format(
                        user_id))

                new_or_due = False

            else:

                t_now = time.time()
                dt_cycle = settings.CYCLE_SPACING_SECS
                time_overdue = t_now - ts_last_returned - dt_cycle
                if time_overdue < 0:

                    logger.debug(
                        'User {} not yet due (would-be time '
                        'left {} secs).'.format(
                            user_id,
                            abs(int(time_overdue))))

                    new_or_due = False

                else:
                    new_or_due = True

        return new_or_due


def update_db_connections(shared_data):
    """
    """

    try:
        db_connections = shared_data['db_connections']

        # On success, we can test here if they are still alive

    except KeyError:

        # No connections, add them here
        db_connections = dict()

        for db_name, db_args in settings.SHARED_DB_DATA.items():

            db_conn = db.get_db_connection(db_args)
            if db_conn is None:
                msg = 'Unable to obtain connection args '
                msg += 'for db {} from {}. '.format(
                    db_name,
                    db_args)
                msg += 'Not connecting.'
                logger.critital(msg)
                raise ValueError(msg)

            db_connections[db_name] = db_conn

            logger.info(
                'Added db connection to shared data: {}'.format(
                    db_name))

        shared_data['db_connections'] = db_connections

    return shared_data


def log_mem_usage(shared_data):

    try:
        s_list = shared_data['memory_snapshots']
    except KeyError:
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        s_list = list()
        shared_data['memory_snapshots'] = s_list

    s_list.append(tracemalloc.take_snapshot())
    if len(s_list) >= 2:
        diff = s_list[-1].compare_to(s_list[-2], 'lineno')
        logger.info('TRACEMALLOC {}:\n{}'.format(
            shared_data['globally_shared_data']['process_id'],
            pprint.pformat(diff[:15])))


def refresh_cached_market_data_for_key(key, shared_data):

    t_now = time.time()
    try:
        dict_key = key + '_dict'
        ts_key = 'ts_last_updated_utc_secs_' + key

        market_dict = shared_data[dict_key]
        ts = shared_data[ts_key]

        cache_refresh_wait_secs = CACHE_REFRESH_MARKET_DATA_SECS
        overdue = ts + cache_refresh_wait_secs - t_now
        if overdue < 0.:
            logger.info('Refreshing cached {} data for {} markets. '
                        'Overdue: {} seconds.'.format(
                            key,
                            len(market_dict.keys()),
                            -int(overdue)))

            market_dict.clear()
            shared_data[ts_key] = t_now

        else:
            logger.info('Not clearing {} cache for {} markets. '
                        'Due in {} seconds.'.format(
                            key,
                            len(market_dict.keys()),
                            int(overdue)))

    except KeyError:
        shared_data[dict_key] = dict()
        shared_data[ts_key] = t_now

    return shared_data


def refresh_cached_market_data(shared_data):
    """
    This function refreshes cached market data.
    """

    keys = ['order_price_limits']

    for key in keys:
        refresh_cached_market_data_for_key(key, shared_data)

    return shared_data


async def update_shared_data_from_data_agent(key_lists, shared_data):

    for key_list in key_lists:

        new_data = await data_agent.get_data_json_by_key_list(
            key_list,
            shared_data)

        shared_data_key = key_list[-1]
        if new_data is not None:
            logger.debug(
                'Got new data for {} from data agent. '
                'Updating shared data and return.'.format(
                    shared_data_key))

            shared_data[shared_data_key] = new_data

        else:
            logger.critical(
                'Got None for new data for key {}. '
                'Fix this, not updating shared data.'.format(
                    shared_data_key))

    return shared_data


async def update_shared_data(shared_data):
    """
    This function regularly updates the data that is shared
    between users within this process.

    Shared data is:
      market_knowledge (ath etc.)
      db_connections

    Returns:
      Updated shared_data
    """

    dt_wait_secs = settings.CYCLE_UPDATE_SHARED_DATA_MIN_WAIT_SECS
    ts_last_secs = shared_data.get('ts_last_updated_shared_data_secs', 0)
    ts_now_secs = time.time()
    dt_secs_overdue = ts_last_secs + dt_wait_secs - ts_now_secs

    if dt_secs_overdue >= 0:
        logger.debug(
            'Not yet updating all shared data. '
            'Due in {:.0f} seconds.'.format(
                dt_secs_overdue))
        return shared_data

    logger.debug(
        'Updating all shared data, overdue {:.0f} seconds'.format(
            - dt_secs_overdue))

    shared_data = update_db_connections(shared_data)
    shared_data = refresh_cached_market_data(shared_data)

    key_lists = [
        ['shared_data', 'subscriptions'],
        ['shared_data', 'trsl_missed_buys_last_ts'],
        ['shared_data', 'exchange_info'],
        ['shared_data', 'market_info'],
        ['shared_data', 'market_knowledge'],
        ['shared_data', 'market_knowledge_2'],
        ['shared_data', 'lasts_dict'],
        ['shared_data', 'users_account_value'],
        ['shared_data', 'min_max_price_rows_market_dict']
    ]
    await update_shared_data_from_data_agent(key_lists, shared_data)

    shared_data['ts_last_updated_shared_data_secs'] = ts_now_secs

    return shared_data


def purge_users_data_for_process_id(users_data, shared_data):

    process_count = shared_data['globally_shared_data']['process_count']
    process_id = shared_data['globally_shared_data']['process_id']

    for key_str in list(users_data.keys()):

        user_data = users_data.pop(key_str)

        key_tuple = data_agent_gen.tuple_key_from_str(key_str)
        if key_tuple is None:
            continue

        if len(key_tuple) != 2:
            continue

        if user_data.get('user_id', None) is None:
            continue

        if user_data['user_id'] % process_count != process_id:
            continue

        logger.debug(
            '{}/{}: Keeping key_tuple {} for processing'.format(
                process_id,
                process_count,
                key_str))

        users_data[key_tuple] = user_data

    logger.info(
        '{}/{}: Finally left with {} key_tuples for processing'.format(
            process_id,
            process_count,
            len(users_data.keys())))


async def get_users_data_from_data_agent(shared_data):

    key_list = ['users_data', 'normal']
    users_data = await data_agent.get_data_json_by_key_list(
        key_list,
        shared_data)
    purge_users_data_for_process_id(users_data, shared_data)
    return users_data


async def get_users_data_to_cancel_orders_from_data_agent(shared_data):

    key_list = ['users_data', 'cancel_all']
    users_data = await data_agent.get_data_json_by_key_list(
        key_list,
        shared_data)
    purge_users_data_for_process_id(users_data, shared_data)
    return users_data


async def get_users_data_cancel_all(shared_data):

    users_data = dict()

    ts_last = shared_data.get(
        'ts_updated_users_data_cancel_all_secs', 0)
    t_now = time.time()

    process_count = shared_data['globally_shared_data']['process_count']
    process_id = shared_data['globally_shared_data']['process_id']

    WAIT_TIME_SECS = 3 * 60
    dt = t_now - ts_last
    if dt < WAIT_TIME_SECS:
        logger.info(
            '{}/{}: Not yet updating users data '
            '(cancel_all) dt={:.1f}/{}'.format(
                process_id,
                process_count,
                dt,
                WAIT_TIME_SECS))

    else:

        logger.info(
            '{}/{}: Updating users data (cancel_all) '
            'due dt={:.1f}/{}'.format(
                process_id,
                process_count,
                dt,
                WAIT_TIME_SECS))

        f = get_users_data_to_cancel_orders_from_data_agent
        users_data = await f(shared_data)

        shared_data['ts_updated_users_data_cancel_all_secs'] = t_now

    return users_data


async def get_users_data_normal(users_data_old, shared_data):

    ts_last = shared_data.get(
        'ts_updated_users_data_normal_secs', 0)
    t_now = time.time()

    process_count = shared_data['globally_shared_data']['process_count']
    process_id = shared_data['globally_shared_data']['process_id']

    WAIT_TIME_SECS = 3 * 60
    dt = t_now - ts_last
    if dt < WAIT_TIME_SECS:
        logger.info(
            '{}/{}: Not yet updating users data (normal) '
            'dt={:.1f}/{}'.format(
                process_id,
                process_count,
                dt,
                WAIT_TIME_SECS))

        users_data = users_data_old

    else:

        logger.info(
            '{}/{}: Updating users data (normal) '
            'due dt={:.1f}/{}'.format(
                process_id,
                process_count,
                dt,
                WAIT_TIME_SECS))

        users_data = await get_users_data_from_data_agent(
            shared_data)

        shared_data['ts_updated_users_data_normal_secs'] = t_now

    return users_data


def is_stop(shared_data):
    """
    Checks DB admin settings whether to stop the trade engines.
    """

    stop = False

    try:
        db_conn = shared_data['db_connections'][settings.DB_NAME_TRADE]

    except KeyError:

        # First run there is not yet a db connection
        return stop

    sql = 'SELECT stop_tradeengines FROM admin_settings '
    sql += 'ORDER BY ts_updated_mysql_timestamp DESC LIMIT 1;'
    result = utils.execute_tenaciously_sql(sql, db_conn)

    if len(result) == 0:
        val = None
    else:
        val = result[0]

    if val is None:

        msg = 'Could not retrieve stop setting '
        msg += 'from admin settings. Assuming stop '
        msg += 'requested.'
        logger.warning(msg)

        stop = 1

    elif len(val) == 1:
        stop = val[0] == 1

    return stop


async def run_users_async(shared_data, loop):

    """
    This function runs asynchronously the trade engines for
    a number of users.

    This function regularly updates the shared data and schedules
    the trade engines per user to run.

    It also implements a graceful shutdown of the loop, when
    a stop condition is met.
    """

    logger.debug('run_users_async')

    glob_data = shared_data['globally_shared_data']
    process_count = glob_data['process_count']
    process_id = glob_data['process_id']

    coro_tracker = CoroutineTracker()
    user_data = dict()
    pending = set()

    stop = False

    while True:

        # Once the DB has flagged the stop signal,
        # then we will stop, no matter what.
        if not stop:
            stop = is_stop(shared_data)

        shared_data = await update_shared_data(shared_data)

        if stop:
            logger.info(
                '(proc id/cnt={}/{}) Stop requested. '
                'Pending tasks: {}.'.format(
                    process_id,
                    process_count,
                    len(pending)))

        else:
            user_data = await get_users_data_normal(user_data, shared_data)

            logger.info(
                '(proc id/cnt={}/{}) Got {} users.'.format(
                    process_id,
                    process_count,
                    len(user_data.keys())))

            # Schedule trade engine processing
            for user_id_ex, udat in user_data.items():

                if coro_tracker.user_is_new_or_due(user_id_ex):

                    logger.debug(
                        '(proc id/cnt={}/{}) User with '
                        'id/ex {} new or due, scheduling '
                        'for processing.'.format(
                            process_id,
                            process_count,
                            user_id_ex))

                    coro = trade_engine.run_for_user(
                        udat,
                        shared_data,
                        loop)

                    pending.add(coro)

                    coro_tracker.notarise_coro_added(coro, user_id_ex)

                else:

                    logger.debug(
                        'User/ex {} not new or not due. Not '
                        'scheduling trade engine processing.'.format(
                            user_id_ex))

            f = get_users_data_cancel_all
            user_data_cancel_orders = await f(shared_data)

            logger.info(
                '(proc id/cnt={}/{}) Got {} users to '
                'cancel their open buy orders.'.format(
                    process_id,
                    process_count,
                    len(user_data_cancel_orders.keys())))

            # Schedule cancelation of open orders
            for user_id_ex, udat in user_data_cancel_orders.items():

                if coro_tracker.user_is_new_or_due(user_id_ex):

                    logger.info(
                        '(proc id/cnt={}/{}) '
                        'User with id/ex {} new or due, scheduling '
                        'for processing to cancel all open '
                        'buy orders.'.format(
                            process_id,
                            process_count,
                            user_id_ex))

                    coro = trade_engine.cancel_orders_for_user(
                        udat,
                        shared_data)

                    pending.add(coro)

                    coro_tracker.notarise_coro_added(coro, user_id_ex)

                else:

                    logger.debug(
                        'User/ex {} not new or not due, not.'
                        'scheduling to cancel all open '
                        'buy orders.' .format(
                            user_id_ex))

        if len(pending) == 0:

            if stop:
                logger.info(
                    '(proc id/cnt={}/{}) '
                    'No pending jobs, stopping now.'.format(
                        process_id,
                        process_count))

                break

            else:
                logger.info(
                    '(proc id/cnt={}/{}) '
                    'No pending jobs, sleep 60 seconds.'.format(
                        process_id,
                        process_count))

                await asyncio.sleep(60)

        else:

            logger.info(
                '(proc id/cnt={}/{}) '
                'Going into wait '
                'with pending job count: {}'.format(
                    process_id,
                    process_count,
                    len(pending)))

            done, pending = await asyncio.wait(
                pending,
                timeout=TIME_OUT_WAIT_SECS + random.uniform(-0.1, 0.1),
                return_when=ALL_COMPLETED)

            logger.info(
                '(proc id/cnt={}/{}) '
                'Returned from wait '
                'with done job count: {} '
                'and pending job count: {}'.format(
                    process_id,
                    process_count,
                    len(done),
                    len(pending)))

            for task in done:

                coro = task._coro
                user_id_ex = coro_tracker.notarise_coro_returned_get_user_id(
                    coro)

                if user_id_ex is None:

                    logger.warning(
                        '(proc id/cnt={}/{}) '
                        'Unable to fetch user_id_ex for coro {}. '
                        'This is a programming error, because every '
                        'task that finishes, should be notarised. '
                        'Please notify Bloctite.'
                        'Trying to get the user id/ex from the task '
                        'itself ... '.format(process_id, process_count, coro))

                try:
                    user_id_ex_from_task = task.result()

                    if user_id_ex != user_id_ex_from_task:

                        msg = 'Programming error: got different '
                        msg += 'user_id_ex for '
                        msg += 'task. From tracker user_id_ex={} and '.format(
                            user_id_ex)
                        msg += 'user_id_ex_from_task={}. '.format(
                            user_id_ex_from_task)
                        msg += 'Please notify Bloctite.'
                        logger.warning(msg)

                        if user_id_ex is None:
                            user_id_ex = user_id_ex_from_task

                            logger.info(
                                '(proc id/cnt={}/{}) '
                                'Set user_id_ex to {} from '
                                'task itself.'.format(
                                    process_id, process_count, user_id_ex))

                except asyncio.CancelledError:

                    msg = '(proc id/cnt={}/{}) '.format(process_id,
                                                        process_count)
                    msg += 'Execution of trade engine cancelled for '
                    msg += 'user_id_ex={}. '.format(user_id_ex)
                    msg += 'Notify Bloctite as this is a programming error. '
                    logger.warning(msg)

                except asyncio.InvalidStateError:

                    msg = '(proc id/cnt={}/{}) '.format(process_id,
                                                        process_count)
                    msg += 'Result not yet available of trade engine for '
                    msg += 'Notify Bloctite as this is a programming error. '
                    msg += 'user_id_ex={}.'.format(user_id_ex)
                    logger.warning(msg)

                except Exception:

                    msg = '(proc id/cnt={}/{}) '.format(process_id,
                                                        process_count)
                    msg += 'Caught unhandled exception raised by trade '
                    msg += 'engine for user_id_ex={}. '.format(user_id_ex)
                    msg += 'Back trace:\n{}'.format(traceback.format_exc())
                    logger.critical(msg)

    # Release resources
    try:
        db_conns = shared_data['db_connections']
        for name, conn in db_conns.items():
            if conn is not None:
                conn.close()
                msg = 'Closed db connection for {}.'.format(name)
                logger.info(msg)

    except KeyError:
        msg = '(proc id/cnt={}/{}) '.format(process_id, process_count)
        msg += 'Unable to fetch db connections '
        msg += 'from shared data. '
        msg += 'Not closing any db connections.'
        logger.warning(msg)


def run(globally_shared_data, *args, **kwargs):
    """
    This function runs in a single process.

    It creates an event loop for asynchronous processing.

    Finally it starts the processing of user
    trade engines.

    Args:
      globally_shared_data:

    kwargs:

    Returns:
      True (not used)
    """

    shared_data = dict()
    shared_data['globally_shared_data'] = globally_shared_data

    process_count = globally_shared_data['process_count']
    process_id = globally_shared_data['process_id']

    logger.info(
        '{}/{}: Going to sleep {} (process id) seconds '
        'to introduce an offset between wait timeouts '
        'between processes.'.format(
            process_id,
            process_count,
            process_id))

    time.sleep(process_id)

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(run_users_async(shared_data, loop))
        msg = '(proc id/cnt={}/{}) Loop completed normally.'.format(
            process_id,
            process_count)
        logger.info(msg)

    except Exception as ex:
        msg = '(proc id/cnt={}/{}) Caught exception '.format(
            process_id,
            process_count)
        msg += 'from main loop: type={} ex={}. '.format(type(ex), ex)
        msg += 'Stopping processing of users. '
        msg += 'Back trace:\n{}'.format(traceback.format_exc())
        logger.critical(msg)

    finally:
        logger.info(
            '(proc id/cnt={}/{}) Shutting down any async tasks '
            'that may be pending ...'.format(process_id, process_count))
        loop.run_until_complete(loop.shutdown_asyncgens())
        logger.info(
            '(proc id/cnt={}/{}) Done shutting down any async tasks '
            'that may have been pending.'.format(process_id, process_count))

        logger.info(
            '(proc id/cnt={}/{}) Closing loop ...'.format(process_id,
                                                          process_count))
        loop.close()
        logger.info(
            '(proc id/cnt={}/{}) Closed loop'.format(process_id,
                                                     process_count))

    return True
