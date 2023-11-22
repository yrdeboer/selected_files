import time
import logging
import pprint

import util.log_config
from dataclient.dataclient import DataClient
from util.handler_responses import HandlerResponse
import util.handler_responses
import util.auth
import util.param_validation


logger = logging.getLogger(__name__)


def check_request(aws_request, gsmg_context):

    util.param_validation.check_request_exchange_name_in_query(
        aws_request,
        gsmg_context)


def get_sql(gsmg_context):

    ts_min_str = util.gen.unix_time_secs_to_utc_timestamp_str(
        time.time() - 2 * 24 * 3600,
        util.gen.TIME_FORMAT_ISO8601_NO_FRAC_NO_TZ_UTC)

    sql = ''
    sql += 'SELECT '
    sql += '  m.name, '
    sql += '  m.base_currency, '
    sql += '  m.target_currency,'
    sql += '  m.exchange_comments,'
    sql += '  m.is_active,'
    sql += '  m.ts_last_active_utc, '
    sql += '  mk2.mtp_pct, '
    sql += '  mk2.wiggle1_pct, '
    sql += '  mk2.wiggle2_pct '
    sql += 'FROM '
    sql += '  markets m LEFT JOIN market_knowledge_2 mk2 '
    sql += 'ON '
    sql += '  m.name = mk2.market_name '
    sql += 'WHERE '
    sql += '  m.name LIKE CONCAT("{}", ":%") AND '.format(
        gsmg_context['query']['exchange'])
    sql += '  m.ts_last_active_utc > "{}";'.format(ts_min_str)
    logger.debug('sql={}'.format(sql))
    return sql


def sql_to_records_from_db(sql, data_client):

    result, exception = data_client.run_query(sql)

    if exception is not None:
        logger.error(
            'Got exception getting market data. '
            'Bailing. exception={}'.format(exception))
        raise HandlerResponse(
            util.handler_responses.INTERNAL_SERVER_ERROR_UNKNOWN)

    records = result.get('records', None)
    if records is None:
        records = list()

    logger.debug('Got {} records from DB.'.format(len(records)))

    return records


def records_to_market_data_raw(records):

    schema = [
        'market_name',
        'base_currency',
        'target_currency',
        'exchange_comments',
        'is_active',
        'ts_last_active_utc',
        'mtp_pct',
        'wiggle1_pct',
        'wiggle2_pct'
    ]
    raw = util.format_query.format_multiple_queries(
        schema,
        records)

    return raw


def raw_to_cleaned_markets_data(raw):

    logger.debug('Going to clean {} raw dicts'.format(len(raw)))

    cleaned = list()
    for r in raw:

        market_name = r.get('market_name', None)
        if market_name is None:
            continue

        if ':' not in market_name:
            continue

        ts = r.get('ts_last_active_utc', None)
        if ts is None or ts == '':
            continue

        c = dict()
        c['base'] = r['target_currency']
        c['quote'] = r['base_currency']
        c['exchange'] = market_name.split(':')[0]

        if not ts.endswith('Z'):
            c['ts_last_active'] = ts + 'Z'

        if int(r.get('is_active', 0)) == 1:
            c['is_active'] = True
        else:
            c['is_active'] = False

        ec = r.get('exchange_comments', None)
        if ec is None or ec == '':
            c['exchange_comments'] = None
        else:
            c['exchange_comments'] = ec

        c['gsmg_opt_mtp_pct'] = r['mtp_pct']
        c['gsmg_opt_trail_threshold_pct'] = r['wiggle1_pct']
        c['gsmg_opt_trail_distance_pct'] = r['wiggle2_pct']

        cleaned.append(c)

    logger.debug(
        'GET /markets data returning data '
        'for {} markets.'.format(len(cleaned)))

    return cleaned


def get_raise_markets_data(aws_request, data_client, gsmg_context):

    sql = get_sql(gsmg_context)
    records = sql_to_records_from_db(sql, data_client)
    raw = records_to_market_data_raw(records)
    cleaned = raw_to_cleaned_markets_data(raw)

    raise HandlerResponse(
        util.handler_responses.SUCCESS_OK,
        cleaned)


def handler(aws_request, aws_context):
    """
    """

    logger.debug(
        'GET /markets handler '
        'aws_request={} aws_context={}'.format(
            aws_request,
            aws_context))

    gsmg_context = util.gen.get_init_gsmg_context()
    try:
        data_client = DataClient(aws_request)
        check_request(aws_request, gsmg_context)
        util.auth.check_integrity(
            aws_request,
            data_client,
            gsmg_context)
        get_raise_markets_data(
            aws_request,
            data_client,
            gsmg_context)

    except HandlerResponse as hr:
        return hr.response

    except Exception as ex:
        logger.error(
            'Caught exception GET /markets, '
            'bailing. type={} ex={}'.format(
                type(ex),
                ex))

    return HandlerResponse(
        util.handler_responses.INTERNAL_SERVER_ERROR_UNKNOWN).response
