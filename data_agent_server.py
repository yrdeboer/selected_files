import asyncio
import logging
import multiprocessing

from Orchestrator import settings, utils
from Orchestrator.data_agent import data_agent_gen
from Orchestrator.data_agent import data_agent_database


CYCLE_TIME_CHECK_SELECTOR_SECS = 10


add_stdout_handler = True
logger = utils.get_logger(
    None,
    '/var/log/gsmg/data_agent_server.log',
    logging.INFO,
    None,
    add_stdout_handler)


class DataAgentServer:

    def __init__(self):
        logger.debug('DataAgentServer __init__')
        self._host = settings.DATA_AGENT_SERVER_NAME
        self._port = settings.DATA_AGENT_SERVER_PORT
        self._data = None
        self._server = None

    async def _start_server(self):

        logger.info('Starting server')

        try:

            logger.info('Starting server at {}:{}'.format(
                self._host,
                self._port))

            self._server = await asyncio.start_server(
                self.handler,
                host=self._host,
                port=self._port)

        except Exception as ex:
            logger.warning(
                'Got exception starting server at {}:{}. '
                'ex={}. Trying again in a bit.'.format(
                    self._host,
                    self._port,
                    ex))
            await asyncio.sleep(1)

        logger.info('Server started at {}:{}'.format(
            self._host,
            self._port))

    def _start_database_agent(self):

        logger.info('Starting database_agent')

        queue = multiprocessing.Queue(maxsize=100)

        database_agent = multiprocessing.Process(
            target=data_agent_database.main,
            args=(queue,),
            daemon=False)
        database_agent.start()
        logger.info('Data base agent started')

        self._database_agent = database_agent
        self._database_queue = queue

    async def close(self):

        if getattr(self, '_server', None) is not None:
            logger.info('Stopping server')
            self._server.close()
            await self._server.wait_closed()
            logger.info('Server stopped')

        if getattr(self, '_database_agent', None) is not None:
            logger.info('Going to terminate and join the database agent ...')
            self._database_agent.terminate()
            self._database_agent.join()
            logger.info('Database agent joined')

    def _update_from_database_agent(self):

        logger.info(
            'Getting data from database_agent, if any ...')

        q = self._database_queue

        if q is None:
            return

        while not self._database_queue.empty():

            try:
                key_list = q.get()
                logger.debug('Got key list from queue: {}'.format(key_list))
                data = q.get()
                logger.debug('Got key data from queue: type={}'.format(
                    type(data)))

                key_b64_bytes = data_agent_gen.json_to_base64_bytes(key_list)
                key_b64_bytes = data_agent_gen.append_os_linesep_to_bytes(
                    key_b64_bytes)
                data_b64_bytes = data_agent_gen.json_to_base64_bytes(data)
                data_b64_bytes = data_agent_gen.append_os_linesep_to_bytes(
                    data_b64_bytes)

                if self._data is None:
                    self._data = dict()

                self._data[key_b64_bytes] = data_b64_bytes

                logger.info('Added data:')
                logger.info('  key_list={}'.format(key_list))
                logger.info('  key_b64_bytes={}'.format(key_b64_bytes))
                logger.info('  data_b64_bytes={} bytes'.format(
                    len(data_b64_bytes)))

            except Exception as ex:
                logger.error(
                    'Exception getting data from '
                    'data base agent queue. Assuming data base agent '
                    'gone. Terminating its process and re-raising. '
                    'ex={}'.format(ex))
                self._database_agent.terminate()
                raise

    async def main(self):

        logger.info('DataAgentServer.main_async, setting up server')
        while True:
            try:
                self._start_database_agent()

                while self._data is None:
                    logger.info(
                        'No data yet from database agent, sleeping '
                        'a bit before fetching and serving.')
                    await asyncio.sleep(5)
                    self._update_from_database_agent()

                await self._start_server()

                while True:
                    logger.info(
                        'Going to sleep {} seconds until next check '
                        'on database pipe.'.format(
                            CYCLE_TIME_CHECK_SELECTOR_SECS))
                    await asyncio.sleep(
                        CYCLE_TIME_CHECK_SELECTOR_SECS)
                    self._update_from_database_agent()

            except Exception as ex:
                logger.debug(
                    'Got exception from serving: {}:{} '
                    'Reraising ex={} type(ex)={}'.format(
                        self._host,
                        self._port,
                        ex,
                        type(ex)))
                await self.close()
                await asyncio.sleep(1)
                raise

        await self.close()

    async def _stop_server(self, writer):

        logger.debug('Setting stop flag on server (from handler)')
        self._stop = True

    async def handler(self, reader, writer):

        try:
            key_b64_bytes = await reader.readline()
            logger.info('Handler, key={}'.format(key_b64_bytes))

            data_b64_bytes = self._data[key_b64_bytes]
            logger.info('Handler, data={} bytes'.format(
                len(data_b64_bytes)))

            try:
                writer.write(data_b64_bytes)
                await writer.drain()

                logger.info(
                    'Done sending {} bytes of data for key {}.'.format(
                        len(data_b64_bytes),
                        key_b64_bytes))

            except Exception as ex:
                logger.warning(
                    'Got exception writing/draining data '
                    'for key_b64_bytes={}. Ignoring. '
                    'ex={}'.format(key_b64_bytes, ex))

        except Exception as ex:
            logger.warning(
                'Got exception reading key from client or '
                'fetching data by key. '
                'Ignoring, but check this if possible. '
                'Closing stream writer. '
                'type(ex)={} ex={}'.format(type(ex), ex))

        writer.close()


async def main():

    server = DataAgentServer()
    await server.main()


if __name__ == '__main__':
    logger.info('data_agent_server starting')
    asyncio.get_event_loop().run_until_complete(main())
    logger.info('data_agent_server done')
