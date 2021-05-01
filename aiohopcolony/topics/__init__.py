import aiohopcolony
from .queue import *
from .exchange import *
import pika
import asyncio
import logging
import threading
from signal import SIGINT, SIGTERM

_logger = logging.getLogger(__name__)


def connection(project=None):
    if not project:
        project = aiohopcolony.get_project()
    if not project:
        raise aiohopcolony.ConfigNotFound(
            "Hop Config not found. Run 'hopctl login' or place a .hop.config file here.")
    if not project.config.project:
        raise aiohopcolony.ConfigNotFound(
            "You have no projects yet. Create one at https://console.hopcolony.io")

    return HopTopicConnection(project)


class HopTopicConnection:
    subscriptions = []

    def __init__(self, project):
        self.project = project

        self.host = "topics.hopcolony.io"
        self.port = 32012
        self.credentials = pika.PlainCredentials(
            self.project.config.identity, self.project.config.token)
        self.parameters = pika.ConnectionParameters(host=self.host, port=self.port,
                                                    virtual_host=self.project.config.identity, credentials=self.credentials)

        self.loops = {}

    def queue(self, name):
        return HopTopicQueue(self.add_subscription, self.parameters, binding=name, name=name)

    def exchange(self, name, create=False):
        return HopTopicExchange(self.add_subscription, self.parameters, name, create, type=ExchangeType.FANOUT)

    def topic(self, name):
        return HopTopicQueue(self.add_subscription, self.parameters, exchange="amq.topic", binding=name)

    def add_subscription(self, conn):
        self.subscriptions.append(conn)

    def signal_handler(self, sig):
        for thread, loop in self.loops.items():
            loop.stop()
            if thread is threading.main_thread():
                _logger.info("Gracefully shutting down")
                loop.remove_signal_handler(SIGTERM)
                loop.add_signal_handler(SIGINT, lambda: None)

    def spin(self, loop=None):
        if not loop:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError as e:
                raise RuntimeError(f"""[ERROR] {e}.""")

        current_thread = threading.current_thread()
        self.loops[current_thread] = loop

        if current_thread is threading.main_thread():
            # Signal handling only works for main thread
            for sig in (SIGTERM, SIGINT):
                loop.add_signal_handler(sig, self.signal_handler, sig)

        loop.run_forever()

        self.close()
        tasks = asyncio.all_tasks(loop=loop)
        for t in tasks:
            t.cancel()
        group = asyncio.gather(*tasks, return_exceptions=True)
        loop.run_until_complete(group)
        loop.close()

    async def close(self):
        for subscription in self.subscriptions:
            await subscription.cancel()
        self.subscriptions.clear()
