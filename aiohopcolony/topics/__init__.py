import aiohopcolony
from .queue import *
from .exchange import *
from .pika_helper import *
import pika
import asyncio
import logging
import threading
import inspect
from signal import SIGINT, SIGTERM

_logger = logging.getLogger(__name__)


async def connection(project=None):
    if not project:
        project = aiohopcolony.get_project()
    if not project:
        raise aiohopcolony.ConfigNotFound(
            "Hop Config not found. Run 'hopctl login' or place a .hop.config file here.")
    if not project.config.project:
        raise aiohopcolony.ConfigNotFound(
            "You have no projects yet. Create one at https://console.hopcolony.io")

    return await HopTopicConnection.create(project)


class HopTopicConnection:
    subscriptions = set()
    channels = {}

    @classmethod
    async def create(self, project):
        self = HopTopicConnection()
        self.project = project

        self.host = "topics.hopcolony.io"
        self.port = 15012
        self.credentials = pika.PlainCredentials(
            self.project.config.identity, self.project.config.token)
        self.parameters = pika.ConnectionParameters(host=self.host, port=self.port,
                                                    virtual_host=self.project.config.identity, credentials=self.credentials)
        self.connection = await PikaConnection.create(self.parameters)
        self.connection.add_subscription = self.add_subscription

        self.loops = {}
        return self

    def queue(self, name, **kwargs):
        queue = HopTopicQueue(self.connection, binding=name,
                              name=name, channel=self.channel)

        # Check if it was decorated
        lines = inspect.stack(context=2)[1].code_context
        decorated = any(line.strip().startswith('@') for line in lines)
        if decorated:
            def _subscribe(cb):
                loop = asyncio.get_event_loop()
                loop.create_task(queue.subscribe(cb, **kwargs))
            return _subscribe
        else:
            return queue

    def exchange(self, name, **kwargs):
        exchange = HopTopicExchange(self.connection, name, create=kwargs.pop(
            "create", None), type=ExchangeType.FANOUT, channel=self.channel)

        # Check if it was decorated
        lines = inspect.stack(context=2)[1].code_context
        decorated = [line for line in lines if line.strip().startswith('@')]
        if decorated:
            def _subscribe(cb):
                loop = asyncio.get_event_loop()
                loop.create_task(exchange.subscribe(cb, **kwargs))
            return _subscribe
        else:
            return exchange

    def topic(self, name, **kwargs):
        queue = HopTopicQueue(
            self.connection, exchange="amq.topic", binding=name, channel=self.channel)

        # Check if it was decorated
        lines = inspect.stack(context=2)[1].code_context
        decorated = any(line.strip().startswith('@') for line in lines)
        if decorated:
            def _subscribe(cb):
                loop = asyncio.get_event_loop()
                loop.create_task(queue.subscribe(cb, **kwargs))
            return _subscribe
        else:
            return queue

    def add_subscription(self, subscription):
        self.subscriptions.add(subscription)

    async def channel(self, name):
        if name not in self.channels:
            self.channels[name] = await PikaChannel.create(self.connection, id=name)

        return self.channels[name]

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

    async def cancel(self, subscription):
        await subscription.cancel()
        self.subscriptions.remove(subscription)

    async def close(self, channel=None):
        if channel:
            await channel.close()
            self.channels.pop(channel.id)
        else:
            for subscription in self.subscriptions:
                await subscription.cancel()

            for channel in self.channels.values():
                if not channel.is_closed and not channel.is_closing:
                    await channel.close()

            await self.connection.close()
            self.subscriptions.clear()
