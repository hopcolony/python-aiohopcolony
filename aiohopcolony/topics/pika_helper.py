import asyncio
import pika
import json
import inspect
import logging
from dataclasses import dataclass
from pika.adapters.asyncio_connection import AsyncioConnection
from concurrent.futures import ThreadPoolExecutor as Executor
from enum import Enum

_logger = logging.getLogger(__name__)


class OutputType(Enum):
    BYTES = 1
    STRING = 2
    JSON = 3


class Subscription(object):
    _canceled = False
    _tasks = []

    def __init__(self, channel, callback, output_type):
        self.channel = channel
        self.channel.add_on_cancel_callback(
            lambda reason: self.channel.close())

        self.loop = channel.loop

        self.executor = Executor()
        self.loop.set_default_executor(self.executor)

        self.callback = callback
        self.output_type = output_type

    def on_message(self, ch, method, props, body):
        self.channel.basic_ack(method.delivery_tag)
        if self.output_type == OutputType.BYTES:
            out = body
        elif self.output_type == OutputType.STRING:
            out = body.decode('utf-8')
        elif self.output_type == OutputType.JSON:
            try:
                out = json.loads(body.decode('utf-8'))
            except json.decoder.JSONDecodeError as e:
                _logger.error(
                    f"[ERROR] Malformed json message received on topic \"{method.routing_key}\": {body}")
                return

        if inspect.iscoroutinefunction(self.callback):
            self.loop.create_task(self.callback(out))
        else:
            self.loop.run_in_executor(self.executor, self.callback, out)

    def start(self, queue_name):
        self.consumer_tag = self.channel.basic_consume(
            queue_name, self.on_message)

    def add_task_to_wait(self, task):
        self._tasks.append(task)

    async def cancel(self):
        if not self._canceled:
            if self.executor:
                # Wait for the pending threads to finish
                self.executor.shutdown(wait=True)
            await asyncio.gather(*self._tasks, return_exceptions=True)
            await self.channel.stop(self.consumer_tag)
            await self.channel.close(self.loop)
            self._canceled = True


@dataclass
class PikaConnection(object):
    connection: AsyncioConnection
    loop: asyncio.unix_events._UnixSelectorEventLoop
    add_subscription = None

    @classmethod
    async def create(cls, parameters, loop=None):
        if not loop:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # If there is no event loop in the thread, create one abd set it
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

        future_conn = asyncio.Future()

        def on_connection_open(conn):
            loop.call_soon_threadsafe(future_conn.set_result, conn)

        def on_connection_open_error(_, reason):
            loop.call_soon_threadsafe(future_conn.set_result, reason)

        AsyncioConnection(
            parameters=parameters,
            on_open_callback=on_connection_open,
            on_open_error_callback=on_connection_open_error)
        conn = await future_conn
        if isinstance(conn, pika.adapters.utils.connection_workflow.AMQPConnectionWorkflowFailed):
            raise Exception(conn)

        return cls(conn, loop)

    async def new_channel(self, loop=None):
        if not loop:
            loop = self.loop

        return await PikaChannel.create(self.connection, loop=loop)

    def channel(self, *args, **kwargs):
        self.connection.channel(*args, **kwargs)

    async def subscribe(self, exchange, binding, queue_declaration, exchange_declaration, callback, output_type):
        channel = await self.new_channel()
        subscription = Subscription(channel, callback, output_type)

        if exchange_declaration is not None:
            # Create the exchange
            await channel.exchange_declare(exchange_declaration)

        # Create the queue
        queue = await channel.queue_declare(queue_declaration)
        queue_name = queue.method.queue

        if exchange:
            await channel.queue_bind(queue_name, exchange, binding)

        # Start consuming
        subscription.start(queue_name)
        self.add_subscription(subscription)
        return subscription

    async def close(self, loop=None):
        if self.connection:
            if not loop:
                loop = self.loop

            connection_closed = asyncio.Future()
            self.add_on_close_callback(
                lambda channel, reason: loop.call_soon(connection_closed.set_result, True))
            self.connection.close()
            await connection_closed

    def add_on_close_callback(self, callback):
        self.connection.add_on_close_callback(callback)


class PikaChannel(object):
    channel: pika.channel.Channel
    id: str
    loop: asyncio.unix_events._UnixSelectorEventLoop

    future_queue_bind: asyncio.Future
    future_channel_closed: asyncio.Future

    def __init__(self, connection, channel, id, loop):
        self.connection = connection
        self.channel = channel
        self.id = id
        self.loop = loop

        self.add_on_close_callback(lambda channel, reason: asyncio.create_task(
            self.on_channel_closed(channel, reason)))
        self.future_queue_bind = asyncio.Future()
        self.future_channel_closed = asyncio.Future()

    @classmethod
    async def create(cls, connection, id=None, loop=None):
        if not loop:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # If there is no event loop in the thread, create one abd set it
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

        future_channel = asyncio.Future()
        connection.channel(on_open_callback=lambda channel: loop.call_soon(
            future_channel.set_result, channel))

        channel = await future_channel

        return cls(connection, channel, id, loop)

    async def on_channel_closed(self, channel, reason):
        if reason.reply_code == 404:
            # Remove the unused queue with a new channel (this one is closed)
            new_channel = await PikaChannel.create(self.connection)
            await new_channel.queue_delete(self.name)
            await new_channel.close()
            # Return the error
            self.loop.call_soon(self.future_queue_bind.set_result, reason)
        else:
            self.loop.call_soon(self.future_channel_closed.set_result, reason)

    async def exchange_declare(self, declaration, loop=None):
        if not loop:
            loop = self.loop

        future_exchange = asyncio.Future()
        self.channel.exchange_declare(**declaration,
                                      callback=lambda exchange: loop.call_soon(future_exchange.set_result, exchange))
        return await future_exchange

    async def exchange_delete(self, name, loop=None):
        if not loop:
            loop = self.loop

        future_exchange_deletion = asyncio.Future()
        self.channel.exchange_delete(name,
                                     callback=lambda queue: loop.call_soon(future_exchange_deletion.set_result, True))
        return await future_exchange_deletion

    async def queue_declare(self, declaration, loop=None):
        if not loop:
            loop = self.loop

        future_queue = asyncio.Future()
        self.channel.queue_declare(**declaration,
                                   callback=lambda queue: loop.call_soon(future_queue.set_result, queue))
        return await future_queue

    async def queue_delete(self, name, loop=None):
        if not loop:
            loop = self.loop

        future_queue_deletion = asyncio.Future()
        self.channel.queue_delete(name,
                                  callback=lambda queue: loop.call_soon(future_queue_deletion.set_result, True))
        return await future_queue_deletion

    async def queue_bind(self, queue, exchange, binding, loop=None):
        if not loop:
            loop = self.loop

        self.name = queue
        self.channel.queue_bind(queue=queue, exchange=exchange, routing_key=binding,
                                callback=lambda queue: loop.call_soon(self.future_queue_bind.set_result, True))
        bind = await self.future_queue_bind
        if isinstance(bind, pika.exceptions.ChannelClosedByBroker):
            raise Exception(bind)
        return bind

    def basic_consume(self, *args, **kwargs):
        self.channel.basic_consume(*args, **kwargs)

    async def send(self, exchange, routing_key, body, exchange_declaration=None):
        if exchange_declaration is not None:
            await self.exchange_declare(exchange_declaration)
        if isinstance(body, dict):
            body = json.dumps(body)
        self.channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=body)
        return self

    def basic_ack(self, *args, **kwargs):
        self.channel.basic_ack(*args, **kwargs)

    def basic_cancel(self, *args, **kwargs):
        self.channel.basic_cancel(*args, **kwargs)

    async def stop(self, consumer_tag, loop=None):
        if not loop:
            loop = self.loop

        if self.channel and consumer_tag:
            consumer_stopped = asyncio.Future()
            self.basic_cancel(consumer_tag, callback=lambda _: loop.call_soon(
                consumer_stopped.set_result, True))
            await consumer_stopped

    async def close(self, loop=None):
        if self.channel:
            if not loop:
                loop = self.loop

            self.channel.close()
            await self.future_channel_closed

    def add_on_close_callback(self, callback):
        self.channel.add_on_close_callback(callback)

    def add_on_cancel_callback(self, callback):
        self.channel.add_on_cancel_callback(callback)

    @property
    def is_closed(self):
        return self.channel.is_closed

    @property
    def is_closing(self):
        return self.channel.is_closing
