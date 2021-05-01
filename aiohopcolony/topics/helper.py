import asyncio
import pika
import json
import inspect
import logging
from pika.adapters.asyncio_connection import AsyncioConnection
from concurrent.futures import ThreadPoolExecutor as Executor
from enum import Enum

_logger = logging.getLogger(__name__)


class OutputType(Enum):
    BYTES = 1
    STRING = 2
    JSON = 3


class MalformedJson(Exception):
    pass


class Subscription(object):
    def __init__(self, loop, connection, callback, output_type):
        self.loop = loop
        self.connection = connection
        self.executor = Executor()
        self.loop.set_default_executor(self.executor)

        self.callback = callback
        self.output_type = output_type

        self._tasks = []

    def on_consumer_cancelled(self, reason):
        if self.channel:
            self.channel.close()

    def add_channel(self, channel):
        self.channel = channel
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)

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
                raise MalformedJson(
                    f"Malformed json message received on topic \"{self.binding}\": {body}") from e

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
        if not self.connection.is_closing and not self.connection.is_closed:
            if self.executor:
                # Wait for the pending threads to finish
                self.executor.shutdown(wait=True)
            await asyncio.gather(*self._tasks, return_exceptions=True)

            await TopicsHelper.stop_consumer(self.loop, self.channel, self.consumer_tag)
            await TopicsHelper.close_channel(self.loop, self.channel)
            await TopicsHelper.close_connection(self.loop, self.connection)


class TopicsHelper(object):

    @classmethod
    def _get_loop(cls):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # If there is no event loop in the thread, create one abd set it
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop

    @classmethod
    async def _create_connection(cls, loop, parameters):
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
        return conn

    @classmethod
    async def _create_channel(cls, loop, conn):
        future_channel = asyncio.Future()
        conn.channel(on_open_callback=lambda channel: loop.call_soon(
            future_channel.set_result, channel))
        return await future_channel

    @classmethod
    async def _create_exchange(cls, loop, channel, exchange_declaration):
        future_exchange = asyncio.Future()
        channel.exchange_declare(**exchange_declaration,
                                 callback=lambda exchange: loop.call_soon(future_exchange.set_result, exchange))
        return await future_exchange

    @classmethod
    async def _create_queue(cls, loop, channel, queue_declaration):
        future_queue = asyncio.Future()
        channel.queue_declare(**queue_declaration,
                              callback=lambda queue: loop.call_soon(future_queue.set_result, queue))
        return await future_queue

    @classmethod
    async def _delete_exchange(cls, loop, channel, name):
        future_exchange_deletion = asyncio.Future()
        channel.exchange_delete(name,
                                callback=lambda queue: loop.call_soon(future_exchange_deletion.set_result, True))
        return await future_exchange_deletion

    @classmethod
    async def close_channel(cls, loop, channel):
        if channel:
            channel_closed = asyncio.Future()
            channel.add_on_close_callback(
                lambda channel, reason: loop.call_soon(channel_closed.set_result, True))
            channel.close()
            await channel_closed

    @classmethod
    async def stop_consumer(cls, loop, channel, consumer_tag):
        if channel and consumer_tag:
            consumer_stopped = asyncio.Future()
            channel.basic_cancel(consumer_tag, callback=lambda _: loop.call_soon(
                consumer_stopped.set_result, True))
            await consumer_stopped

    @classmethod
    async def close_connection(cls, loop, connection):
        if connection:
            connection_closed = asyncio.Future()
            connection.add_on_close_callback(
                lambda channel, reason: loop.call_soon(connection_closed.set_result, True))
            connection.close()
            await connection_closed

    @classmethod
    async def subscribe(cls, parameters, exchange_name, binding, queue_declaration, exchange_declaration, callback, output_type):
        loop = cls._get_loop()
        conn = await cls._create_connection(loop, parameters)
        subscription = Subscription(loop, conn, callback, output_type)

        channel = await cls._create_channel(loop, conn)
        subscription.add_channel(channel)

        future_bind = asyncio.Future()

        async def on_channel_closed(channel, reason):
            if reason.reply_code == 404:
                _logger.error(
                    f"Channel for exchange \"{exchange_name}\" and queue \"{queue_name}\" closed due to: Exchange not found")
                # Remove the unused channel
                new_channel = await asyncio.ensure_future(cls._create_channel(loop, conn))
                new_channel.queue_delete(queue_name)
                new_channel.close()
                # Return the error
                loop.call_soon(future_bind.set_result, reason)

        channel.add_on_close_callback(lambda channel, reason: asyncio.create_task(
            on_channel_closed(channel, reason)))

        if exchange_declaration is not None:
            # Create the exchange
            exchange = await cls._create_exchange(loop, channel, exchange_declaration)

        # Create the queue
        queue = await cls._create_queue(loop, channel, queue_declaration)
        queue_name = queue.method.queue

        if exchange_name:
            # Bind the queue to the exchange
            # The future_bind can be set by on_queue_bind_ok or by on_channel_closed
            # depending if the exchange exists or not
            def on_queue_bind_ok(bind):
                loop.call_soon(future_bind.set_result, bind)
            channel.queue_bind(queue=queue_name, exchange=exchange_name,
                               routing_key=binding, callback=on_queue_bind_ok)
            bind = await future_bind
            if isinstance(bind, pika.exceptions.ChannelClosedByBroker):
                await subscription.cancel()
                raise Exception(bind)

        # Start consuming
        subscription.start(queue_name)
        return subscription

    @classmethod
    async def send(cls, parameters, exchange, routing_key, body, exchange_declaration=None):
        loop = cls._get_loop()
        conn = await cls._create_connection(loop, parameters)
        channel = await cls._create_channel(loop, conn)

        if exchange_declaration is not None:
            exchange = await cls._create_exchange(loop, channel, exchange_declaration)

        if isinstance(body, dict):
            body = json.dumps(body)
        channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=body)

        await cls.close_channel(loop, channel)
        await cls.close_connection(loop, conn)

    @classmethod
    async def delete_exchange(cls, parameters, name):
        loop = cls._get_loop()
        conn = await cls._create_connection(loop, parameters)
        channel = await cls._create_channel(loop, conn)
        result = await cls._delete_exchange(loop, channel, name)
        await cls.close_channel(loop, channel)
        await cls.close_connection(loop, conn)
        return result
