import asyncio, pika, json, inspect, logging
from pika.adapters.asyncio_connection import AsyncioConnection
from concurrent.futures import ThreadPoolExecutor as Executor
from dataclasses import dataclass
from enum import Enum

_logger = logging.getLogger(__name__)

class OutputType(Enum):
    BYTES = 1
    STRING = 2
    JSON = 3

class MalformedJson(Exception):
    pass

class Subscription(object):
    _consuming: bool = False
    _closing: bool = False

    def __init__(self, loop, connection, exchange_name, binding, callback, output_type):
        self.loop = loop
        self.conn = connection
        self.executor = Executor()
        self.loop.set_default_executor(self.executor)

        self.exchange = exchange_name
        self.binding = binding
        self.callback = callback
        self.output_type = output_type

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
                raise MalformedJson(f"Malformed json message received on topic \"{self.binding}\": {body}") from e
                
        if inspect.iscoroutinefunction(self.callback):
            self.loop.create_task(self.callback(out))
        else:
            self.loop.run_in_executor(self.executor, self.callback, out)

    def start(self, queue_name):
        self.queue_name = queue_name
        self.consumer_tag = self.channel.basic_consume(queue_name, self.on_message)
    
    async def stop_consuming(self):
        if self.channel:
            future_channel_closed = asyncio.Future()
            def on_cancel_ok(_):
                self._consuming = False
                self.channel.close()
                self.loop.call_soon_threadsafe(future_channel_closed.set_result, True)
            self.channel.basic_cancel(self.consumer_tag, callback=on_cancel_ok)
            await future_channel_closed

    async def stop(self):
        if not self._closing:
            self._closing = True
            if self._consuming:
                await self.stop_consuming()

    async def cancel(self):
        if not self.conn.is_closing and not self.conn.is_closed:
            await self.stop()
            if self.executor:
                # Wait for the pending threads to finish
                self.executor.shutdown(wait=True)
            self.conn.close()

class Subscriber(object):

    @classmethod
    async def create_connection(cls, loop, parameters):
        future_conn = asyncio.Future()
        def on_connection_open(conn):
            loop.call_soon_threadsafe(future_conn.set_result, conn)
        def on_connection_open_error(_, reason):
            loop.call_soon_threadsafe(future_conn.set_result, reason)
        def on_connection_closed(_, reason):
            loop.call_soon_threadsafe(future_conn.set_result, reason)

        AsyncioConnection(
            parameters = parameters,
            on_open_callback=on_connection_open,
            on_open_error_callback=on_connection_open_error,
            on_close_callback=on_connection_closed)
        conn = await future_conn
        if isinstance(conn, str):
            raise Exception(conn)
        return conn

    @classmethod
    async def create_channel(cls, loop, conn):
        future_channel = asyncio.Future()
        conn.channel(on_open_callback=lambda channel: loop.call_soon(future_channel.set_result, channel))
        return await future_channel

    @classmethod
    async def subscribe(cls, parameters, exchange_name, binding, queue_declaration, exchange_declaration, callback, output_type):
        # Get event loop
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # If there is no event loop in the thread, create one abd set it
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Create futures
        future_exchange = asyncio.Future()
        future_queue = asyncio.Future()
        future_bind = asyncio.Future()
        
        # Create connection
        conn = await cls.create_connection(loop, parameters)
        
        # Create subscription
        subscription = Subscription(loop, conn, exchange_name, binding, callback, output_type)

         # Create channel
        channel = await cls.create_channel(loop, conn)
        subscription.add_channel(channel)

        def on_channel_closed(channel, reason):
            if reason.reply_code == 404:
                loop.call_soon(future_bind.set_result, reason)
                _logger.error(f"Channel for exchange \"{exchange_name}\" and queue \"{queue_name}\" closed due to: Exchange not found")
                # Create a blocking connection to remove the unused queue
                # new_channel = loop.run_until_complete(asyncio.create_task(cls.create_channel(loop, conn)))
                # new_channel.queue_delete(queue_name)
                # new_channel.close()
            
        channel.add_on_close_callback(on_channel_closed)

        if exchange_declaration is not None:
            # Create the exchange and wait
            def on_exchange_created(exchange):
                loop.call_soon(future_exchange.set_result, exchange)
            channel.exchange_declare(**exchange_declaration, callback=on_exchange_created)
            exchange = await future_exchange
        
        # Create the queue and wait
        def on_queue_created(queue):
            loop.call_soon(future_queue.set_result, queue)
        channel.queue_declare(**queue_declaration, callback=on_queue_created)
        queue = await future_queue
        queue_name = queue.method.queue
        
        if exchange_name:
            # Bind the queue to the exchange
            def on_queue_bind_ok(bind):
                loop.call_soon(future_bind.set_result, bind)
            channel.queue_bind(queue=queue_name, exchange=exchange_name, 
                routing_key=binding, callback=on_queue_bind_ok)
            bind = await future_bind
            if isinstance(bind, tuple):
                raise Exception(bind)
        
        # Start consuming
        subscription.start(queue_name)

        return subscription