import pika, threading, json, logging, functools, inspect, asyncio
from pika.adapters.asyncio_connection import AsyncioConnection
from concurrent.futures import ThreadPoolExecutor as Executor
from enum import Enum
from dataclasses import dataclass

_logger = logging.getLogger(__name__)

class OutputType(Enum):
    BYTES = 1
    STRING = 2
    JSON = 3

class MalformedJson(Exception):
    pass

class HopTopicQueue:
    def __init__(self, add_open_connection, parameters, exchange = "", binding = "", name = "", durable = False, exclusive = False, auto_delete = True):
        self.add_open_connection = add_open_connection
        self.parameters = parameters
        self.exchange = exchange
        self.binding = binding
        self.name = name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete

        self.should_reconnect = False
        self._closing = False
        self._consuming = False
        self.executor = None
    
    async def subscribe(self, callback, output_type = OutputType.STRING):
        self.callback = callback
        self.output_type = output_type

        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            # If there is no event loop in the thread, create one abd set it
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        self.conn = AsyncioConnection(
            parameters=self.parameters,
            on_open_callback=lambda _: self.conn.channel(on_open_callback = self.on_channel_open),
            on_open_error_callback=lambda _, __: cancel(),
            on_close_callback=self.on_connection_closed)
        
        self.add_open_connection(self)

        return self
    
    def cancel(self):
        self._consuming = False
        if not self.conn.is_closing and not self.conn.is_closed:
            self.stop()
            if self.executor:
                # Wait for the pending threads to finish
                self.executor.shutdown(wait=True)
            self.conn.close()
    
    def on_connection_closed(self, _, reason):
        self.channel = None
        # Maybe reconnect?
    
    def stop(self):
        if not self._closing:
            self._closing = True
            if self._consuming:
                self.stop_consuming()
    
    def stop_consuming(self):
        if self.channel:
            self.channel.basic_cancel(self._consumer_tag, callback=self.on_cancel_ok)
    
    def on_cancel_ok(self, _):
        self._consuming = False
        self.channel.close()

    def on_channel_open(self, channel):
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.queue_declare(self.name, durable=self.durable, exclusive=self.exclusive, 
            auto_delete=self.auto_delete, callback=self.on_queue_declare_ok)
    
    def on_channel_closed(self, channel, reason):
        if reason.reply_code == 404:
            _logger.error(f"Channel for exchange \"{self.exchange}\" and queue \"{self.queue_name}\" closed due to: Exchange not found")
            # Create a blocking connection to remove the unused queue
            conn = pika.BlockingConnection(self.parameters)
            conn.channel().queue_delete(self.queue_name)
            conn.close()
        self.cancel()

    def on_queue_declare_ok(self, queue):
        self.queue_name = queue.method.queue
        if self.exchange:
            self.channel.queue_bind(queue=self.queue_name, exchange=self.exchange, 
                routing_key=self.binding, callback=lambda _: self.start_consuming())
        else:
            self.start_consuming()

    def start_consuming(self):
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self.channel.basic_consume(
            self.queue_name, self.on_message)
        self._consuming = True

    def on_consumer_cancelled(self, _):
        if self.channel:
            self.channel.close()

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
            if not self.executor:
                self.executor = Executor()
                self.loop.set_default_executor(self.executor)
            self.loop.run_in_executor(self.executor, self.callback, out)

    def send(self, body):
        conn = pika.BlockingConnection(self.parameters)

        if isinstance(body, dict):
            body = json.dumps(body)
        
        conn.channel().basic_publish(exchange=self.exchange, routing_key=self.binding, body=body)
        conn.close()