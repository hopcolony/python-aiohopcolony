import asyncio, json
from pika.adapters.asyncio_connection import AsyncioConnection

class Publisher(object):
    
    @classmethod
    async def send(cls, parameters, exchange, routing_key, body, exchange_declaration = None):
        # Get event loop
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # If there is no event loop in the thread, create one abd set it
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Wait until the connection is opened
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

        # Wait until channel is opened
        future_channel = asyncio.Future()
        def on_channel_open(channel):
            loop.call_soon(future_channel.set_result, channel)
        conn.channel(on_open_callback=on_channel_open)
        channel = await future_channel

        if exchange_declaration is not None:
            # Create the exchange and wait
            future_exchange = asyncio.Future()
            def on_exchange_created(exchange):
                loop.call_soon(future_exchange.set_result, exchange)
            channel.exchange_declare(**exchange_declaration, callback=on_exchange_created)
            exchange = await future_channel
        
        # Publish to exchange
        if isinstance(body, dict):
            body = json.dumps(body)

        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
        channel.close()
        conn.close()