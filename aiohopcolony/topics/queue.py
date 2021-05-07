from .pika_helper import *


class HopTopicQueue:
    def __init__(self, connection, exchange="", binding="", name="", durable=False,
                 exclusive=False, auto_delete=True, exchange_declaration=None, channel=None):
        self.connection = connection
        self.channel = channel
        self.exchange = exchange
        self.binding = binding
        self.exchange_declaration = exchange_declaration

        self.queue_declaration = {"queue": name, "durable": durable, "exclusive": exclusive,
                                  "auto_delete": auto_delete}

    async def subscribe(self, callback, output_type=OutputType.STRING):
        return await self.connection.subscribe(self.exchange, self.binding,
                                               queue_declaration=self.queue_declaration,
                                               exchange_declaration=self.exchange_declaration,
                                               callback=callback, output_type=output_type)

    async def send(self, body, channel=None):
        if not channel:
            channel = await self.channel(f"{self.exchange}.{self.binding}")
        return await channel.send(self.exchange, self.binding, body, self.exchange_declaration)
