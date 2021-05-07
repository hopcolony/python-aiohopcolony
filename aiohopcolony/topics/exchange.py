import logging
from enum import Enum
from .queue import *

_logger = logging.getLogger(__name__)


class ExchangeType(Enum):
    DIRECT = 1
    FANOUT = 2
    TOPIC = 3


class HopTopicExchange:
    def __init__(self, connection, name, create=None, type=ExchangeType.TOPIC, durable=True, auto_delete=False, channel=None):
        self.connection = connection
        self.channel = channel
        self.name = name
        self.exchange_declaration = None

        if create:
            self.exchange_declaration = {"exchange": self.name, "exchange_type": self.str_type(
                type), "durable": durable, "auto_delete": auto_delete}

    def str_type(self, type):
        if type == ExchangeType.DIRECT:
            return "direct"
        if type == ExchangeType.FANOUT:
            return "fanout"
        return "topic"

    async def subscribe(self, callback, output_type=OutputType.STRING):
        return await HopTopicQueue(self.connection, exchange=self.name, exchange_declaration=self.exchange_declaration) \
            .subscribe(callback, output_type=output_type)

    async def send(self, body, channel=None):
        if not channel:
            channel = await self.channel(f"{self.name}.")
        return await channel.send(self.name, "", body, self.exchange_declaration)

    def topic(self, name):
        return HopTopicQueue(self.connection, exchange=self.name,
                             binding=name, exchange_declaration=self.exchange_declaration, channel=self.channel)

    def queue(self, name):
        return HopTopicQueue(self.connection, exchange=self.name,
                             name=name, binding=name, exchange_declaration=self.exchange_declaration, channel=self.channel)

    async def delete(self, channel=None):
        if not channel:
            channel = await self.channel(f"{self.name}.")
        return await channel.exchange_delete(self.name)
