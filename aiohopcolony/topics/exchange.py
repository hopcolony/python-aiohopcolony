import logging
from enum import Enum
from .helper import TopicsHelper
from .queue import *

_logger = logging.getLogger(__name__)


class ExchangeType(Enum):
    DIRECT = 1
    FANOUT = 2
    TOPIC = 3


class HopTopicExchange:
    def __init__(self, add_subscription, parameters, name, create, type=ExchangeType.TOPIC, durable=True, auto_delete=False):
        self.add_subscription = add_subscription
        self.parameters = parameters
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
        return await HopTopicQueue(self.add_subscription, self.parameters, exchange=self.name, exchange_declaration=self.exchange_declaration) \
            .subscribe(callback, output_type=output_type)

    async def send(self, body):
        await TopicsHelper.send(self.parameters, self.name, "", body, self.exchange_declaration)

    def topic(self, name):
        return HopTopicQueue(self.add_subscription, self.parameters, exchange=self.name,
                             binding=name, exchange_declaration=self.exchange_declaration)

    def queue(self, name):
        return HopTopicQueue(self.add_subscription, self.parameters, exchange=self.name,
                             name=name, binding=name, exchange_declaration=self.exchange_declaration)

    async def delete(self):
        return await TopicsHelper.delete_exchange(self.parameters, self.name)
