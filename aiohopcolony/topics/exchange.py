import pika, logging
from enum import Enum
from .publisher import Publisher
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
        self.create = create
        self.type = type
        self.durable = durable
        self.auto_delete = auto_delete
        self.exchange_declaration = None

        if self.create:
            self.exchange_declaration = {"exchange": self.name, "exchange_type": self.str_type, "durable": self.durable, "auto_delete": self.auto_delete}

    @property
    def str_type(self):
        if self.type == ExchangeType.DIRECT:
            return "direct"
        if self.type == ExchangeType.FANOUT:
            return "fanout"
        return "topic"
    
    async def subscribe(self, callback, output_type = OutputType.STRING):
        return await HopTopicQueue(self.add_subscription, self.parameters, exchange = self.name, exchange_declaration = self.exchange_declaration) \
            .subscribe(callback, output_type=output_type)
  
    async def send(self, body):
        await Publisher.send(self.parameters, self.name, "", body, self.exchange_declaration)

    def topic(self, name):
        return HopTopicQueue(self.add_subscription, self.parameters, exchange = self.name, 
            binding = name, exchange_declaration = self.exchange_declaration)

    def queue(self, name):
        return HopTopicQueue(self.add_subscription, self.parameters, exchange = self.name, 
            name = name, binding = name, exchange_declaration = self.exchange_declaration)