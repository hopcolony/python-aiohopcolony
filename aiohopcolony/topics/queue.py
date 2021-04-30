import pika, threading, json, logging, functools, inspect, asyncio
from pika.adapters.asyncio_connection import AsyncioConnection
from concurrent.futures import ThreadPoolExecutor as Executor
from dataclasses import dataclass
from .publisher import Publisher
from .subscriber import *

class HopTopicQueue:
    def __init__(self, add_subscription, parameters, exchange = "", binding = "", name = "", durable = False, 
            exclusive = False, auto_delete = True, exchange_declaration = None):
        self.add_subscription = add_subscription
        self.parameters = parameters
        self.exchange = exchange
        self.binding = binding
        self.name = name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.exchange_declaration = exchange_declaration

        self.queue_declaration = {"queue": self.name, "durable": self.durable, "exclusive": self.exclusive, 
                "auto_delete": self.auto_delete}

    async def subscribe(self, callback, output_type = OutputType.STRING):
        subscription = await Subscriber.subscribe(self.parameters, self.exchange, self.binding,
            queue_declaration=self.queue_declaration,
            exchange_declaration=self.exchange_declaration,
            callback=callback, output_type=output_type)
        self.add_subscription(subscription)
        return subscription

    async def send(self, body):
        await Publisher.send(self.parameters, self.exchange, self.binding, body, self.exchange_declaration)