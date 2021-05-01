from .helper import *


class HopTopicQueue:
    def __init__(self, add_subscription, parameters, exchange="", binding="", name="", durable=False,
                 exclusive=False, auto_delete=True, exchange_declaration=None):
        self.add_subscription = add_subscription
        self.parameters = parameters
        self.exchange = exchange
        self.binding = binding
        self.exchange_declaration = exchange_declaration

        self.queue_declaration = {"queue": name, "durable": durable, "exclusive": exclusive,
                                  "auto_delete": auto_delete}

    async def subscribe(self, callback, output_type=OutputType.STRING):
        subscription = await TopicsHelper.subscribe(self.parameters, self.exchange, self.binding,
                                                    queue_declaration=self.queue_declaration,
                                                    exchange_declaration=self.exchange_declaration,
                                                    callback=callback, output_type=output_type)
        self.add_subscription(subscription)
        return subscription

    async def send(self, body):
        await TopicsHelper.send(self.parameters, self.exchange, self.binding, body, self.exchange_declaration)
