import aiohopcolony
from aiohopcolony import topics
import asyncio


async def main():
    await aiohopcolony.initialize()

    conn = await topics.connection()
    await conn.topic("topic-cat").send({"data": "Hello"})
    await conn.exchange("broadcast").send("Hello")
    await conn.queue("queue-test").send("Hello")

asyncio.run(main())
