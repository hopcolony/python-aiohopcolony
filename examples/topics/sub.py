import aiohopcolony
from aiohopcolony import topics
import threading
import asyncio


async def main():
    await aiohopcolony.initialize()

    conn = await topics.connection()
    threading.Thread(target=lambda: topics.connection())

    print(f"Main thread identity: {threading.get_ident()}")

    # Asyncio coroutine example
    async def queue_test(msg):
        await asyncio.sleep(1)
        print(f"[QUEUE, {threading.get_ident()}] {msg}")

    await conn.queue("queue-test").subscribe(queue_test)

    # Decorator example
    @conn.topic("topic-cat", output_type=topics.OutputType.JSON)
    async def topic_cat(msg):
        print(f"[CAT, {threading.get_ident()}] {msg}")

    # Lambda example
    await conn.exchange("broadcast", create=True).subscribe(lambda msg: print(f"[BROADCAST, {threading.get_ident()}] {msg}"))

loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
