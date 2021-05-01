import pytest, aiohopcolony, time, json, asyncio, functools
from .config import *
from aiohopcolony import topics

@pytest.fixture
async def project():
    return await aiohopcolony.initialize(username = user_name, project = project_name, 
                                            token = token)

@pytest.fixture
def conn():
    return topics.connection()

class TestTopics(object):

    topic = "test-topic"
    exchange = "test"
    data_string = "Test Message"
    data_json = {"data": "Testing Hop Topics!"}

    queue = asyncio.Queue()
    async def check(self, msg):
        await self.queue.put(msg)

    queue_aux = asyncio.Queue()
    async def check_aux(self, msg):
        await self.queue_aux.put(msg)

    @pytest.mark.asyncio
    async def test_a_initialize(self, project, conn):
        assert project.config != None
        assert project.name == project_name

        assert conn.project.name == project.name
        assert conn.host == "topics.hopcolony.io"
        assert conn.credentials.username == project.config.identity
        assert conn.credentials.password == project.config.token

    @pytest.mark.asyncio
    async def test_b_subscriber_publisher_string(self, conn):
        await conn.topic(self.topic).subscribe(self.check, output_type=topics.OutputType.STRING)
        await conn.topic(self.topic).send(self.data_string)
        assert await self.queue.get() == self.data_string
        await conn.close()

    @pytest.mark.asyncio
    async def test_c_subscriber_publisher_good_json(self, conn):
        await conn.topic(self.topic).subscribe(self.check, output_type=topics.OutputType.JSON)
        await conn.topic(self.topic).send(self.data_json)
        assert await self.queue.get() == self.data_json
        await conn.close()

    @pytest.mark.asyncio
    async def test_d_exchange_topic_fail(self, conn):
        with pytest.raises(Exception):
            await conn.exchange(self.exchange, create = False).topic(self.topic).subscribe(self.check, output_type=topics.OutputType.JSON)
    
    @pytest.mark.asyncio
    async def test_e_exchange_topic_fail(self, conn):
        await conn.exchange(self.exchange, create = True).topic(self.topic).subscribe(self.check, output_type=topics.OutputType.JSON)
        await conn.exchange(self.exchange).topic(self.topic).send(self.data_json)
        assert await self.queue.get() == self.data_json
        await conn.close()

    @pytest.mark.asyncio
    async def test_f_exchange_queue(self, conn):  
        await conn.exchange(self.exchange).queue(self.topic).subscribe(self.check, output_type=topics.OutputType.JSON)
        await conn.exchange(self.exchange).queue(self.topic).subscribe(self.check_aux, output_type=topics.OutputType.JSON)
        await conn.exchange(self.exchange).queue(self.topic).send(self.data_json)
        await conn.exchange(self.exchange).queue(self.topic).send(self.data_json)
        assert await self.queue.get() == self.data_json
        assert await self.queue_aux.get() == self.data_json
        await conn.close()

    @pytest.mark.asyncio
    async def test_g_delete_exchange(self, conn):
        await conn.exchange(self.exchange).delete()