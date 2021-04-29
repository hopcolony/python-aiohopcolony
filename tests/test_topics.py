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

    @pytest.mark.asyncio
    async def test_a_initialize(self, project, conn):
        assert project.config != None
        assert project.name == project_name

        assert conn.project.name == project.name
        assert conn.host == "topics.hopcolony.io"
        assert conn.credentials.username == project.config.identity
        assert conn.credentials.password == project.config.token

    # @pytest.mark.asyncio
    # async def test_b_subscriber_publisher_string(self, conn):
    #     async def check(queue, msg):
    #         print(msg)
    #         queue.put(msg)

    #     queue = asyncio.Queue()
    #     await conn.topic(self.topic).subscribe(functools.partial(check, queue), output_type=topics.OutputType.STRING)
    #     await asyncio.sleep(0.2)
    #     conn.topic(self.topic).send(self.data_string)

    #     await asyncio.sleep(0.4)
    #     result = await queue.get()
    #     assert result != self.data_string
    #     conn.close()

    # @pytest.mark.asyncio
    # async def test_c_subscriber_publisher_good_json(self, conn):  
    #     self.conn.topic(self.topic).subscribe(lambda msg: self.assertEqual(msg, self.data_json), 
    #                 output_type=topics.OutputType.JSON)
    #     time.sleep(0.1)
    #     self.conn.topic(self.topic).send(self.data_json)
    #     time.sleep(0.1)
    #     self.conn.close_open_connections()

    # @pytest.mark.asyncio
    # async def test_d_exchange_topic(self, conn):  
    #     self.conn.exchange(self.exchange).topic(self.topic).subscribe(lambda msg: self.assertEqual(msg, self.data_json), 
    #                 output_type=topics.OutputType.JSON)
    #     time.sleep(0.1)
    #     self.conn.exchange(self.exchange).topic(self.topic).send(self.data_json)
    #     time.sleep(0.1)
    #     self.conn.close_open_connections()

    # @pytest.mark.asyncio
    # async def test_e_exchange_queue(self, conn):  
    #     self.conn.exchange(self.exchange).queue(self.topic).subscribe(lambda msg: self.assertEqual(msg, self.data_json), 
    #                 output_type=topics.OutputType.JSON)
    #     self.conn.exchange(self.exchange).queue(self.topic).subscribe(lambda msg: self.assertEqual(msg, self.data_json), 
    #                 output_type=topics.OutputType.JSON)
    #     time.sleep(0.1)
    #     self.conn.exchange(self.exchange).queue(self.topic).send(self.data_json)
    #     self.conn.exchange(self.exchange).queue(self.topic).send(self.data_json)
    #     time.sleep(0.1)
    #     self.conn.close_open_connections()