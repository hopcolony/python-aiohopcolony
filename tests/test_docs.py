import pytest
from .config import *
import aiohopcolony
from aiohopcolony import docs


@pytest.fixture
async def project():
    return await aiohopcolony.initialize(username=user_name, project=project_name,
                                         token=token)


@pytest.fixture
def db():
    return docs.client()


class TestDocs(object):

    index = ".hop.tests"
    uid = "hopcolony"
    data = {"purpose": "Test Hop Docs!"}

    @pytest.mark.asyncio
    async def test_a_initialize(self, project, db):
        assert project.config != None
        assert project.name == project_name

        assert db.project.name == project.name
        assert db.client.host == "docs.hopcolony.io"
        assert db.client.identity == project.config.identity

    @pytest.mark.asyncio
    async def test_b_status(self, db):
        status = await db.status
        assert status["status"] != "red"

    @pytest.mark.asyncio
    async def test_c_create_document(self, db):
        snapshot = await db.index(self.index).document(self.uid).setData(self.data)
        assert snapshot.success == True
        doc = snapshot.doc
        assert doc.index == self.index
        assert doc.id == self.uid
        assert doc.source == self.data

    @pytest.mark.asyncio
    async def test_d_get_document(self, db):
        snapshot = await db.index(self.index).document(self.uid).get()
        assert snapshot.success == True
        doc = snapshot.doc
        assert doc.index == self.index
        assert doc.id == self.uid
        assert doc.source == self.data

    @pytest.mark.asyncio
    async def test_e_delete_document(self, db):
        snapshot = await db.index(self.index).document(self.uid).delete()
        assert snapshot.success == True

    @pytest.mark.asyncio
    async def test_f_find_non_existing(self, db):
        snapshot = await db.index(self.index).document(self.uid).get()
        assert snapshot.success == False

        snapshot = await db.index(self.index).document(self.uid).update({"data": "test"})
        assert snapshot.success == False

        snapshot = await db.index(self.index).document(self.uid).delete()
        assert snapshot.success == False

        snapshot = await db.index(".does.not.exist").get()
        assert snapshot.success == False

    @pytest.mark.asyncio
    async def test_g_create_document_without_id(self, db):
        snapshot = await db.index(self.index).add(self.data)
        assert snapshot.success == True
        doc = snapshot.doc
        assert doc.index == self.index
        assert doc.source == self.data

        snapshot = await db.index(self.index).document(doc.id).delete()
        assert snapshot.success == True

    @pytest.mark.asyncio
    async def test_h_delete_index(self, db):
        result = await db.index(self.index).delete()
        assert result == True

    @pytest.mark.asyncio
    async def test_i_index_not_there(self, db):
        result = await db.get()
        assert self.index not in [index.name for index in result]
