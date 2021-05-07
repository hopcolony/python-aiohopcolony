import pytest
import os
import aiohttp
from .config import *
import aiohopcolony
from aiohopcolony import drive

obj = "test"


@pytest.fixture
async def project():
    return await aiohopcolony.initialize(username=user_name, project=project_name,
                                         token=token)


@pytest.fixture
def db():
    return drive.client()


@pytest.fixture
async def img():
    return await drive.load_image(os.path.join(os.path.dirname(__file__), "resources", obj))


class TestDrive(object):

    bucket = "hop-test"

    @pytest.mark.asyncio
    async def test_a_initialize(self, project, db):
        assert project.config != None
        assert project.name == project_name

        assert db.project.name == project.name
        assert db.client.host == "drive.hopcolony.io"
        assert db.client.identity == project.config.identity

    @pytest.mark.asyncio
    async def test_b_load_image(self):
        with pytest.raises(FileNotFoundError):
            await drive.load_image("whatever")

    @pytest.mark.asyncio
    async def test_c_get_non_existing_bucket(self, db):
        snapshot = await db.bucket("whatever").get()
        assert snapshot.success == False

    @pytest.mark.asyncio
    async def test_d_create_bucket(self, db):
        success = await db.bucket(self.bucket).create()
        assert success == True

    @pytest.mark.asyncio
    async def test_e_get_existing_bucket(self, db):
        snapshot = await db.bucket(self.bucket).get()
        assert snapshot.success == True

    @pytest.mark.asyncio
    async def test_f_list_buckets(self, db):
        buckets = await db.get()
        assert self.bucket in [bucket.name for bucket in buckets]

    @pytest.mark.asyncio
    async def test_g_delete_bucket(self, db):
        result = await db.bucket(self.bucket).delete()
        assert result == True

    @pytest.mark.asyncio
    async def test_h_delete_non_existing_bucket(self, db):
        result = await db.bucket(self.bucket).delete()
        assert result == True

    @pytest.mark.asyncio
    async def test_i_create_object(self, db, img):
        snapshot = await db.bucket(self.bucket).object(obj).put(img)
        assert snapshot.success == True

    @pytest.mark.asyncio
    async def test_j_find_object(self, db):
        snapshot = await db.bucket(self.bucket).get()
        assert snapshot.success == True
        assert obj in [obj.id for obj in snapshot.objects]

    @pytest.mark.asyncio
    async def test_k_get_object(self, db, img):
        snapshot = await db.bucket(self.bucket).object(obj).get()
        assert snapshot.success == True
        assert obj == snapshot.object.id
        assert img == snapshot.object.data

    @pytest.mark.asyncio
    async def test_l_get_presigned_object(self, db, img):
        url = db.bucket(self.bucket).object(obj).get_presigned()
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.get(url) as response:
                content = await response.read()
                assert content == img

    @pytest.mark.asyncio
    async def test_m_delete_object(self, db):
        result = await db.bucket(self.bucket).object(obj).delete()
        assert result == True

    @pytest.mark.asyncio
    async def test_n_add_object(self, db, img):
        snapshot = await db.bucket(self.bucket).add(img)
        assert snapshot.success == True
        assert snapshot.object.id != None

    @pytest.mark.asyncio
    async def test_o_delete_bucket(self, db):
        result = await db.bucket(self.bucket).delete()
        assert result == True
