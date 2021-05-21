import pytest
import asyncio
from .config import *
import aiohopcolony
from aiohopcolony import auth


@pytest.fixture
async def project():
    return await aiohopcolony.initialize(username=user_name, project=project_name,
                                         token=token)


@pytest.fixture
def db():
    return auth.client()


class TestAuth(object):

    email = "tests@hopcolony.io"
    password = "secret"
    uid = "eb562632-c4b3-50da-9a65-d2083f347af5"

    @pytest.mark.asyncio
    async def test_a_initialize(self, project, db):
        assert project.config != None
        assert project.name == project_name
        assert db.project.name == project.name

    @pytest.mark.asyncio
    async def test_b_register_with_username_and_password(self, db):
        result = await db.register_with_email_and_password(self.email, self.password)
        assert result.success == True
        user = result.user
        assert user.provider == "email"
        assert user.email == self.email
        assert user.password == self.password
        assert user.uuid != None

    @pytest.mark.asyncio
    async def test_c_register_duplicated(self, db):
        with pytest.raises(auth.DuplicatedEmail):
            await db.register_with_email_and_password(self.email, self.password)

    @pytest.mark.asyncio
    async def test_d_list_users(self, db):
        # It takes some time to index the previous addition
        await asyncio.sleep(1)
        users = await db.get()
        assert self.email in [user.email for user in users]

    @pytest.mark.asyncio
    async def test_e_sign_in(self, db):
        result = await db.sign_in_with_email_and_password(self.email, self.password)
        assert result.success == True
        user = result.user
        assert user.provider == "email"
        assert user.email == self.email
        assert user.password == self.password
        assert user.uuid != None

    @pytest.mark.asyncio
    async def test_f_delete_user(self, db):
        await db.delete(self.uid)
