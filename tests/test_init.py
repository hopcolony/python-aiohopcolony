import pytest
import aiohopcolony
from .config import *


class TestInitialize(object):

    @pytest.mark.asyncio
    async def test_initialize(self):
        with pytest.raises(aiohopcolony.ConfigNotFound):
            await aiohopcolony.initialize(config_file="..")

        with pytest.raises(aiohopcolony.InvalidConfig):
            await aiohopcolony.initialize(username=user_name)

        with pytest.raises(aiohopcolony.InvalidConfig):
            await aiohopcolony.initialize(username=user_name, project=project_name)

        project = await aiohopcolony.initialize(username=user_name, project=project_name,
                                                token=token)

        assert project.config.username == user_name
        assert project.config.project == project_name
        assert project.config.token == token
