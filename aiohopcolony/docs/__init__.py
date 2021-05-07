import aiohopcolony
from .docs_index import *
from .docs_document import *
import aiohttp
import re


def client(project=None):
    if not project:
        project = aiohopcolony.get_project()
    if not project:
        raise aiohopcolony.ConfigNotFound(
            "Hop Config not found. Run 'hopctl login' or place a .hop.config file here.")
    if not project.config.project:
        raise aiohopcolony.ConfigNotFound(
            "You have no projects yet. Create one at https://console.hopcolony.io")

    return HopDoc(project)


class HopDocClient:
    project: aiohopcolony.Project
    host: str
    port: int
    identity: str
    _base_url: str

    def __init__(self, project):
        self.project = project
        self.host = "docs.hopcolony.io"
        self.port = 443
        self.identity = project.config.identity
        self._base_url = f"https://{self.host}:{self.port}/{self.identity}/api"

    async def get(self, path, **kwargs):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.get(self._base_url + path, **kwargs) as response:
                return await response.json()

    async def post(self, path, **kwargs):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.post(self._base_url + path, **kwargs) as response:
                return await response.json()

    async def put(self, path, **kwargs):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.put(self._base_url + path, **kwargs) as response:
                return await response.json()

    async def delete(self, path, **kwargs):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.delete(self._base_url + path, **kwargs) as response:
                return await response.json()


class HopDoc:
    project: aiohopcolony.Project
    client: HopDocClient

    def __init__(self, project):
        self.project = project
        self.client = HopDocClient(project)

    @property
    async def status(self):
        try:
            return await self.client.get("/_cluster/health")
        except aiohttp.client_exceptions.ClientResponseError:
            return {"status": "Cluster not reachable"}

    def index(self, index):
        return IndexReference(self.client, index)

    async def get(self, filter_hidden=True):
        response = await self.client.get("/_cluster/health?level=indices")
        indices = []
        for name, status in response["indices"].items():
            if (not filter_hidden or not re.match(r"\..*", name)) and not re.match(r"ilm-history-.*", name):
                num_docs = await self.index(name).count
                indices.append(Index.fromJson(name, status, num_docs))
        return indices
