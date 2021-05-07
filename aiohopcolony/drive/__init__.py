import aiohopcolony
import aiofile
from .drive_signer import *
from .drive_bucket import *
from .drive_object import *

from bs4 import BeautifulSoup
from datetime import datetime


def client(project=None):
    if not project:
        project = aiohopcolony.get_project()
    if not project:
        raise aiohopcolony.ConfigNotFound(
            "Hop Config not found. Run 'hopctl login' or place a .hop.config file here.")
    if not project.config.project:
        raise aiohopcolony.ConfigNotFound(
            "You have no projects yet. Create one at https://console.hopcolony.io")

    return HopDrive(project)


async def load_image(path):
    async with aiofile.async_open(path, "rb") as image:
        f = await image.read()
        return bytearray(f)


class HopDriveClient(object):
    project: aiohopcolony.Project
    host: str
    port: int
    identity: str
    base_url: str
    signer: drive_signer.Signer

    def __init__(self, project):
        self.project = project
        self.host = "drive.hopcolony.io"
        self.port = 443
        self.identity = project.config.identity
        self.base_url = f"https://{self.host}:{self.port}/{self.identity}"
        self.signer = drive_signer.Signer(
            self.host, project.config.project, project.config.token)

    async def get(self, path, bytesOut=False):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            signDetails = self.signer.sign("GET", path)
            async with session.get(self.base_url + path, headers=signDetails.headers) as response:
                return await response.text() if not bytesOut else await response.read()

    async def put(self, path, bodyBytes):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            signDetails = self.signer.sign("PUT", path, bodyBytes=bodyBytes)
            async with session.put(self.base_url + path, headers=signDetails.headers, data=bodyBytes) as response:
                return await response.text()

    async def delete(self, path):
        async with aiohttp.ClientSession(raise_for_status=True) as session:
            signDetails = self.signer.sign("DELETE", path)
            async with session.delete(self.base_url + path, headers=signDetails.headers) as response:
                return await response.text()


class HopDrive(object):
    project: aiohopcolony.Project
    client: HopDriveClient

    def __init__(self, project):
        self.project = project
        self.client = HopDriveClient(project)

    def bucket(self, bucket):
        return BucketReference(self.client, bucket)

    async def get(self):
        response = await self.client.get("/")
        soup = BeautifulSoup(response, features="html.parser")
        buckets = []
        for soup in soup.find_all("bucket"):
            name = soup.find("name").text
            creation_date = datetime.strptime(
                soup.find("creationdate").text, '%Y-%m-%dT%H:%M:%S.%fZ')
            num_objs = 0
            try:
                # Get the objects in the bucket
                snapshot = await self.bucket(name).get()
                if snapshot.success:
                    num_objs = len(snapshot.objects)
            except aiohttp.client_exceptions.ClientResponseError:
                pass
            buckets.append(Bucket(name, creation_date, num_objs))
        return buckets
