from .drive_object import *
from bs4 import BeautifulSoup
from datetime import datetime
import uuid
import aiohttp
from dataclasses import dataclass


@dataclass
class Bucket:
    printable_headers = ["Name", "Number of Objects", "Creation Date"]

    name: str
    creation_date: datetime
    num_objs: int

    @property
    def json(self):
        return {"name": self.name, "creation_date": self.creation_date, "num_objs": self.num_objs}

    @property
    def printable(self):
        return [self.name, self.num_objs, self.creation_date]


class BucketReference:
    def __init__(self, client, bucket):
        self.client = client
        self.bucket = bucket

    async def get(self):
        try:
            response = await self.client.get(f"/{self.bucket}")
            soup = BeautifulSoup(response, features="html.parser")
            objects = []
            for s in soup.find_all("contents"):
                url = self.object(s.find("key").text).get_presigned()
                objects.append(Object.fromSoup(url, s))
            return BucketSnapshot(objects, success=True)
        except aiohttp.client_exceptions.ClientResponseError:
            return BucketSnapshot(None, success=False)

    @property
    async def exists(self):
        try:
            await self.client.get(f"/{self.bucket}")
            return True
        except aiohttp.client_exceptions.ClientResponseError:
            return False

    async def create(self):
        try:
            await self.client.put(f"/{self.bucket}", bodyBytes=b"")
            return True
        except aiohttp.client_exceptions.ClientResponseError as e:
            print(e)
            return False

    async def add(self, data):
        id = uuid.uuid4().hex.upper()[0:10]
        return await self.object(id).put(data)

    def object(self, id):
        async def bucket_create():
            # Create a coroutine to check and create the bucket in the object
            # because we don't want to await here
            if not await self.exists:
                success = await self.create()
                assert success, f"{self.bucket} did not exist and could not be created"
        return ObjectReference(self.client, self.bucket, id, bucket_create)

    async def delete(self):
        # Delete all the objects before deleting the bucket
        if not await self.exists:
            # Deleting a non existing bucket
            return True
        else:
            snapshot = await self.get()
            for object in snapshot.objects:
                await self.object(object.id).delete()
        try:
            await self.client.delete(f"/{self.bucket}")
            return True
        except aiohttp.client_exceptions.ClientResponseError as e:
            return False


class BucketSnapshot:
    def __init__(self, objects, success=False):
        self.objects = objects
        self.success = success
