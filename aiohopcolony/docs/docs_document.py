import aiohttp
from dataclasses import dataclass


@dataclass
class Document:

    source: dict
    index: str = None
    id: str = None
    version: int = None

    @classmethod
    def fromJson(cls, json):
        return cls(json["_source"], index=json["_index"], id=json["_id"], version=json["_version"] if "_version" in json else None)

    def __str__(self):
        return f'"source": {self.source}, "index": {self.index}, "id": {self.id}, "version": {self.version}'

    @classmethod
    def printable_headers(self, columns):
        return ['Id', *columns]

    def printable(self, columns):
        data = []
        for column in columns:
            value = self.source[column] if column in self.source else None
            data.append(value)
        return [self.id, *data]


class DocumentSnapshot:
    def __init__(self, doc, success=False, reason=""):
        self.doc = doc
        self.success = success
        self.reason = reason


class DocumentReference:
    def __init__(self, client, index, id):
        self.client = client
        self.index = index
        self.id = id

    async def get(self):
        try:
            response = await self.client.get(f"/{self.index}/_doc/{self.id}")
            return DocumentSnapshot(Document.fromJson(response), success=True)
        except aiohttp.client_exceptions.ClientResponseError as e:
            return DocumentSnapshot(None, success=False, reason=str(e))

    async def setData(self, doc):
        try:
            response = await self.client.post(f"/{self.index}/_doc/{self.id}", json=doc)
            return DocumentSnapshot(Document(doc, index=self.index, id=response["_id"], version=response["_version"]), success=True)
        except aiohttp.client_exceptions.ClientResponseError as e:
            return DocumentSnapshot(None, success=False, reason=str(e))

    async def update(self, fields):
        try:
            await self.client.post(f"/{self.index}/_doc/{self.id}/_update", json={"doc": fields})
            return await self.get()
        except aiohttp.client_exceptions.ClientResponseError as e:
            return DocumentSnapshot(None, success=False, reason=str(e))

    async def delete(self):
        try:
            response = await self.client.delete(f"/{self.index}/_doc/{self.id}")
            return DocumentSnapshot(Document(None, index=self.index, id=response["_id"], version=response["_version"]), success=True)
        except aiohttp.client_exceptions.ClientResponseError as e:
            return DocumentSnapshot(None, success=False, reason=str(e))
