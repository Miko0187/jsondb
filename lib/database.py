import typing
from .collection import Collection
if typing.TYPE_CHECKING:
    from .connection import Connection

class Database:
    def __init__(self, connection: "Connection", name: str):
        self.name = name
        self._connection = connection
        
        self.raise_error = self._connection.raise_error
        self._send = self._connection._send
        
    async def create_collection(self, name: str):
        req = await self._send("create_collection", {
            "name": name
        })
            
        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"], "Collection", name)
            
    async def list_collections(self) -> list[str]:
        req = await self._send("list_collections")
            
        if req["op"] != "ok":
            self.raise_error(req["error"], req["id"])
        
        return req["d"]["result"]
        
    async def get_collection(self, name: str) -> Collection | None:
        colls = await self.list_collections()
        
        for coll in colls:
            if coll == name:
                return Collection(self, coll)
            
        return None
        
    async def delete_collection(self, name: str):
        req = await self._send("delete_collection", {
            "name": name
        })
            
        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"], "Collection", name)
        