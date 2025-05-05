import typing
if typing.TYPE_CHECKING:
    from .database import Database

class Collection:
    def __init__(self, database: "Database", name: str):
        self.name = name
        self.database = database
        
        self.raise_error = self.database.raise_error
        self._send = self.database._send
    
    async def insert(self, document: dict):
        req = await self._send("insert_doc", {
            "collection": self.name,
            "dict": document
        })
            
        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"], "Collection", self.name)
            
    async def find_one(self, query: dict):
        req = await self._send("find_one_doc", {
            "collection": self.name,
            "query": query
        })
            
        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"], "Collection", self.name)
        else:
            return req["d"]["result"]
        
    async def find_all(self, query: dict = None):
        req = await self._send("find_all_doc", {
            "collection": self.name,
            "query": query
        })
            
        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"], "Collection", self.name)
        else:
            return req["d"]["result"]
        
    async def update(self, query: dict, update: dict):
        req = await self._send("update_doc", {
            "collection": self.name,
            "query": query,
            "update": update
        })
            
        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"], "Collection", self.name)
            
    async def delete(self, query: dict):
        req = await self._send("delete_doc", {
            "collection": self.name,
            "query": query
        })
            
        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"], "Collection", self.name)
    