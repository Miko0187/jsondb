import os
import json
import uuid
import copy
import typing
if typing.TYPE_CHECKING:
    from .db import Database

class Collection:
    def __init__(self, name: str, db: "Database"):
        self.name = name
        self.data: list[dict] = None
        self.db = db
        self.path = f"{self.db.save_dir}/files/{self.db.db_name}/{self.name}.json"

    def load(self):
        if not os.path.exists(self.path):
            self.data = []
        else:
            with open(self.path, "r") as f:
                content = f.read()
            self.data = json.loads(content)

    def delete(self):
        os.remove(self.path)

    async def save(self):
        await self.db.manager.save(self)

    async def insert(self, document: dict):
        document["@id"] = str(uuid.uuid4())
        self.data.append(document)

        await self.save()

    async def update(self, query: dict, updates: dict):
        old = {}
        new = {}
        for doc in self.data:
            if all(doc.get(k) == v for k, v in query.items()):
                old = copy.deepcopy(doc)
                doc.update(updates)
                new = copy.deepcopy(doc)
                
        await self.save()
        return old, new
    
    async def delete(self, str, query: dict):
        to_delete = [
            copy.deepcopy(doc)
            for doc in self.data
            if all(doc.get(k) == v for k, v in query.items())
        ]
        self.data[:] = [
            doc for doc in self.data
            if not all(doc.get(k) == v for k, v in query.items())
        ]
        
        await self.save()

        return to_delete
            
    def find_all(self, query: dict = None):
        if query is None:
            return self.data
        
        return [doc for doc in self.data if all(doc.get(k) == v for k, v in query.items())]
    
    def find_one(self, query: dict):
        for doc in self.data:
            if all(doc.get(k) == v for k, v in query.items()):
                return doc
            
        return None  
        
