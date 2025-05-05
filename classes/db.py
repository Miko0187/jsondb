import os
import json
import typing
from .collection import Collection
if typing.TYPE_CHECKING:
    from .manager import Manager

class Database:
    def __init__(self, db_name: str, manager: "Manager"):
        self.save_dir = manager.save_dir
        self.db_name = db_name

        self.manager = manager
        self.collections: dict[str, Collection] = dict()

    def load(self):
        for file in os.scandir(f"{self.save_dir}/files/{self.db_name}"):
            if file.is_file():
                if file.name.endswith(".json"):
                    coll = Collection(file.name[:-5], self)
                    coll.load()
                    self.collections[coll.name] = coll
    
    def get(self, collection: str):
        return self.collections.get(collection)

    def collection_exists(self, collection: str):
        return True if self.get(collection) else False
    
    def create_collection(self, name: str):
        path = f"{self.save_dir}/files/{self.db_name}/{name}.json"
        if os.path.isfile(path):
            return False
            
        with open(path, "w") as f:
            json.dump([], f)

        coll = Collection(name, self)
        coll.load()
        self.collections[coll.name] = coll
            
        return True
    
    def list_collections(self):
        result = []
        for name, _ in self.collections.items():
            result.append(name)

        return result
    
    def delete_collection(self, name: str):
        coll = self.get(name)
        if coll == None:
            return False
        
        try:
            coll.delete()
            del self.collections[coll.name]
        except:
            pass
            
        return True
