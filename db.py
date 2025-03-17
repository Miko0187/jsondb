import json
import os
import uuid


_cache: dict[str, dict[str, list[dict]]] = {}


def remove_db(name: str):
    try:
        del _cache[name]
    except:
        pass


class Database:
    def __init__(self, data_files: str, db_name: str):
        self.data_files = data_files
        self.db_name = db_name
        self.data: dict[str, list[dict]] = {}
        
        self.load()

    def _save(self, collection: str):
        with open(f"{self.data_files}/files/{self.db_name}/{collection}.json", "w") as f:
            json.dump(self.data[collection], f)
        
    def load(self):
        with open(f"{self.data_files}/files/jsondb.json", "r") as f:
            try:
                self.data["jsondb"] = json.load(f)
            except json.JSONDecodeError:
                print(f"Failed to user file {self.data_files}/files/jsondb.json")
        
        if not _cache.get(self.db_name):
            _cache[self.db_name] = {}
        
        for file in os.scandir(f"{self.data_files}/files/{self.db_name}"):
            if file.is_file():
                if file.name.endswith(".json"):
                    if not _cache[self.db_name].get(file.name[:-5]): 
                        with open(f"{self.data_files}/files/{self.db_name}/{file.name}", "r") as f:
                            try:
                                _data = json.load(f)
                                self.data[file.name[:-5]] = _data
                                _cache[self.db_name][file.name[:-5]] = _data
                            except json.JSONDecodeError:
                                print(f"Failed to load {self.data_files}/files/{self.db_name}/{file.name}")
                    else:
                        self.data[file.name[:-5]] = _cache[self.db_name][file.name[:-5]]
                            
    def get_user(self, name: str):
        return self.data["jsondb"].get(name)
    
    def collection_exists(self, collection: str):
        return self.data.get(collection)
    
    def create_collection(self, name: str):
        if os.path.isfile(f"{self.data_files}/files/{self.db_name}/{name}.json"):
            return False
            
        self.data[name] = []
        _cache[self.db_name][name] = []

        self._save(name)
            
        return True
    
    def list_collections(self):
        result = []
        
        for file in os.scandir(f"{self.data_files}/files/{self.db_name}"):
            if file.is_file():
                result.append(file.name[:-5])
                
        return result
    
    def delete_collection(self, name: str):
        if not os.path.isfile(f"{self.data_files}/files/{self.db_name}/{name}.json"):
            return False
        
        os.remove(f"{self.data_files}/files/{self.db_name}/{name}.json")
        
        try:
            del self.data[name]
            del _cache[self.db_name][name]
        except:
            return False
            
        return True
    
    def insert_doc(self, collection: str, document: dict):
        document["@id"] = str(uuid.uuid4())
        
        self.data[collection].append(document)

        self._save(collection)
            
    def update(self, collection: str, query: dict, updates: dict):
        for doc in self.data[collection]:
            if all(doc.get(k) == v for k, v in query.items()):
                doc.update(updates)
                
        _cache[self.db_name][collection] = self.data[collection]
        
        self._save(collection)

    def delete(self, collection: str, query: dict):
        self.data[collection][:] = [doc for doc in self.data[collection] if not all(doc.get(k) == v for k, v in query.items())]
        
        _cache[self.db_name][collection] = self.data[collection]
        
        self._save(collection)
            
    def find_all(self, collection: str, query: dict = None):
        if query is None:
            return self.data[collection]
        
        return [doc for doc in self.data[collection] if all(doc.get(k) == v for k, v in query.items())]
    
    def find_one(self, collection: str, query: dict):
        for doc in self.data[collection]:
            if all(doc.get(k) == v for k, v in query.items()):
                return doc
            
        return None  
        