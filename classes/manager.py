import os
import json
import asyncio
from datetime import datetime
from .db import Database
from .collection import Collection
from .ratelimit import RateLimiter
from .eventmanager import EventManager
from .security import UserManagement

class Manager:
    def __init__(self, save_dir: str):
        self.save_dir = save_dir
        self.dbs: dict[str, Database] = dict()
        self.userm = UserManagement(self.save_dir)
        self.ratelimiter = RateLimiter(auth_limit=3, interval=60, delay=10)
        self.event_manager = EventManager(self)

        self._log_queue = asyncio.Queue()
        self._save_queue = asyncio.Queue()
        self._save_lock = asyncio.Lock()

        self._log_task = None
        self._save_task = None

    def init(self):
        self.userm.init()

        self._log_task = asyncio.create_task(self._log_worker())
        self._save_task = asyncio.create_task(self._save_worker())
        self.ratelimiter.start()

        for dir in os.scandir(f"{self.save_dir}/files"):
            if dir.is_dir():
                db = Database(dir.name, self)
                db.load()
                self.dbs[db.db_name] = db

    def stop(self):
        self._log_task.cancel()
        self._save_task.cancel()
        self.ratelimiter.stop()

    def create_db(self, name: str):
        if self.dbs.get(name):
            return False
        
        try:
            os.mkdir(f"{self.save_dir}/files/{name}")
        except FileExistsError:
            pass
        except PermissionError:
            return False
        db = Database(name, self)
        db.load()
        self.dbs[db.db_name] = db

        return True

    def get_db(self, name: str):
        return self.dbs.get(name)
    
    def get_dbs(self):
        dbs = []
        for db in self.dbs.keys():
            dbs.append(db)

        return dbs
    
    def delete_db(self, name: str):
        if not self.dbs.get(name):
            return False
        
        try:
            os.rmdir(f"{self.save_dir}/files/{name}")
        except FileNotFoundError:
            pass
        except PermissionError:
            return False
        self.dbs.pop(name)

        return True

    def get_user(self, user: str):
        return self.userm.get_user(user)
        # return self.users.get(user)
    
    async def log(self, addr: tuple, msg: str, db: str = None, coll: str = None):
        await self._log_queue.put((addr, msg, db, coll))

    async def save(self, collection: "Collection"):
        await self._save_queue.put((collection))

    def _save_sync(collection: "Collection"):
        with open(collection.path, "w") as f:
            json.dump(collection.data, f)

    async def _save_worker(self):
        while True:
            collection = await self._save_queue.get()
            try:
                async with self._save_lock:
                    await asyncio.to_thread(self._save_sync, collection)
            finally:
                self._save_queue.task_done()

    async def _log_worker(self):
        while True:
            addr, msg, db, collection = await self._log_queue.get()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] [{addr[0]}:{addr[1]}]{f'/{db}' if db != None else ''}{f'/{collection}' if collection != None else ''} {msg}")   
            self._log_queue.task_done()
