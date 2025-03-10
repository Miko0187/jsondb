import json
import asyncio
from error import *
from database import Database


class Connection:
    def __init__(
        self,
        host: str,
        port: int,
        name: str,
        password: str
    ):
        self.host = host
        self.port = port
        self.name = name
        self.password = password
        
        self.database = None
        
        self._reader = None
        self._writer = None
        self._authed = False
        self._mutex = asyncio.Lock()
        
    def raise_error(self, error: str, prefix: str = "Something", name: str = "Unknown"):
        match error:
            case "user":
                raise InvalidUser
            case "format":
                raise FormatException
            case "already_opened":
                raise DatabaseAlreadyOpenException
            case "doesnt_exist":
                raise DoesntExistException(prefix, name)
            case "exists":
                raise ExistException(prefix, name)
            case _:
                raise RuntimeError(f"Unknown error '{error}'")
                    
    async def _send(self, op: str, data: dict = None):
        _req = {
            "op": op
        }
        
        if data != None:
            _req["d"] = data
            
        self._writer.write(json.dumps(_req).encode() + b"\n")
        await self._writer.drain()
        
    async def _read(self) -> dict:
        req = await self._reader.readuntil(b"\n")
        req = req.decode().strip()
        req = json.loads(req)
        
        return req
        
    async def connect(self):
        async with self._mutex:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            self._reader = reader
            self._writer = writer
            
            req = await self._read()
            
            if req.get("op") == "auth":
                await self._send("auth", {
                    "name": self.name,
                    "password": self.password
                })
            else:
                self.raise_error(req["error"])
            
            req = await self._read()
            
            if req.get("op") == "authed":
                self._authed = True
            else:
                self.raise_error(req["error"])
                
    async def close(self):
        async with self._mutex:
            self._writer.close()
            
            await self._writer.wait_closed()
            
    async def open_database(self, name: str):
        async with self._mutex:
            await self._send("open_db", {
                "name": name
            })
            
            req = await self._read()
        
        if req.get("op") != "ok":
            self.raise_error(req["error"], "database", name)
        else:
            self.database = Database(self, name)
            return self.database
            
    async def create_database(self, name: str):
        async with self._mutex:
            await self._send("create_db", {
                "name": name
            })
            
            req = await self._read()
        
        if req.get("op") != "ok":
            self.raise_error(req["error"], "database", name)
            
    async def list_databases(self) -> list[str]:
        async with self._mutex:
            await self._send("list_db")
            
            req = await self._read()
            
        return req["d"]["result"]
    
    async def delete_database(self, name: str):
        async with self._mutex:
            await self._send("delete_db", {
                "name": name
            })
            
            req = await self._read()
            
        if req.get("op") != "ok":
            self.raise_error(req["error"], "database", name)
        