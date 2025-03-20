import json
import asyncio
import logging
from .error import *
from .database import Database


logger = logging.getLogger(__name__)

class Connection:
    def __init__(
        self,
        host: str,
        port: int
    ):
        self.host = host
        self.port = port
        
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
            case "non_open":
                raise NoDbOpenException
            case _:
                raise RuntimeError(f"Unknown error '{error}'")
                    
    async def _send(self, op: str, data: dict = None):
        _req = {
            "op": op
        }
        
        if data != None:
            _req["d"] = data
            
        self._writer.write(json.dumps(_req).encode() + b"\n\r\n\r")
        await self._writer.drain()
        
    async def _read(self) -> dict:
        req = await self._reader.readuntil(b"\n\r\n\r")
        req = req.decode().strip()
        req = json.loads(req)
        
        return req

    async def connect(
        self, 
        name: str, 
        password: str, 
        reconnect: bool = True, 
        retries: int = 0
    ):
        tries = 0

        while True:
            try:
                await self._connect(name, password)
                break
            except ConnectionRefusedError:
                tries += 1
                
                if tries >= retries and retries != 0:
                    raise ConnectionRefusedError("Failed to connect")
                    
                if reconnect:
                    logger.warning(f"Failed to connect. Retrying in 10 seconds. ({tries}/{retries if retries != 0 else '∞'})")
                    await asyncio.sleep(10)
                else:
                    raise ConnectionRefusedError("Failed to connect")

    async def _connect(self, name: str, password: str):
        async with self._mutex:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            self._reader = reader
            self._writer = writer
            
            req = await self._read()
            
            if req.get("op") == "auth":
                await self._auth(name, password)
            else:
                self.raise_error(req["error"])

    async def _auth(self, name: str, password: str):
        await self._send("auth", {
            "name": name,
            "password": password
        })

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
        