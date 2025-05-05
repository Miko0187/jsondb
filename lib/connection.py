import json
import uuid
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
        self._listen_task = None

        self._pending_requests: dict[str, asyncio.Future] = {}
        self._event_handlers: dict[str, list[any]] = {}
        
    def raise_error(self, error: str, id: str, prefix: str = "Something", name: str = "Unknown"):
        if id in self._pending_requests:
            self._pending_requests.pop(id).set_exception(RuntimeError(f"JsonDB Error {error}"))

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
            case "client":
                raise ClientError
            case _:
                raise RuntimeError(f"Unknown error '{error}'")
                    
    async def _send(self, op: str, data: dict = None, response: bool = True) -> dict | None:
        request_id = str(uuid.uuid4())

        _req = {
            "op": op,
            "id": request_id
        }
        
        if data != None:
            _req["d"] = data

        if response:
            future = asyncio.get_event_loop().create_future()
            self._pending_requests[request_id] = future
            
        self._writer.write(json.dumps(_req).encode() + b"\n\r\n\r")
        await self._writer.drain()

        if response:
            return await future

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
        reader, writer = await asyncio.open_connection(self.host, self.port)
        self._reader = reader
        self._writer = writer
        self._listen_task = asyncio.create_task(self._listen())

        await self._auth(name, password)
        await self._reg_events()

    async def _auth(self, name: str, password: str):
        req = await self._send("auth", {
            "name": name,
            "password": password
        })

        if req.get("op") == "authed":
            self._authed = True
        else:
            self.raise_error(req["error"], req["id"])

    async def _reg_events(self):
        if len(list(self._event_handlers.keys())) == 0:
            return

        req = await self._send("event_sub", {
            "events": list(self._event_handlers.keys())
        })

        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"])

    async def _listen(self):
        while True:
            try:
                req = await self._reader.readuntil(b"\n\r\n\r")
                req = req.decode().strip()
                req = json.loads(req)

                print(req)

                if req.get("op") == "event":
                    await self._dispatch_event(req["d"]["ev"], req["d"]["d"])
                else:
                    req_id = req.get("id")

                    if req_id in self._pending_requests:
                        self._pending_requests.pop(req_id).set_result(req)
            except json.JSONDecodeError as e:
                logger.exception(f"Error while loading JSON: {e}")
            except UnicodeDecodeError as e:
                logger.exception(f"Unicode error")

    async def _dispatch_event(self, event: str, data: dict):
        listeners = self._event_handlers.get(event)

        if listeners == None or len(listeners) == 0:
            return
        
        for listener in listeners:
            task = asyncio.create_task(listener(data))
            task.add_done_callback(
                lambda t: logger.exception(f"Error in event: {t.exception()}") if t.exception() else None
            )

    def event(self, name: str):
        def decorator(func):
            if self._event_handlers.get(name) == None:
                self._event_handlers[name] = list()

            self._event_handlers[name].append(func)
        return decorator
                
    async def close(self):
        self._listen_task.cancel()
        self._writer.close()
            
    async def open_database(self, name: str):
        req = await self._send("open_db", {
            "name": name
        })
    
        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"], "database", name)
        else:
            self.database = Database(self, name)
            return self.database
            
    async def create_database(self, name: str):
        req = await self._send("create_db", {
            "name": name
        })
        
        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"], "database", name)
            
    async def list_databases(self) -> list[str]:
        req = await self._send("list_db")
            
        return req["d"]["result"]
    
    async def delete_database(self, name: str):
        req = await self._send("delete_db", {
            "name": name
        })
            
        if req.get("op") != "ok":
            self.raise_error(req["error"], req["id"], "database", name)
        