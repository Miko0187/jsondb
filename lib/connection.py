import json
import uuid
import zstd
import struct
import asyncio
import logging
from .error import *
from .database import Database

HEADER_SIZE = 4 # Bytes

logger = logging.getLogger(__name__)

class Connection:
    """
    Represents a client connection to a JsonDB server.
    """
    
    def __init__(
        self,
        host: str,
        port: int
    ):
        """
        Initializes a new Connection instance.

        Args:
            host (str): The server hostname or IP.
            port (int): The server port number.
        """
        self.host = host
        self.port = port
        
        self.database = None
        
        self._reader = None
        self._writer = None
        self._authed = False
        self._zstd = False
        self._listen_task = None

        self._pending_requests: dict[str, asyncio.Future] = {}
        self._event_handlers: dict[str, list[any]] = {}
        
    def _raise_error(self, error: str, id: str, *args, **kwargs):
        """
        Internal. Raises an exception based on an error code received from the server.

        Args:
            error (str): Error type identifier.
            id (str): Associated request ID.
            prefix (str, optional): Context for resource-related errors.
            name (str, optional): Resource name for context.
        """
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
                raise DoesntExistException(*args, **kwargs)
            case "exists":
                raise ExistException(*args, **kwargs)
            case "non_open":
                raise NoDbOpenException
            case "client":
                raise ClientError
            case "permissions":
                raise PermissionError(*args, **kwargs)
            case _:
                raise RuntimeError(f"Unknown error '{error}'")
            
    async def _write(self, body: str):
        """
        Internal. Sends a raw message to the server, applying ZSTD compression if enabled.

        Args:
            body (str): UTF-8 string to send.
        """
        encoded = body.encode()
        if self._zstd:
            encoded = zstd.compress(encoded, 1)
        header = struct.pack(">I", len(encoded))

        body = header + encoded
        self._writer.write(body)
        await self._writer.drain()

    async def _read(self):
        """
        Internal. Reads a raw message from the server, handling decompression.

        Returns:
            str | None: The received message as UTF-8 string or None on disconnect.
        """
        try:
            header = await self._reader.readexactly(HEADER_SIZE)
        except asyncio.IncompleteReadError:
            return None
        
        msg_len = struct.unpack(">I", header)[0]
        data = await self._reader.readexactly(msg_len)
        if self._zstd:
            data = zstd.uncompress(data)
        return data.decode()
                    
    async def _send(self, op: str, data: dict = None, response: bool = True) -> dict | None:
        """
        Internal. Sends a structured command to the server and optionally awaits a response.

        Args:
            op (str): The operation identifier.
            data (dict, optional): Operation-specific payload.
            response (bool): Whether a response is expected.

        Returns:
            dict | None: The response dictionary if awaited.
        """
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
            
        await self._write(json.dumps(_req))

        if response:
            return await future

    async def connect(
        self, 
        name: str, 
        password: str,
        zstd: bool = True,
        reconnect: bool = True, 
        retries: int = 0
    ):
        """
        Establishes a connection and authenticates with the server. Retries if configured.
        """
        self._zstd = zstd
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
        """
        Internal. Handles low-level connection setup and authentication handshake.
        """
        reader, writer = await asyncio.open_connection(self.host, self.port)
        self._reader = reader
        self._writer = writer
        self._listen_task = asyncio.create_task(self._listen())

        await self._auth(name, password)
        await self._reg_events()

    async def _auth(self, name: str, password: str):
        """
        Internal. Sends credentials and performs the auth negotiation.
        """
        temp = self._zstd
        self._zstd = False
        req = await self._send("auth", {
            "name": name,
            "password": password,
            "zstd": temp
        })

        if req.get("op") == "authed":
            self._zstd = temp
            self._authed = True
        else:
            self._raise_error(req["error"], req["id"])

    async def _reg_events(self):
        """
        Internal. Registers event subscriptions.
        """
        if len(list(self._event_handlers.keys())) == 0:
            return

        req = await self._send("event_sub", {
            "events": list(self._event_handlers.keys())
        })

        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"])

    async def _listen(self):
        """
        Internal. Background task that continuously reads messages and dispatches events.
        """
        while True:
            try:
                req = await self._read()
                if not req:
                    continue
                req = json.loads(req.strip())

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
        """
        Internal. Dispatches a server event to all registered handlers.
        """
        listeners = self._event_handlers.get(event)

        if listeners == None or len(listeners) == 0:
            return
        
        for listener in listeners:
            task = asyncio.create_task(listener(data))
            task.add_done_callback(
                lambda t: logger.exception(f"Error in event: {t.exception()}") if t.exception() else None
            )

    def event(self, name: str):
        """
        Registers a coroutine function as an event listener.

        Args:
            name (str): The event name.
        """
        def decorator(func):
            if self._event_handlers.get(name) == None:
                self._event_handlers[name] = list()

            self._event_handlers[name].append(func)
        return decorator
                
    def close(self):
        """
        Closes the connection to the server and cancels background listeners.
        """
        self._listen_task.cancel()
        self._writer.close()
            
    async def open_database(self, name: str):
        """
        Opens a database on the server.

        Args:
            name (str): The name of the database.

        Returns:
            Database: The database instance.
        """
        req = await self._send("open_db", {
            "name": name
        })
    
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], prefix="database", name=name, action="open_database")
        else:
            self.database = Database(self, name)
            return self.database
            
    async def create_database(self, name: str):
        """
        Creates a new database on the server.

        Args:
            name (str): Name of the database to create.
        """
        req = await self._send("create_db", {
            "name": name
        })
        
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], prefix="database", name=name, action="create_database")
            
    async def list_databases(self) -> list[str]:
        """
        Lists all databases available on the server.

        Returns:
            list[str]: A list of database names.
        """
        req = await self._send("list_db")
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], prefix="database", action="list_databases")
        else:
            return req["d"]["result"]
    
    async def delete_database(self, name: str):
        """
        Deletes a database from the server.

        Args:
            name (str): The name of the database to delete.
        """
        req = await self._send("delete_db", {
            "name": name
        })
            
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], prefix="database", name=name, action="delete_database")
        