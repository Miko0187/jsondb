import typing
from .collection import Collection
if typing.TYPE_CHECKING:
    from .connection import Connection

class Database:
    """
    Represents a remote database instance on the JsonDB server.

    Provides methods to manage and interact with collections in the database.
    Instances of this class should be obtained through `Connection.open_database()`.
    """

    def __init__(self, connection: "Connection", name: str):
        self.name = name
        self._connection = connection
        
        self._raise_error = self._connection._raise_error
        self._send = self._connection._send
        
    async def create_collection(self, name: str):
        """
        Creates a new collection inside the current database.

        Args:
            name (str): The name of the collection to create.

        Raises:
            Error: If the collection already exists or creation fails.
        """
        req = await self._send("create_collection", {
            "name": name
        })
            
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], prefix="Collection", name=name, action="create_collection")
            
    async def list_collections(self) -> list[str]:
        """
        Retrieves a list of all collections in the current database.

        Returns:
            list[str]: List of collection names.

        Raises:
            Error: If the server returns an error.
        """
        req = await self._send("list_collections")
            
        if req["op"] != "ok":
            self._raise_error(req["error"], req["id"], action="list_collections")
        
        return req["d"]["result"]
        
    async def get_collection(self, name: str) -> Collection | None:
        """
        Returns a Collection instance if the collection exists in the database.

        Args:
            name (str): The name of the collection.

        Returns:
            Collection | None: The collection instance, or None if not found.
        """
        colls = await self.list_collections()
        
        for coll in colls:
            if coll == name:
                return Collection(self, coll)
            
        return None
        
    async def delete_collection(self, name: str):
        """
        Deletes a collection from the database.

        Args:
            name (str): The name of the collection to delete.

        Raises:
            Error: If the deletion fails.
        """
        req = await self._send("delete_collection", {
            "name": name
        })
            
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], prefix="Collection", name=name, action="delete_collection")
        