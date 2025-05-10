import typing
if typing.TYPE_CHECKING:
    from .database import Database

class Collection:
    """
    Represents a collection inside a JsonDB database.

    Provides CRUD operations (create, read, update, delete) for documents.
    Instances of this class are created via `Database.get_collection()`.
    """

    def __init__(self, database: "Database", name: str):
        """
        Initializes a Collection instance.

        Args:
            database (Database): The parent database this collection belongs to.
            name (str): The name of the collection.
        """
        self.name = name
        self.database = database
        
        self._raise_error = self.database._raise_error
        self._send = self.database._send
    
    async def insert(self, document: dict):
        """
        Inserts a new document into the collection.

        Args:
            document (dict): The document to insert.

        Raises:
            Error: If the insertion fails.
        """
        req = await self._send("insert_doc", {
            "collection": self.name,
            "dict": document
        })
            
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], "Collection", self.name)
            
    async def find_one(self, query: dict):
        """
        Finds a single document matching the query.

        Args:
            query (dict): The filter to apply.

        Returns:
            dict | None: The matched document, or None if nothing matches.

        Raises:
            Error: If the server returns an error.
        """
        req = await self._send("find_one_doc", {
            "collection": self.name,
            "query": query
        })
            
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], "Collection", self.name)
        else:
            return req["d"]["result"]
        
    async def find_all(self, query: dict = None):
        """
        Finds all documents matching the query.

        Args:
            query (dict, optional): The filter to apply. If None, returns all documents.

        Returns:
            list[dict]: A list of matching documents.

        Raises:
            Error: If the query fails.
        """
        req = await self._send("find_all_doc", {
            "collection": self.name,
            "query": query
        })
            
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], "Collection", self.name)
        else:
            return req["d"]["result"]
        
    async def update(self, query: dict, update: dict):
        """
        Updates all documents that match the query with the given update instructions.

        Args:
            query (dict): Filter to find documents.
            update (dict): The update to apply.

        Raises:
            Error: If the update operation fails.
        """
        req = await self._send("update_doc", {
            "collection": self.name,
            "query": query,
            "update": update
        })
            
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], "Collection", self.name)
            
    async def delete(self, query: dict):
        """
        Deletes all documents matching the given query.

        Args:
            query (dict): Filter to determine which documents to delete.

        Raises:
            Error: If the deletion fails.
        """
        req = await self._send("delete_doc", {
            "collection": self.name,
            "query": query
        })
            
        if req.get("op") != "ok":
            self._raise_error(req["error"], req["id"], "Collection", self.name)
    