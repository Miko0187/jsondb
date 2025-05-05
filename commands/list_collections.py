from .base import Command
from classes import Manager, Session

class ListCollections(Command):
    name = "list_collections"
    requires_login = True
    requires_db = True
    requires_coll = False

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")    
        await session.operation("ok", {"result": session.db.list_collections()}, data_id=data_id)
            