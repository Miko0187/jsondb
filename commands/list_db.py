from .base import Command
from classes import Manager, Session

class ListDb(Command):
    name = "list_db"
    requires_login = True
    requires_db = False
    requires_coll = False

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")
        dbs = manager.get_dbs()
        
        await session.operation("ok", {"result": dbs}, data_id=data_id)
