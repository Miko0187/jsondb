from .base import Command
from classes import Manager, Session, Permissions

class FindAllDoc(Command):
    name = "find_all_doc"
    requires_login = True
    requires_db = True
    requires_coll = True
    permission = Permissions.READ

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")    
        data_d = request.get("d")       
        coll = data_d.get("collection")
        if not coll:
            await session.error("format", data_id)
            return
        
        if session.db.collection_exists(coll) == None:
            await session.error("doesnt_exist", data_id)
            return
        coll = session.db.get(coll)
        
        query = data_d.get("query")
        if not query:
            await session.error("format", data_id)
            return
        
        await session.operation("ok", {"result": coll.find_one(query)}, data_id=data_id)
