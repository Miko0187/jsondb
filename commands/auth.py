from .base import Command
from argon2 import PasswordHasher
from classes import Manager, Session

ph = PasswordHasher()

class Auth(Command):
    name = "auth"
    requires_login = False
    requires_db = False
    requires_coll = False

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")
        name = request.get("d", {}).get("name") or ""
        password = request.get("d", {}).get("password") or ""
        
        user = manager.get_user(name)
        if user == None:
            await session.error("user", data_id)
            return
        
        try:
            ph.verify(user, password)
        except:
            manager.ratelimiter.register_auth_attempt(addr[0], False)
            await session.error("user", data_id)
            return
        
        session.authed = True
        manager.ratelimiter.register_auth_attempt(addr[0], True)
        manager.event_manager.subs[session] = []

        await session.operation("authed", data_id=data_id)
        await manager.log(addr, "authed")

class AlreadyAuthed(Command):
    name = "auth"
    requires_login = True
    requires_db = False
    requires_coll = False

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")
        await session.error("already_authed", data_id)
