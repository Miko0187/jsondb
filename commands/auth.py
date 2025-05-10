from .base import Command
from argon2 import PasswordHasher
from classes import Manager, Session

class Auth(Command):
    name = "auth"
    requires_login = False
    requires_db = False
    requires_coll = False

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        if session.authed:
            data_id = request.get("id")
            return await session.error("already_authed", data_id)

        data_id = request.get("id")
        name = request.get("d", {}).get("name") or ""
        password = request.get("d", {}).get("password") or ""
        zstd = request.get("d", {}).get("zstd") or False
        
        user = manager.get_user(name)
        if user == None:
            await session.error("user", data_id)
            return
        
        if not user.verify_password(password):
            manager.ratelimiter.register_auth_attempt(addr[0], False)
            await session.error("user", data_id)
            return
        
        session.authed = True
        manager.ratelimiter.register_auth_attempt(addr[0], True)
        manager.event_manager.subs[session] = []

        await session.operation("authed", data_id=data_id)
        session.zstd = zstd
        await manager.log(addr, "authed")
