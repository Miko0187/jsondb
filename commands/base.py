from abc import ABC, abstractmethod
from classes import Manager, Session

class Command(ABC):
    name: str
    requires_login: bool
    requires_db: bool
    requires_coll: bool
    permission: str = None

    @classmethod
    async def check_requirements(cls, session: Session, manager: Manager, request: dict) -> bool:
        if cls.requires_login and not session.authed:
            await session.error("unauthed", request.get("id"))
            return False
        if cls.requires_db and not session.db:
            await session.error("non_open", request.get("id"))
            return False
        if cls.requires_coll and not (session.db and request.get("d", {}).get("collection")):
            await session.error("format", request.get("id"))
            return False
        if cls.permission and not session.user.has_permission(cls.permission, session.db.db_name if session.db else None):
            await session.error("permissions", request.get("id"))
            return False
        return True

    @abstractmethod
    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        pass
