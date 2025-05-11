import json
from argon2 import PasswordHasher

ph = PasswordHasher()

class Permissions:
    READ = "read"
    WRITE = "write"
    USER = "user"
    CREATE_COLL = "create_coll"
    DELETE_COLL = "delete_coll"
    CREATE_DB = "create_db"
    DELETE_DB = "delete_db"
    ADMIN = "admin"

class User:
    def __init__(self, username: str, hashed_pw: str, global_permissions: list[str], db_permissions: dict[str, list[str]]):
        self.username = username
        self.hashed_pw = hashed_pw
        self.global_permissions = set(global_permissions)
        self.db_permissions = {db: set(perms) for db, perms in db_permissions.items()}

    def has_permission(self, permission: str, db: str = None) -> bool:
        if Permissions.ADMIN in self.global_permissions:
            return True

        if db:
            return permission in self.db_permissions.get(db, set())
        return permission in self.global_permissions
    
    def verify_password(self, password: str) -> bool:
        try:
            return ph.verify(self.hashed_pw, password)
        except:
            return False
        
class UserManagement:
    def __init__(self, save_dir: str):
        self.save_dir = save_dir
        self.users: list[User] = list()
        self._user_file = f"{self.save_dir}/files/jsondb.json"

    def init(self):
        with open(self._user_file, "r") as f:
            content = json.load(f)

        for name, data in content.items():
            self.users.append(User(
                name,
                data["password"],
                data.get("global_permissions", []),
                data.get("db_permissions", {})
            ))
        
    def _save(self):
        raw = {
            u.username: {
                "password": u.hashed_pw,
                "global_permissions": list(u.global_permissions),
                "db_permissions": u.db_permissions
            }
            for u in self.users
        }

        with open(self._user_file, "w") as f:
            json.dump(raw, f)

    def get_users(self):
        return self.users
    
    def get_user(self, name: str):
        for user in self.users:
            if user.username == name:
                return user
            
        return None
    
    def add_db_permission(self, username: str, db: str, permission: str):
        user = self.get_user(username)
        if user:
            user.db_permissions.setdefault(db, set()).add(permission)
            self._save()

    def remove_db_permission(self, username: str, db: str, permission: str):
        user = self.get_user(username)
        if user:
            if db in user.db_permissions:
                user.db_permissions[db].discard(permission)
                if not user.db_permissions[db]:
                    del user.db_permissions[db]
            self._save()
