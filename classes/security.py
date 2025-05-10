import json
from argon2 import PasswordHasher

ph = PasswordHasher()

class Permissions:
    READ = "read"
    WRITE = "write"
    CREATE_COLL = "create_coll"
    DELETE_COLL = "delete_coll"
    CREATE_DB = "create_db"
    DELETE_DB = "delete_db"
    ADMIN = "admin"

class User:
    def __init__(self, username: str, hashed_pw: str, permissions: list[str]):
        self.username = username
        self.hashed_pw = hashed_pw
        self.permissions = set(permissions)

    def has_permissions(self, permission: str) -> bool:
        return permission in self.permissions or Permissions.ADMIN in self.permissions
    
    def verify_password(self, password: str) -> bool:
        try:
            return ph.verify(self.hashed_pw, password)
        except:
            return False
        
class UserManagement:
    def __init__(self, save_dir: str):
        self.save_dir = None
        self.users: list[User] = list()
        self._user_file = f"{self.save_dir}/files/jsondb.json"

    def init(self):
        with open(self._user_file, "r") as f:
            content = json.load(f)

        for name, data in content.items():
            self.users.append(User(
                name,
                data["password"],
                data["permissions"]
            ))
        
    def _save(self):
        raw = {
            u.username: {
                "password": u.hashed_pw,
                "permissions": list(u.permissions)
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
    
    def create_user(self, username: str, password: str, permissions: list[str]):
        if self.get_user(username):
            raise ValueError("User already exists")

        hashed = ph.hash(password)
        self.users[username] = User(username, hashed, permissions)
        self._save()
