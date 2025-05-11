import os
import json
import sys
from argon2 import PasswordHasher

def setup_data(dir: str):
    if not os.path.isdir(f"{dir}/files"):
        os.mkdir(f"{dir}/files")
        
    if not os.path.isfile(f"{dir}/files/jsondb.json"):
        root_pw = os.environ.get("ROOT_PASSWORD")
        if not root_pw:
            return "Make sure to for the first launch to set the enviroment variable \"ROOT_PASSWORD\" to the root password you want to set"
            
        ph = PasswordHasher()
        try:
            hashed = ph.hash(root_pw)
        except:
            return "Failed to Hash password"
        with open(f"{dir}/files/jsondb.json", "w") as f:
            json.dump({
                "root": {
                    "password": hashed,
                    "global_permissions": ["admin"]
                }
            }, f)
    
    return None
    