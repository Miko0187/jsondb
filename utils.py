import os
import json
import sys
import getpass
from argon2 import PasswordHasher

def setup_data(dir: str):
    if not os.path.isdir(f"{dir}/files"):
        os.mkdir(f"{dir}/files")
        
    if not os.path.isfile(f"{dir}/files/jsondb.json"):
        root_pw = getpass.getpass("Please enter the new root password > ")
        root_pw_again = getpass.getpass("Please enter the new root password again > ")
        
        if root_pw != root_pw_again:
            print("The password is not the same. exiting...")
            
            sys.exit(1)
         
        if " " in root_pw:
            print("The password cant contain spaces")
            
        ph = PasswordHasher()
        
        with open(f"{dir}/files/jsondb.json", "w") as f:
            json.dump(
                {
                    "root": ph.hash(root_pw),
                },
                f
            )
            
def create_config():
    with open("config.json", "w") as f:
        json.dump(
            {
                "address": "0.0.0.0",
                "port": 8989,
                "db_files": "."
            }, 
            f, 
            indent=4
        )
        
    return {
        "address": "0.0.0.0",
        "port": 8989,
        "db_files": "."
    }
    
def get_user(data: str, user: str):
    with open(f"{data}/files/jsondb.json", "r") as f:
        try:
            users = json.load(f)
        except json.JSONDecodeError:
            return None
        
    return users.get(user)

def db_exists(data: str, name: str):
    found = False
    for dir in os.scandir(f"{data}/files"):
        if dir.is_dir():
            if dir.name == name:
                found = True
                
                break
            
    return found
