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
            
        ph = PasswordHasher()
        with open(f"{dir}/files/jsondb.json", "w") as f:
            json.dump({"root": ph.hash(root_pw)}, f)
            
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
    