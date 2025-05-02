import json
import os

def load_domain_config(spider_name):
    config_path = f"configs/domains/{spider_name}.json"
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return json.load(f)
    return {}