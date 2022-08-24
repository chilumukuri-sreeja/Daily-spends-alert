import os
import yaml
from pathlib import Path

# Importing Configuration Settings
config_path = Path.cwd() / "config.yaml"
CONFIGURATION: dict = None
with open(config_path) as stream:
    CONFIGURATION = yaml.safe_load(stream)

CONFIGURATION["DATA_DIRECTORY"] = "/tmp"
ENVIRONMENT = os.environ.get("ENVIRONMENT")
if ENVIRONMENT == "dev" : CONFIGURATION["DATA_DIRECTORY"] = "Data"