import json
import os

from src.utils.project_paths import DATA_RAW


class JsonHanddler:

    def save_to_json(self, data, filename=os.path.join(DATA_RAW, "backup_transfer_timestamp.json")):
        with open(filename, 'w') as file:
            json.dump(data, file, default=str)

    def load_from_json(self, filename=os.path.join(DATA_RAW, "backup_transfer_timestamp.json")):
        try:
            with open(filename, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return None
