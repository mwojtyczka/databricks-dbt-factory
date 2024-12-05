import json


class FileHandler:

    @staticmethod
    def read(path: str):
        with open(path, 'r', encoding="utf-8") as file:
            return json.load(file)
