import json
import csv

class JSONHandler:

    def __init__(self, json_file_path: str) -> None:
        self.json_file_path = json_file_path

    def json_to_csv(self, csv_file_path: str) -> None:

        with open(self.json_file_path, 'r') as json_file:
            data = json.load(json_file)

        with open(csv_file_path, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(data)