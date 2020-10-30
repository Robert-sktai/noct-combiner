import yaml
import logging

class Config():
    def __init__(self, config_path=r"./conf/config.yaml"):
        self.documents = dict()
        with open(config_path) as file:
            for items in yaml.full_load(file):
                for key, value in items.items():
                    self.documents[key] = value

        self.server_num_workers = self.documents["server"]["num_workers"]
        self.bigtable_project_id = self.documents["bigtable"]["project_id"]
        self.bigtable_instance_id = self.documents["bigtable"]["instance_id"]
        self.bigtable_table_id = self.documents["bigtable"]["table_id"]
        self.logger_file = self.documents["logger"]["file"]
        self.logger_level = logging.getLevelName(self.documents["logger"]["level"])
        self.logger_max_bytes = self.documents["logger"]["max_bytes"]
        self.logger_backup_count = self.documents["logger"]["backup_count"]

if __name__ == "__main__":
    config = Config(r"./conf/config.yaml")
