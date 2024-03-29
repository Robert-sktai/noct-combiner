import yaml
import logging
import os

cwd = os.path.dirname(os.path.abspath(__file__))
config_path = r"conf/config.yaml"

class Config():
    def __init__(self, config_path=config_path):
        self.documents = dict()
        with open(config_path) as file:
            for items in yaml.full_load(file):
                for key, value in items.items():
                    self.documents[key] = value

        self.incoming_data_path = self.documents["incoming_data_path"]
        self.num_workers = self.documents["num_workers"]
        self.report_interval = self.documents["report_interval"]
        self.data_expiry = self.documents["data_expiry"]

        self.bigtable_project_id = self.documents["bigtable"]["project_id"]
        self.bigtable_instance_id = self.documents["bigtable"]["instance_id"]
        self.bigtable_app_profile_id = self.documents["bigtable"]["app_profile_id"]

        self.logger_file = self.documents["logger"]["file"]
        self.logger_level = logging.getLevelName(self.documents["logger"]["level"])
        self.logger_max_bytes = self.documents["logger"]["max_bytes"]
        self.logger_backup_count = self.documents["logger"]["backup_count"]

        self.metadata_file = self.documents["metadata"]["file"]
        self.metadata_rowkey_url = self.documents["metadata"]["rowkey_url"]

        self.slack_username = self.documents["slack"]["username"]
        self.slack_channel = self.documents["slack"]["channel"]
        self.slack_icon_emoji = self.documents["slack"]["icon_emoji"]

if __name__ == "__main__":
    config = Config(r"./conf/config.yaml")
    print (config.documents)
