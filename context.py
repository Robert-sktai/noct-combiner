import os

from metadata import Metadata
from logger import LoggerFactory

class Context:
    def __init__(self, incoming_data_path, bigtable_instance_id, bigtable_table_id, collect_statistics):
        self.logger_factory = LoggerFactory()
        self.cwd = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(self.cwd, "metadata.db")
        self.metadata = Metadata(self.db_path)

        self.incoming_data_path = incoming_data_path 
        self.bigtable_instance_id = bigtable_instance_id
        self.bigtable_table_id = bigtable_table_id
        self.collect_statistics = collect_statistics
