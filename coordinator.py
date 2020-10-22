import os
import logging

from context import Context
from thread import Thread
from file_manager import FileManager
from worker import Worker

class Coordinator(Thread):
    def __init__(self):
        incoming_data_path = "/home/svcapp_su/robert/workspace/noct-combiner/swing"
        bigtable_instance_id="sktai-noct-poc"
        bigtable_table_id="noct"
        collect_statistics = True

        context = Context(incoming_data_path, bigtable_instance_id, bigtable_table_id, collect_statistics)
        super().__init__(context=context, level=logging.INFO)

        self.file_manager = FileManager(self.context)
        self.workers = list()
        index = 1
        for table_name in self.context.metadata.get_swing_migration_tables():
            self.workers.append(Worker(self.context, index, self.file_manager, table_name))
            index += 1

    def run(self):
        self.file_manager.start()
        self.info("Start workers.")
        for worker in self.workers:
            worker.start()

if __name__ == "__main__":
    coordinator = Coordinator()
    coordinator.start()
