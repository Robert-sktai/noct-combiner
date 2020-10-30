import os
import logging
import multiprocessing


from config import Config
from metadata import Metadata
from logging import handlers
from listener import Listener
from process import Process
from file_manager import FileManager
from worker import Worker

def root_configurer(log_queue, level):
    h = handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(level)

class Coordinator(Process):
    def __init__(self, log_queue, manager):
        super().__init__(log_queue=log_queue)
        self.manager = manager

    def run(self):
        pending_tasks_dict = self.manager.dict()
        for table_name in self.metadata.get_swing_migration_tables():
            pending_tasks_dict[table_name] = self.manager.Queue(-1)
        done_tasks = self.manager.Queue(-1)

        processes.append(FileManager(log_queue, pending_tasks_dict, done_tasks))
        index = 1
        for table_name in self.metadata.get_swing_migration_tables():
            worker = Worker(log_queue, index, pending_tasks_dict[table_name], done_tasks, table_name)
            processes.append(worker)
            index += 1

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        self.logger.info("Bye :)")

if __name__ == "__main__":
    manager = multiprocessing.Manager()
    processes = list()
    config = Config()
    log_queue = multiprocessing.Queue(-1)
    processes.append(Listener(log_queue))
    root_configurer(log_queue, config.logger_level)
    coordinator = Coordinator(log_queue, manager)
    coordinator.start()
    coordinator.join()
