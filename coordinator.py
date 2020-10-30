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

if __name__ == "__main__":
    processes = list()
    config = Config()
    metadata = Metadata(config.metadata_file)
    log_queue = multiprocessing.Queue(-1)
    processes.append(Listener(log_queue))
    root_configurer(log_queue, config.logger_level)
    logger = logging.getLogger(__name__)

    pending_tasks_dict = dict()
    for table_name in metadata.get_swing_migration_tables():
        pending_tasks_dict[table_name] = multiprocessing.Queue(-1)
    done_tasks = multiprocessing.Queue(-1)

    processes.append(FileManager(log_queue, pending_tasks_dict, done_tasks))
    index = 1
    for table_name in metadata.get_swing_migration_tables():
        worker = Worker(log_queue, index, pending_tasks_dict[table_name], done_tasks, table_name)
        processes.append(worker)
        index += 1

    for process in processes:
        process.start()

    for process in processes:
        process.join()

    logger.info("Bye :)")
