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
from notifier import Notifier

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
    listener = Listener(log_queue)
#    listener.start()
    root_configurer(log_queue, config.logger_level)
    logger = logging.getLogger(__name__)

    pending_tasks = multiprocessing.Queue(-1)
    done_tasks = multiprocessing.Queue(-1)
    error_tasks = multiprocessing.Queue(-1)
    slack_queue = multiprocessing.Queue(-1)

    processes.append(FileManager(log_queue, pending_tasks, done_tasks, error_tasks, slack_queue))
    processes.append(Notifier(log_queue, slack_queue))
    index = 1
    logger.info("* Program has been started...")
    for _ in range(0, config.num_workers):
        worker = Worker(log_queue, index, pending_tasks, done_tasks, error_tasks, slack_queue)
        processes.append(worker)
        index += 1

    for process in processes:
        process.start()

    for process in processes:
        process.join()

    logger.info("* Bye :)")
