import os
import logging
import signal
import multiprocessing

from config import Config
from logging import handlers
from worker import Worker
from listener import Listener 

def root_configurer(log_queue, level):
    h = handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(level)

if __name__ == "__main__":
    config = Config()
    log_queue = multiprocessing.Queue(-1)
    listener = Listener(log_queue)
#    listener.start()

    root_configurer(log_queue, config.logger_level)
    logger = logging.getLogger(__name__)
    logger.info("* start")

    workers = list()
    for index in range(0, config.server_num_workers):
        worker = Worker(log_queue, index)
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()
    logger.info("Bye :)")
