import os
import logging
import signal
import multiprocessing

from logging import handlers
from worker import Worker
from listener import Listener 

logger = logging.getLogger(__name__)

def root_configurer(log_queue):
    h = handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.DEBUG)

if __name__ == "__main__":
    log_queue = multiprocessing.Queue(-1)
    listener = Listener(log_queue)
    listener.start()

    root_configurer(log_queue)
    logger.info('Logging from main')

    workers = list()
    for index in range(0, 32):
        worker = Worker(log_queue, index)
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()
