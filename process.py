import multiprocessing
import logging
import signal

from config import Config
from metadata import Metadata

class Process(multiprocessing.Process):
    def __init__(self, log_queue, level=None, name=None):
        super().__init__()
        self.stop_event = multiprocessing.Event()
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.config = Config()
        self.metadata = Metadata(self.config.metadata_file)
        self.name = name if name is not None else type(self).__name__
        self.log_queue = log_queue
        self.logger = logging.getLogger(self.name) 
        self.logger.setLevel(level if level is not None else self.config.logger_level)

    def signal_handler(self, signum, frame):
        self.stop()

    def stop(self):
        self.stop_event.set()

    def stopped(self):
        return self.stop_event.is_set()

    def get_name(self):
        return name

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def critical(self, msg):
        self.logger.critical(msg)

if __name__ == "__main__":
    process = Process(log_queue=None)
