import threading
import logging

from context import Context

class Thread(threading.Thread):
    def __init__(self, context, level=logging.INFO, name=None):
        super().__init__()
        self.stop_event = threading.Event()
        self.context = context
        self.name = name if name is not None else type(self).__name__
        self.logger = self.context.logger_factory.issue_logger(self.name, level)

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
