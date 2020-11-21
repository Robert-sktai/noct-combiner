import logging
from logging import handlers
from time import sleep
from process import Process

class Listener(Process):
    def __init__(self, log_queue):
        super().__init__(log_queue)
        self.listener_configurer()

    def listener_configurer(self):
        root = logging.getLogger()
        format = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
        file_handler = handlers.RotatingFileHandler(filename=self.config.logger_file, maxBytes=int(eval(self.config.logger_max_bytes)), backupCount=int(self.config.logger_backup_count))
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter(format)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        root.addHandler(file_handler)
        root.addHandler(console_handler)

    def run(self):
        while not self.stopped():
            while not self.log_queue.empty():
                record = self.log_queue.get()
                cur_logger = logging.getLogger(record.name)
                cur_logger.handle(record)
                sleep(0.01)
