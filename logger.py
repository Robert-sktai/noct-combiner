import logging
import logging.handlers

class LoggerFactory:
    def __init__(self):
        format = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
        logging.basicConfig(format=format, datefmt="%m-%d %H:%M:%S")
        self.handler = logging.handlers.RotatingFileHandler(
                      "my.log", maxBytes=512*1024*1024, backupCount=5)
        self.handler.setFormatter(logging.Formatter(format))

    def issue_logger(self, name, level):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(self.handler)
        return logger
