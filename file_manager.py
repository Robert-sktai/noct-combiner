import os
import queue
import time
import logging
import datetime
import shutil

from datetime import date
from config import Config
from process import Process

class Statistics:
    def __init__(self, slack_queue, config, logger):
        self.reset()
        self.slack_queue = slack_queue
        self.config = config
        self.logger = logger

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

    def reset(self):
        self.last = time.time()
        self.total_files = dict()

    def elapsed_time(self):
        duration = time.time() - self.last
        return duration

    def put(self, path):
        table_name = self.get_table_name_from_path(path)
        self.debug(f"stat.put: {table_name}")
        if table_name not in self.total_files:
            self.total_files[table_name] = 1
        else:
            self.total_files[table_name] = self.total_files[table_name] + 1

    def check_and_notify(self):
        curr_val = int(self.elapsed_time())
        interval = int(self.config.report_interval)
        self.debug(f"curr_val:{curr_val} interval:{interval}")

        if curr_val > interval:
            self.notify()

    def notify(self):
        today = date.today().strftime("%b-%d-%Y")
        msg = f"\n* Summary information: {today}\n"
        msg += "======================================================\n"
        idx = 1
        for key,value in sorted(self.total_files.items()):
            msg += f"[{idx}] {key}: {value} are processed.\n"
            idx += 1
        msg += "======================================================\n"
        self.slack_queue.put(msg)
        self.info(msg)
        self.reset()

    def get_table_name_from_path(self, path):
        try:
            file_name = path[path.rindex("/")+1:]
            table_name = file_name[:file_name[:file_name.rindex("_")].rindex("_")].lower()
            return table_name
        except ValueError:
            self.error(f"Parsing error, but ignored: unexpected .dat file name format: {path}")
        return None

class FileManager(Process):
    def __init__(self, log_queue, pending_tasks, done_tasks, error_tasks, slack_queue):
        super().__init__(log_queue=log_queue)

        self.last_subdir = ""
        self.swing_migration_tables = self.metadata.get_swing_migration_tables()
        self.pending_tasks = pending_tasks
        self.done_tasks = done_tasks
        self.error_tasks = error_tasks
        self.error_set = set()
        self.slack_queue = slack_queue
        self.prev_subdir = None
        self.stat = Statistics(self.slack_queue, self.config, self.logger)

    def run(self):
        is_empty_task = False
        while not self.stopped():
            try:
                self.move_error_tasks()
                size_of_done_tasks = self.close_tasks()
                size_of_collected_tasks = self.collect_tasks()
                self.stat.check_and_notify()
                if (size_of_collected_tasks + size_of_done_tasks == 0):
                    if not is_empty_task:
                        self.info(f"Waiting for new incoming tasks.\nThe size of pending task queue: {self.pending_tasks.qsize()}, The size of done task queue: {self.done_tasks.qsize()}")
                        is_empty_task = True
                        self.remove_expiry()
                    time.sleep(0.5)
                else:
                    is_empty_task = False
                time.sleep(5)
            except Exception as e:
                msg = f"Unexpected error: {e}"
                self.error(msg)
#                self.slack_queue.put(msg)
                continue
        self.debug("Exit FileManager")

    def get_subdirs(self):
        return list(sorted(filter(lambda x: not( (x.endswith("_tmp") or (len(x)-x.rfind("_")-1 > 3))), [f.path for f in os.scandir(self.config.incoming_data_path) if f.is_dir()])))

    def validate(self, subdirs):
        self.prev_subdir = None
        for subdir in subdirs:
            prev = None if self.prev_subdir is None else self.prev_subdir[self.prev_subdir.rfind("_")+1:]
            curr = subdir[subdir.rfind("_")+1:]
            failed = False
            if prev is not None:
                if curr == '001':
                    if prev != '288':
                        failed = True
                else:
                    if int(prev)+1 != int(curr):
                        failed = True
            if failed:
                msg = f"""Found a missing subdir between '{self.prev_subdir}' and '{subdir}'"""
                self.error(msg)
                self.slack_queue.put(msg)
            self.prev_subdir = subdir

    def is_expired(self, path):
        expiry_baseline = datetime.date.today() - datetime.timedelta(days=self.config.data_expiry)
        return datetime.date.fromtimestamp(os.path.getctime(path)) <= expiry_baseline

    def remove_expiry(self):
        subdirs = list(filter(lambda x: self.is_expired(x), [f.path for f in os.scandir(self.config.incoming_data_path) if f.is_dir()]))
        for subdir in subdirs:
            self.info(f"Removed due to data expiry: {subdir}")
            shutil.rmtree(subdir)

    def collect_tasks(self):
        if self.pending_tasks.empty():
            # To collect and retry failed data
            subdirs = self.get_subdirs()
        else:
            subdirs = list(filter(lambda x: x > self.last_subdir, self.get_subdirs()))
        self.validate(subdirs)
        if len(subdirs) > 0:
            self.last_subdir = subdirs[-1]
        size = 0
        for subdir in subdirs:
            self.debug(f"Try to collect tasks from the directory: {subdir}")
            dat_files = list(sorted(filter(lambda x: x.endswith(".dat"), [f.path for f in os.scandir(subdir) if f.is_file()])))
            size += len(dat_files)
            for dat_file in dat_files:
                if dat_file in self.error_set:
                    continue
                self.pending_tasks.put(dat_file)
                self.debug(f"collected task: {dat_file}")
        return size

    def move_error_tasks(self):
        while not self.error_tasks.empty():
            error_task = self.error_tasks.get()
            if not os.path.isfile(error_task):
                continue
            self.error_set.add(error_task)
            self.debug(f"Error task: {error_task}")

    def close_tasks(self):
        size = self.done_tasks.qsize()
        while not self.done_tasks.empty():
            done_task = self.done_tasks.get()
            if not os.path.isfile(done_task):
                continue
            self.stat.put(done_task)
            os.rename(done_task, done_task + ".done")
            self.debug(f"Closed task: {done_task}")
        return size
