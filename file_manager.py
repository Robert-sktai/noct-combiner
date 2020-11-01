import os
import queue
import time
import logging

from process import Process

class FileManager(Process):
    def __init__(self, log_queue, pending_tasks, done_tasks):
        super().__init__(log_queue=log_queue)

        self.last_subdir = ""
        self.swing_migration_tables = self.metadata.get_swing_migration_tables()
        self.pending_tasks = pending_tasks
        self.done_tasks = done_tasks

    def run(self):
        is_empty_task = False
        while not self.stopped():
            size_of_collected_tasks = self.collect_tasks()
            size_of_done_tasks = self.close_tasks()
            if (size_of_done_tasks + size_of_done_tasks == 0):
                if not is_empty_task:
                    self.info("Not found further tasks. Waiting for new tasks.")
                    is_empty_task = True
                time.sleep(0.5)
            else:
                is_empty_task = False

    def get_subdirs(self):
        return list(sorted(filter(lambda x: not x.endswith("_tmp"), [f.path for f in os.scandir(self.config.incoming_data_path) if f.is_dir()])))

    def collect_tasks(self):
        subdirs = list(filter(lambda x: x > self.last_subdir, self.get_subdirs()))
        if len(subdirs) > 0:
            self.last_subdir = subdirs[-1]
        size = 0
        for subdir in subdirs:
            self.debug(f"Try to collect tasks from the directory: {subdir}")
            dat_files = list(sorted(filter(lambda x: x.endswith(".dat"), [f.path for f in os.scandir(subdir) if f.is_file()])))
            size += len(dat_files)
            for dat_file in dat_files:
                self.pending_tasks.put(dat_file)
                self.debug(f"collected task: {dat_file}")
        return size 
    
    def close_tasks(self):
        size = self.done_tasks.qsize()
        while not self.done_tasks.empty():
            done_task = self.done_tasks.get()
            os.rename(done_task, done_task + ".done")
            self.debug(f"Closed task: {done_task}")
        return size
