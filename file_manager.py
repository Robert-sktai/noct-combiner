import os
import queue
import time
import threading

class FileManager(threading.Thread):
    def __init__(self, root_path):
        threading.Thread.__init__(self)
        self._stop_event = threading.Event()

        self.root_path = root_path
        self.last_subdir = ""
        self.pending_tasks = queue.Queue() 
        self.done_tasks = queue.Queue()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def run(self):
        self.unlock_files()
        while not self.stopped():
            size_of_collected_tasks = self.collect_tasks()
            size_of_done_tasks = self.close_tasks()
            if (size_of_done_tasks + size_of_done_tasks == 0):
                time.sleep(0.5)

    def get_subdirs(self):
        return list(sorted(filter(lambda x: not x.endswith("_tmp"), [f.path for f in os.scandir(self.root_path) if f.is_dir()])))

    def unlock_files(self): 
        for subdir in self.get_subdirs():
            lock_files = list(filter(lambda x: x.endswith(".dat.locked"), [f.path for f in os.scandir(subdir) if f.is_file()]))
            for lock_file in lock_files:
                if os.path.isfile(lock_file):
                    print (f"Unlock file: {lock_file}")
                    os.rename(lock_file, lock_file[:-len(".locked")])

    def collect_tasks(self):
        subdirs = list(filter(lambda x: x > self.last_subdir, self.get_subdirs()))
        if len(subdirs) > 0:
            self.last_subdir = subdirs[-1]
        size = 0
        for subdir in subdirs:
            dat_files = list(sorted(filter(lambda x: x.endswith(".dat"), [f.path for f in os.scandir(subdir) if f.is_file()])))
            size += len(dat_files)
            for dat_file in dat_files:
                print (f"collect_tasks: {dat_file}")
                self.pending_tasks.put(dat_file)
        return size 
    
    def get_pending_tasks(self):
        pending_tasks = []
        for subdir in self.get_subdirs():
            dat_files = list(sorted(filter(lambda x: x.endswith(".dat"), [f.path for f in os.scandir(subdir) if f.is_file()])))
            for dat_file in dat_files:
                pending_tasks.append(dat_file)
        return pending_tasks

    def close_tasks(self):
        size = self.done_tasks.qsize()
        while not self.done_tasks.empty():
            done_task = self.done_tasks.get()
            print (f"Close task: {done_task}")
            os.rename(done_task, done_task + ".done")
        return size

    def dispatch_task(self):
       return None if self.pending_tasks.empty() else self.pending_tasks.get() 

    def close_task(self, task):
       self.done_tasks.put(task) 
