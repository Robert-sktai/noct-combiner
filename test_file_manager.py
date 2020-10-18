import time
import os
import shutil
import logging
import inspect

from datetime import datetime, timedelta
from thread import Thread
from context import Context
from file_manager import FileManager 
    
class TestFileManager(Thread):
    def __init__(self, context):
        super().__init__(context=context, level=logging.INFO)
        self.swing_migration_tables = self.context.metadata.get_swing_migration_tables()

    def remove_dir(self, path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                self.debug('Failed to delete %s. Reason: %s' % (file_path, e))

    def make_dir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def make_file(self, path):
        file = open(path, "w") 
        file.close() 

    def create_dummy_files(self, num_dirs, gen_lock_files=False):
        for dir_idx in range(1, num_dirs+1):
            yyyymmdd = (datetime.today() + timedelta(days=dir_idx)).strftime("%Y%m%d")
            dir_name = yyyymmdd + "_" + str(dir_idx).zfill(3)
            dir_path = os.path.join(self.context.incoming_data_path, dir_name)
            tmp_dir_path = dir_path + "_tmp" 
            self.make_dir(tmp_dir_path)
            for table_name in self.swing_migration_tables:
                file_path = os.path.join(tmp_dir_path, table_name + "_" + dir_name + ".dat")
                if gen_lock_files and dir_idx % 10:
                    file_path += ".locked"
                self.make_file(file_path)
            os.rename(tmp_dir_path, dir_path)

    def process_tasks(self, file_manager, num_tasks):
        count = 0
        for table_name in self.swing_migration_tables: 
            for _ in range(0, num_tasks):
                task = file_manager.dispatch_task(table_name)
                self.debug(f"Found task: {task}" if task is not None else "Not found task")
                if task is not None:
                    count += 1
                    file_manager.close_task(task)
        return count

    def check(self, file_manager):
        for table_name in self.swing_migration_tables: 
            pending_tasks = file_manager.get_pending_tasks(table_name)
            while not pending_tasks.empty():
                self.error(f"Found unexpected pending task: {self.pending_tasks.get()}")

    def test1(self):
        func_name = inspect.currentframe().f_code.co_name
        self.info(f"* Started {func_name} ... ")
        self.remove_dir(self.context.incoming_data_path)

        file_manager = FileManager(self.context)
        num_dirs = 25
        self.create_dummy_files(num_dirs, gen_lock_files=True)
        file_manager.start()
        time.sleep(0.5)
        self.process_tasks(file_manager, num_dirs)
        self.check(file_manager)
        file_manager.stop()
        self.info(f"* Finished {func_name} ... ")
       
    def test2(self):
        func_name = inspect.currentframe().f_code.co_name
        self.info(f"* Started {func_name} ... ")
        self.remove_dir(self.context.incoming_data_path)

        file_manager = FileManager(self.context)
        num_dirs = 10000
        num_tasks = 100
        num_done_tasks = 0
        secs = 0.05
        time_limit = 30
        self.create_dummy_files(num_dirs)
        file_manager.start()
        start = time.time()
        while True:
            num_done_tasks += self.process_tasks(file_manager, num_tasks)
            if num_done_tasks == num_dirs * len(self.swing_migration_tables):
                break
            if time.time() - start > time_limit:
                self.error(f"Timeout: test exceeds the limit {time_limit} secs")
                break
            time.sleep(secs)
        file_manager.stop()
        self.info(f"* Finished {func_name} ... ")

if __name__ == "__main__":
    incoming_data_path = "/home/svcapp_su/robert/workspace/noct-combiner/tmp"
    context = Context(incoming_data_path)
    test_file_manager = TestFileManager(context)
    test_file_manager.test1()
    test_file_manager.test2()
