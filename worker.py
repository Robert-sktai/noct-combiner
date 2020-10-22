import hashlib
import resource
import logging
import os
import time
import datetime
from skt.gcp import get_bigtable 

from thread import Thread

class WorkerStatistics:
    def __init__(self):
        pass

class Worker(Thread):
    def __init__(self, context, index, file_manager, table_name):
        name = type(self).__name__ + "-" + str(index)
        super().__init__(context=context, level=logging.INFO, name=name)
        self.cbt = get_bigtable(instance_id=self.context.bigtable_instance_id, table_id=self.context.bigtable_table_id)
        self.file_manager = file_manager
        self.table_name = table_name
        self.columns = self.context.metadata.get_swing_table_columns()[self.table_name]
        self.primary_key_indexes = self.context.metadata.get_primary_key_indexes_of_swing_tables()[self.table_name]
        self.expected_num_cols = len(self.columns)+1
        # TODO: should be filled out 
        self.hashed_indexes = []
        self.sha256 = hashlib.sha256()
        self.timestamp = datetime.datetime.utcnow()
        if self.context.collect_statistics:
            self.statistics = WorkerStatistics()

    def run(self):
        while not self.stopped():
            task = self.file_manager.dispatch_task(self.table_name)
            if task is not None:
                try:
                    self.process(task)
                except UnicodeDecodeError as e:
                    self.error(f"Unicode decode error: {e} [{task}]")
                    # TODO: fix this after resolving the known unicode issue.
                    continue
#                    raise
                self.file_manager.close_task(task)
            else:
                time.sleep(0.005)

    # https://cloud.google.com/bigtable/docs/writing-data#python
    def mutate_rows(self, rows):
        num_errors = 0
        response = self.cbt.mutate_rows(rows)
        for i, status in enumerate(response):
            if status.code != 0:
                num_errors += 1
                self.error("Error happened while writing row: {}".format(status.message))
        return num_errors

    def write(self, data, ops, file_name):
        rows = []
        num_errors = 0
        counter = 0
        for key,value in data.items():
            num_cols = len(value)
            row = self.cbt.direct_row(key)
            for i in range(len(self.columns)):
                row.set_cell(self.table_name,
                self.columns[i],
                value[i].encode('utf-8'),
                self.timestamp)
            if ops[key].startswith('D'):
                row.set_cell(self.table_name,
                'flag_deleted',
                'true',
                self.timestamp)
            rows.append(row)
            counter += num_cols 
            if counter + num_cols >= int(1e5):
                num_errors += self.mutate_rows(rows)
                rows = []
                counter = 0

        if counter > 0:
            num_errors += self.mutate_rows(rows)

        self.info(f'# rows: {len(data)}, Failed rows: {num_errors}, File: {file_name}')

    def process(self, file_path):  
        with open(file_path, encoding="cp949") as f:
          # getting lines by lines starting from the last line up
            # 1: End of separator
            data = dict()
            ops = dict()
            for line in reversed(list(f)):
                tokens = line.split(chr(0x02))
                cols = tokens[2:]
                num_cols = len(cols)
                if num_cols != self.expected_num_cols:
                    error_message = f"Column size mismatch. Actual # of cols is {num_cols}"
                    error_message += f", but expected # of cols is {self.expected_num_cols}. file_path={file_path}"
                    self.error(error_message)
                    raise
                for hashed_index in self.hashed_indexes:
                    self.sha256.update(cols[hashed_index].encode('utf-8'))
                    cols[hashed_index] = self.sha256.hexdigest()
                key = None
                for i in self.primary_key_indexes:
                    key = self.table_name + "#" + cols[i] if key is None else key + "#" + cols[i]
                if key is None:
                    self.error(f"Key is empty: {table_name}")
                if key in data:
                    self.debug(f"Found duplicate key: {key}")
                else:
                    data[key] = cols
                    ops[key] = tokens[0]
       
        if len(data) > 0:
            file_name = file_path[file_path.rindex("/")+1:]
            self.write(data, ops, file_name)
