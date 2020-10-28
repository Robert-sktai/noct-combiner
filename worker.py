import hashlib
import resource
import logging
import os
import time

from datetime import datetime
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
        hashing_identification_column_indexes = self.context.metadata.get_hashing_identification_column_indexes_of_swing_tables()
        masking_identification_column_indexes = self.context.metadata.get_masking_identification_column_indexes_of_swing_tables()
        self.hashing_identification_column_indexes = hashing_identification_column_indexes[self.table_name] if self.table_name in hashing_identification_column_indexes else set()
        self.masking_identification_column_indexes = masking_identification_column_indexes[self.table_name] if self.table_name in masking_identification_column_indexes else set()
        self.expected_num_cols = len(self.columns)+1
        self.sha256 = hashlib.sha256()
        if self.context.collect_statistics:
            self.statistics = WorkerStatistics()

    def run(self):
        while not self.stopped():
            task = self.file_manager.dispatch_task(self.table_name)
            if task is not None:
                try:
                    self.process(task)
                except Exception as e:
                    self.error(f"Unexpected error: {e} [{task}]")
                    continue
                self.file_manager.close_task(task)
            else:
                time.sleep(0.005)

    def mutate_rows(self, rows):
        num_errors = 0
        response = self.cbt.mutate_rows(rows)
        for i, status in enumerate(response):
            if status.code != 0:
                num_errors += 1
                self.error("Error happened while writing row: {}".format(status.message))
        return num_errors

    def get_timestamp(self, file_name):
        prefix = file_name[:file_name.rindex(".")]
        datetime_str = prefix[prefix[:prefix.rindex("_")].rindex("_")+1:]
        date_str = datetime_str[:datetime_str.rindex("_")]
        time_str = datetime_str[datetime_str.rindex("_")+1:]
        return datetime.fromtimestamp(datetime.strptime(date_str, "%Y%m%d").timestamp() + 300 * int(time_str))

    def write(self, data, ops, file_name):
        rows = []
        num_errors = 0
        counter = 0
        ts = self.get_timestamp(file_name)
        for key,value in data.items():
            num_cols = len(value)
            row = self.cbt.direct_row(key.encode(encoding="utf-8",errors="ignore"))
            for i in range(len(self.columns)):
                row.set_cell(self.table_name,
                self.columns[i],
                value[i].encode('utf-8'),
                ts)
            if ops[key].startswith('D'):
                row.set_cell(self.table_name,
                'flag_deleted',
                'true',
                ts)
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
        with open(file_path, encoding="cp949", errors="ignore") as f:
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
                for index in self.hashing_identification_column_indexes:
                    self.sha256.update(cols[index].encode('utf-8'))
                    cols[index] = self.sha256.hexdigest()
                for index in self.masking_identification_column_indexes:
                    cols[index] = "#"
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
