import hashlib
import resource
import logging
import os
import time

from datetime import datetime
from skt.gcp import get_bigtable 
from process import Process

class WorkerStatistics:
    def __init__(self):
        #TODO
        pass

class Worker(Process):
    def __init__(self, log_queue, index, pending_tasks, done_tasks):
        name = type(self).__name__ + "-" + str(index)
        super().__init__(log_queue=log_queue, level=logging.INFO, name=name)
        self.pending_tasks = pending_tasks
        self.done_tasks = done_tasks

    def dispatch_task(self):
       return None if self.pending_tasks.empty() else self.pending_tasks.get() 

    def close_task(self, task):
       self.done_tasks.put(task) 

    def filter(self, task):
        return self.get_table_name_from_path(task) in self.metadata.get_swing_migration_tables()

    def run(self):
        while not self.stopped():
            task = self.dispatch_task()
            if task is not None:
                try:
                    if self.filter(task):
                        self.process(task)
                        self.close_task(task)
                except Exception as e:
                    self.logger.exception(e)
                    self.error(f"Unexpected error: {e} [{task}]")
                    continue
                time.sleep(0.005)
            else:
                time.sleep(0.1)

    def mutate_rows(self, cbt, rows):
        num_errors = 0
        response = cbt.mutate_rows(rows)
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

    def write(self, data, ops, file_path, table_name, columns):
        num_errors = 0
        curr_size = 0
        total_size = 0
        rows = []
        file_name = file_path[file_path.rindex("/")+1:]
        ts = self.get_timestamp(file_name)
        # TODO: reuse of cbt 
        cbt = get_bigtable(instance_id=self.config.bigtable_instance_id,
                           app_profile_id=self.config.bigtable_app_profile_id,
                           table_id=table_name)
        for key,value in data.items():
            num_cols = len(value)
            row = cbt.direct_row(key.encode(encoding="utf-8",errors="ignore"))
            for i in range(len(columns)):
                row.set_cell(table_name,
                columns[i],
                value[i].encode('utf-8'),
                ts)
            if ops[key].startswith('D'):
                row.set_cell(table_name,
                'flag_deleted',
                'true',
                ts)
            rows.append(row)
            curr_size += num_cols
            total_size += num_cols 
            if curr_size + num_cols >= int(1e5):
                num_errors += self.mutate_rows(cbt, rows)
                curr_size = 0
                rows = []

        if curr_size > 0:
            num_errors += self.mutate_rows(cbt, rows)
            curr_size = 0
            rows = []
        self.info(f'# changes: {total_size}, Failed rows: {num_errors}, Files: {file_name}')

    def get_table_name_from_path(self, path):
        try:
            file_name = path[path.rindex("/")+1:]
            table_name = file_name[:file_name[:file_name.rindex("_")].rindex("_")].lower()
            return table_name
        except ValueError:
            self.error(f"Parsing error, but ignored: unexpected .dat file name format: {path}")
        return None

    def generate_rowkey(self, table_name, primary_key_dict, cols):
        key = None
        for pk in self.metadata.get_ordered_pks()[table_name]:
            key = cols[primary_key_dict[pk]] if key is None else key + "#" + cols[primary_key_dict[pk]]
        return key

    def process(self, file_path):  
        with open(file_path, encoding="cp949", errors="ignore") as f:
            table_name = self.get_table_name_from_path(file_path)
            columns = self.metadata.get_swing_table_columns()[table_name]
            primary_key_dict = dict()
            if table_name in self.metadata.get_primary_keys_of_swing_tables():
                primary_key_dict = self.metadata.get_primary_keys_of_swing_tables()[table_name]

            hashing_identification_column_indexes = set() 
            if table_name in self.metadata.get_hashing_identification_column_indexes_of_swing_tables():
                hashing_identification_column_indexes = self.metadata.get_hashing_identification_column_indexes_of_swing_tables()[table_name]

            masking_identification_column_indexes = set()
            if table_name in self.metadata.get_masking_identification_column_indexes_of_swing_tables():
                masking_identification_column_indexes = self.metadata.get_masking_identification_column_indexes_of_swing_tables()[table_name]
            self.expected_num_cols = len(columns)+1
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
                for index in hashing_identification_column_indexes:
                    sha256 = hashlib.sha256()
                    sha256.update(cols[index].encode('utf-8'))
                    cols[index] = sha256.hexdigest()
                for index in masking_identification_column_indexes:
                    cols[index] = "#"

                key = self.generate_rowkey(table_name, primary_key_dict, cols)
                if key is None:
                    self.error(f"Key is empty: {table_name}")
                if key in data:
                    self.debug(f"Found duplicate key: {key}")
                else:
                    data[key] = cols
                    ops[key] = tokens[0]

        if len(data) > 0:
            self.write(data, ops, file_path, table_name, columns)
