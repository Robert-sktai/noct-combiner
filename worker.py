import hashlib
import resource
import logging
import os
import time

from datetime import datetime
from skt.gcp import get_bigtable
from process import Process

class Worker(Process):
    def __init__(self, log_queue, index, pending_tasks, done_tasks, error_tasks, slack_queue):
        name = type(self).__name__ + "-" + str(index)
        super().__init__(log_queue=log_queue, level=logging.INFO, name=name)
        self.pending_tasks = pending_tasks
        self.done_tasks = done_tasks
        self.error_tasks = error_tasks
        self.slack_queue = slack_queue
        self.last = time.time()

    def dispatch_task(self):
       return None if self.pending_tasks.empty() else self.pending_tasks.get()

    def close_task(self, task):
       self.done_tasks.put(task)

    def error_task(self, task):
       self.error_tasks.put(task)

    def filter(self, task):
        return os.path.exists(task) and self.get_table_name_from_path(task) in self.metadata.get_swing_migration_tables()

    def elapsed_time(self):
        duration = time.time() - self.last
        self.last = time.time()
        return duration

    def run(self):
        while not self.stopped():
            task = self.dispatch_task()
            try:
                if task is None:
                    time.sleep(0.5)
                    continue
                self.debug(f"[1] took a task: {task} [{self.elapsed_time()} secs]")
                if not os.path.isfile(task):
                    continue
                if self.filter(task):
                    self.process(task)
                    self.close_task(task)
                time.sleep(0.005)
            except Exception as e:
                self.logger.exception(e)
                msg = f"Unexpected error: {e} [{task}]"
                self.error(msg)
                self.error_task(task)
#                self.slack_queue.put(msg)
                continue

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

    def validate(self, file_path, lines):
        eof_file_path = file_path[:file_path.rfind(".dat")] + ".eof"
        with open(eof_file_path, encoding="cp949", errors="ignore") as f:
            tokens = list(f)[0].split(chr(0x02))
            expected_lines = tokens[2]
            if len(lines) != int(expected_lines):
                msg = f"""Inconsistent # of lines between the actual and the exepected: {len(lines)} != {expected_lines}. {file_path}"""
                self.error(msg)
#                self.slack_queue.put(msg)

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
            lines = list(f)
            self.validate(file_path, lines)
            for line in reversed(lines):
                tokens = line.split(chr(0x02))
                cols = tokens[2:]
                num_cols = len(cols)

                if num_cols != self.expected_num_cols:
                    msg = f"Column size mismatch. Actual # of cols is {num_cols}"
                    msg += f", but expected # of cols is {self.expected_num_cols}. file_path={file_path}"
                    self.error(msg)
                    self.slack_queue.put(msg)
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

        self.debug(f"[2] processed a task: {file_path} [{self.elapsed_time()} secs]")
        if len(data) > 0:
            self.write(data, ops, file_path, table_name, columns)
