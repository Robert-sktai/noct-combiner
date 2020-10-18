import hashlib
import resource
import logging
import os
import time
import datetime
from skt.gcp import get_bigtable 

from thread import Thread

class Worker(Thread):
    def __init__(self, context, index, file_manager, table_name):
        name = type(self).__name__ + "-" + str(index)
        super().__init__(context=context, level=logging.INFO, name=name)
        self.file_manager = file_manager
        self.table_name = table_name
        self.columns = self.context.metadata.get_swing_table_columns()[self.table_name]
        self.expected_col_size = len(self.columns)+1
        # TODO: are you sure? 
        self.col_index_key = 0
        # TODO: should be filled out 
        self.hashed_indexes = [self.col_index_key]

    def run(self):
        while not self.stopped():
            task = self.file_manager.dispatch_task(self.table_name)
            if task is not None:
                self.process(task)
                self.file_manager.close_task(task)
            else:
                time.sleep(0.005)

    # https://cloud.google.com/bigtable/docs/writing-data#python
    def mutate_rows(self, data):
        cbt = get_bigtable(instance_id=self.context.bigtable_instance_id, table_id=self.context.bigtable_table_id)
        timestamp = datetime.datetime.utcnow()

        rows = []
        for key,value in data.items():
            row = cbt.direct_row(key)
            for i in range(len(self.columns)):
                row.set_cell(self.table_name,
                self.columns[i],
                value[i].encode('utf-8'),
                timestamp)
            if value[0].startswith('D'):
                row.set_cell(self.table_name,
                'deleted',
                'true',
                timestamp)
            rows.append(row)
        response = cbt.mutate_rows(rows)
        num_errors = 0
        for i, status in enumerate(response):
            if status.code != 0:
                num_errors += 1
                self.error("Error happened while writing row: {}".format(status.message))

        self.info(f'Total rows: {len(data)}, Failed rows: {num_errors}')

    def process(self, file_path):  
        with open(file_path, encoding="euc-kr") as f:
          # getting lines by lines starting from the last line up
            # 1: End of separator
            data = dict()
            m = hashlib.sha256()
            for line in reversed(list(f)):
                cols = line.split(chr(0x02))[2:]
                actual_col_size = len(cols)
                if actual_col_size != self.expected_col_size:
                    error_message = f"Column size mismatch. Actual # of cols is {actual_col_size}"
                    error_message += f", but expected # of cols is {self.expected_col_size}. file_path={file_path}"
                    self.error(error_message)
                    raise
                for hashed_index in self.hashed_indexes:
                    m.update(cols[hashed_index].encode('utf-8'))
                    cols[hashed_index] = m.hexdigest()
                if cols[self.col_index_key] in data:
                    self.debug(f"Found duplicate key: {cols[self.col_index_key]}")
                else:
                    data[cols[self.col_index_key]] = cols
#            print (cols)
#            self.mutate_rows(data)

if __name__ == "__main__":
    file_path = "./swing/20201015_001/ZORD_CO_CUST_20201015_001.dat"
    start = time.time()
    process(file_path)
    print(f"elapsed time: {time.time() - start}")
