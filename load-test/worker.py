import logging
import datetime
import time

from process import Process
from google.cloud import bigtable
from google.cloud.bigtable import Client

class Worker(Process):
    def __init__(self, log_queue, index):
        name = type(self).__name__ + "-" + str(index)
        super().__init__(log_queue=log_queue, name=name)
        self.client = bigtable.Client(admin=True, project=self.config.bigtable_project_id)
        self.instance = self.client.instance(self.config.bigtable_instance_id)
        # TODO: removal of the constant table name 
        self.table = self.instance.table("load-test")
        self.data = self.generate_data()

    def run(self):
        try:
            self.write()
        except Exception as e:
            self.error(e)
        self.info("Worker has been stopped")

    def generate_data(self):
        data = dict()
        record = list()
        cols = 100
        rows = 1000
        length_of_record = 10
        length_of_column = 20
        for index in range(0, cols):
            record.append(str(index).zfill(length_of_column)) 

        for index in range(0, rows):
            key = str(index).zfill(length_of_record) 
            data[key] = record 
        return data

    # https://cloud.google.com/bigtable/docs/writing-data#python
    def mutate_rows(self, rows):
        num_errors = 0
        response = self.table.mutate_rows(rows)
        for i, status in enumerate(response):
            if status.code != 0:
                num_errors += 1
                self.error("Error happened while writing row: {}".format(status.message))
        return num_errors

    def write(self):
        rows = []
        num_errors = 0
        counter = 0
        while not self.stopped():
            cur_time = datetime.datetime.utcnow()
            for key,value in self.data.items():
                num_cols = len(value)
                row = self.table.direct_row(key)
                for i in range(num_cols):
                    row.set_cell("cf1",
                    "col"+str(i),
                    value[i],
                    cur_time)
                rows.append(row)
                counter += num_cols
                if counter + num_cols >= int(1e5):
#                    num_errors += self.mutate_rows(rows)
                    self.info(f'# of cells: {counter}, Failed rows: {num_errors}')
                    rows = []
                    counter = 0
    
    def write_single(self):
        num_errors = 0
        counter = 0
        while True:
            cur_time = datetime.datetime.utcnow()
            counter+=1
            for key,value in self.data.items():
                num_cols = len(value)
                row = self.table.direct_row(key)
                for i in range(num_cols):
                    row.set_cell("cf1",
                    "col"+str(i),
                    value[i],
                    cur_time)
                row.commit()
            if counter % 100000 == 0:
                self.info(f'# rows: {counter}, Failed rows: {num_errors}')
