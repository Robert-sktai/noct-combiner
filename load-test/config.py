class Config():
    def __init__(self):
        # TODO
        test_name="test-parallelism"
        bigtable_project_id="sktaic-datahub"
        bigtable_instance_id="sktai-noct-load-test"
        bigtable_table_id=test_name

        self.bigtable_project_id = bigtable_project_id
        self.bigtable_instance_id = bigtable_instance_id
        self.bigtable_table_id = bigtable_table_id
        self.logger_file = "my.log"
