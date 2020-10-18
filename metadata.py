import sqlite3

class Metadata:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.cached_swing_tables = None
        self.cached_swing_table_columns = None
        self.cached_swing_migration_tables = None

    def get_swing_tables(self):
        if self.cached_swing_tables is None:
            self.cursor.execute('select table_name from swing_tables group by table_name')
            self.cached_swing_tables = [_[0] for _ in self.cursor.fetchall()]
        return self.cached_swing_tables
    
    def get_swing_table_columns(self):
        if self.cached_swing_table_columns is None:
            self.cursor.execute('select table_name, column_name from swing_tables order by column_id asc')
            self.cached_swing_table_columns = dict()
            for row in self.cursor.fetchall():
                if row[0] not in self.cached_swing_table_columns:
                    self.cached_swing_table_columns[row[0]] = list()
                self.cached_swing_table_columns[row[0]].append(row[1])
        return self.cached_swing_table_columns

    def get_swing_migration_tables(self):
        if self.cached_swing_migration_tables is None:
            self.cursor.execute('select table_name from swing_migration_tables')
            self.cached_swing_migration_tables = [_[0] for _ in self.cursor.fetchall()]
        return self.cached_swing_migration_tables


if __name__ == "__main__":
    import os
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "metadata.db")
    metadata = Metadata(db_path)
    print ("\n* get_swing_tables()")
    print (metadata.get_swing_tables())
    print ("\n* get_swing_table_columns()")
    print (metadata.get_swing_table_columns())
    print ("\n* get_swing_migration_tables()")
    print (metadata.get_swing_migration_tables())
