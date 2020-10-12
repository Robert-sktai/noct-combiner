import sqlite3

conn = sqlite3.connect('metadata.db')
c = conn.cursor()
c.execute('select table_name from swing_tables group by table_name')
rows = c.fetchall()
print (rows)
print (len(rows))
