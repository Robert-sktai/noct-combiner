with open('tables.txt') as f:
    new_f = open("createtable.sh", "w+")
    for table in list(f):
        table = table.rstrip('\n')
        new_f.write(f"""cbt createtable {table} "families={table}"\n""")
    new_f.close()

