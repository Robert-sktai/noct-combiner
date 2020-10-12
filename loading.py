import resource
import logging
import os
import collections
from file_read_backwards import FileReadBackwards
from skt.gcp import get_bigtable 


# https://cloud.google.com/bigtable/docs/writing-data#python
def mutate_rows(table_id, col_names, data):
    import datetime
    cbt = get_bigtable(instance_id="sktai-noct-poc", table_id=table_id)
    timestamp = datetime.datetime.utcnow()

    rows = []
    for key,value in data.items():
        row = cbt.direct_row(key)
        for i in range(len(col_names)):
            row.set_cell('data',
            col_names[i],
            value[i+2].encode('UTF-8'),
            timestamp)
        if value[0].startswith('D'):
            row.set_cell('others',
            'deleted',
            'true',
            timestamp)
        rows.append(row)
    response = cbt.mutate_rows(rows)
    num_errors = 0
    for i, status in enumerate(response):
        if status.code != 0:
            num_errors += 1
            print("Error writing row: {}".format(status.message))

    print(f'Total rows: {len(data)}, Failed rows: {num_errors}')

if __name__ == "__main__":
    file_path = "./"
    file_name = "s.dat"

    print(f'File size is {os.stat(file_name).st_size / (1024*1024)}MB');
    txt_file = open(file_name)

    # 800MB reverse iteration + parsing + writing => 5 mins
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
            datefmt="%H:%M:%S")

    with FileReadBackwards(file_path + file_name, encoding="euc-kr") as frb:
      # getting lines by lines starting from the last line up
        # 3: OP + TS + EOL
        expected_col_size = 17+3
        separator = '@'
        col_index_key = 2
        table_id = "zord_co_cust"
        col_names = ["CUST_NUM",
            "CO_CL_CD",
            "AUDIT_ID",
            "AUDIT_DTM",
            "CUST_NM",
            "DISP_CUST_NM",
            "WLF_DC_CD",
            "WLF_NUM",
            "HNDCAP_FST_RGST_DT",
            "BIZ_MBR_GR_CD",
            "FORGN_BIZR_TYP_CD",
            "TEEN_WEDD_YN",
            "CTZ_CORP_BIZ_NUM_PINF",
            "INDV_BIZR_YN",
            "LOGIC_DEL_YN",
            "CUST_RGST_DT",
            "CUST_CURNT_DT"]
        data = dict()
        for l in frb:
            cols = l.split(separator)
            actual_col_size = len(cols)
            if actual_col_size != expected_col_size:
                error_message = f"Column size mismatch. Actual # of cols is {actual_col_size}"
                error_message += f", but expected # of cols is {expected_col_size}"
                raise RuntimeError(error_message) 
            if cols[col_index_key] in data:
                logging.debug(f"Found duplicate key: {cols[col_index_key]}")
            else:
                data[cols[col_index_key]] = cols
            
        mutate_rows(table_id, col_names, collections.OrderedDict(sorted(data.items())))
#                print (cols[col_index_key])

