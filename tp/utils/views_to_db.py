import pandas as pd
import os
from glob import glob
import cx_Oracle
from sqlalchemy import create_engine


if __name__ == '__main__':

    import sqlalchemy
    print(sqlalchemy.__version__)
    # engine = cx_Oracle.connect('TP_DEV', 'TP_DEV', 'localhost:1521/XE')
    engine = create_engine('oracle+cx_oracle://XE')


    csv_files_path = r'C:\Users\arunr\Dropbox\Python project\db\db\tbls_from_original_code'
    print(csv_files_path)

    for file in glob(os.path.join(csv_files_path,'*.csv')):
        base_name = os.path.basename(file).split('.')[0]
        print(base_name)
        df = pd.read_csv(file)
        df.to_sql(base_name,engine,if_exists='replace')
