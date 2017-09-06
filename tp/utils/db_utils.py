import os
import cx_Oracle as cx


def get_db_connection():
    host = os.environ['DB_HOST']
    user = os.environ['DB_USER']
    password = os.environ['DB_PASSWORD']
    sid = os.environ['DB_SID']

    return cx.connect(user, password)
