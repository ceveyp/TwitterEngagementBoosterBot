import mysql.connector
from config import *


def get_mysql_conn():
    try:
        conn = mysql.connector.connect(host=mysql_host,
                                       port=mysql_port,
                                       user=mysql_user,
                                       password=mysql_pass)
        return conn
    except Exception as e:
        print(e)
        return False


def mysql_query(sql, params=None) -> list:
    try:
        conn = get_mysql_conn()
        cursor = conn.cursor(dictionary=True)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        records = cursor.fetchall()
        conn.commit()
        conn.close()
        if not records:
            return []
        if not type(records) == list:
            return [records]
        return records
    except Exception as e:
        print(e)
        return []


def mysql_exec(sql, params=None) -> bool:
    try:
        conn = get_mysql_conn()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(e)
        return False
