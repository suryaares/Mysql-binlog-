# Mysql-binlog-replication-to-snowflake
Fetching binlog from mysql using python

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser
import pymysqlreplication
import csv
import boto3
import mysql.connector
from mysql.connector import Error
import pymysql
import collections
from collections import Counter
import sys
from impala.dbapi import connect
from hdfs import InsecureClient
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import re
import snowflake.connector

while True:
    conn = pymysql.connect(host="XXXXXXXXX", user="XXXX", password="XXXXX", port=3306, db="XXXXX")
    mysql_settings = {'host': 'XXXXX',
                      'port': 3306,
                      'user': 'XXXX',
                      'passwd': 'XXXXXX'
                      }

    stream = BinLogStreamReader(connection_settings=mysql_settings, server_id=100,
                                only_events=[row_event.WriteRowsEvent])
    log_ist_events = []
    for binlogevent in stream:
        for row in binlogevent.rows:
            if binlogevent.table == 'XXXXX':
                wri_event = {}
                if isinstance(binlogevent, row_event.WriteRowsEvent):
                    wri_event["action"] = "insert"
                    wri_event["table"] = binlogevent.table
                    wri_event["database"] = binlogevent.schema
                    wri_event.update(row["values"].items())
                    log_ist_events.append(wri_event)
    stream.close()
    changes = log_ist_events
    if (len(changes) > 0):
        for event_iterations in changes:
            if event_iterations['action'] == 'insert':
                dict_value = event_iterations.copy()
                tablename = dict_value['table']
                database = dict_value['database']
                if (database == 'denododb'):
                    dict_value.pop('action')
                    dict_value.pop('table')
                    dict_value.pop('database')
                    placeholders = ' , '.join(['%s'] * len(dict_value))
                    columns = ' , '.join(dict_value.keys())
                    value_list = list()
                    for value in dict_value.values():
                        if value is None:
                            value_list.append("Null")
                        else:
                            value_list.append(value)
                    value_str = ','.join("'" + str(e) + "'" for e in value_list)
                    snf = "INSERT INTO %s( %s ) VALUES( %s )" % (tablename, columns, value_str)
                    print(snf)
                    user = "XXXXXXX"
                    password = "XXXXXX"
                    account = "XXXXXXXX"
                    warehouse = "COMPUTE_WH"
                    database = "DENODO"
                    schema = "PUBLIC"
                    conn = snowflake.connector.connect(
                        user=user,
                        password=password,
                        account=account,
                        warehouse=warehouse,
                        database=database,
                        schema=schema
                    )
                    cursor = conn.cursor()
                    cursor.execute(snf)
                    conn.commit()
                    print("Insert Operation")
