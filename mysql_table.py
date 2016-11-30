# -*- coding: utf-8 -*-
"""
提供information_schema.columns的mysql层访问
对外暴露的方法会自己维护数据库连接（自己打开关闭连接）
 v0.1.0  2016/07/20  yilai    created
"""

from __future__ import print_function
import time,os
import datetime
import re
import logging
import pymysql


logger = logging.getLogger("__main__")

class MysqlTable(object):


    def __init__(self):
        self._connection = None


    def get_columns(self,schema,table):
        try:
            with self._connection.cursor(pymysql.cursors.DictCursor) as cursor:
               sql = """
                    SELECT
                        COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                        COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY
                    FROM
                        information_schema.columns
                    WHERE
                        table_schema = %s AND table_name = %s
                    """
               cursor.execute(sql, (schema, table))
           #必须用fetchone,它返回dict,fetchall返回的是list
            arr=cursor.fetchall()
            if len(arr)==0:
                raise  ValueError(u"can't find table {0}.{1}".format(schema,table))
            return (arr)
        finally:
            self.disconnect()

    def get_last_binary_log_name(self):
        """
         得到最后一个binary log name
        :return: dict{u'Log_name': u'mysql-bin.000025', u'File_size': 191}
        """
        try:
            with self._connection.cursor(pymysql.cursors.DictCursor) as cursor:
               sql = """
                    SHOW binary logs;
                    """
               cursor.execute(sql)
           #必须用fetchone,它返回dict,fetchall返回的是list
            arr=cursor.fetchall()
            if (len(arr)==0):
                raise  ValueError("can't get the last binary log name,'SHOW binary logs' return null ")
            return(arr[len(arr)-1]["Log_name"])
        finally:
            self.disconnect()

    def get_current_datetime(self):
        """
         得到最后一个binary log name
        :return: dict{u'Log_name': u'mysql-bin.000025', u'File_size': 191}
        """
        try:
            with self._connection.cursor(pymysql.cursors.DictCursor) as cursor:
               sql = """
                    select now() current
                    """
               cursor.execute(sql)
               dt=cursor.fetchone()
               return dt["current"]
        finally:
            self.disconnect()


    def connect(self,connection_setting):
        self._connection = pymysql.connect(**(connection_setting))

    def disconnect(self):
        if self._connection is not None:
            self._connection.close()
            #print("close")



