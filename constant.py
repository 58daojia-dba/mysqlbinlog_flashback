#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
常量定义（正式环境）
 v0.1.0  2016/07/20  yilai    created
"""

from __future__ import print_function


class Constant(object):
    """
    版本号标识
    """
    VERSION="0.1.1"
    """
        存一些全局的变量
        :return:
   """
    #保存数据的表名的模板
    KEEP_DATA_TABLE_TEMPLATE=u"`_{0}_keep_data_`"
    #定义程序日志的路径
    LOGFILE_PATH="./"
    #定义程序日志的文件名
    LOGFILE_NAME="mysqlbinlog_flashback.log"
    #输出文件的编码方式
    FILE_ENCODING="utf-8"
    #处理到数量后这个打印一条提示信息
    PROCESS_INTERVAL=10000
    #PROCESS_INTERVAL=100
    #timedelta (minutes)of current time
    TIMEDELTA_CURRENT_TIME=0
    #是否对update语句生成update pk的字句
    UPDATE_PK=True

    #允许解析的字段类型，不在里面的会报错
    ALLOW_TYPE={
        "varchar":True,
        "char":True,
        "datetime":True,
        "date":True,
        "time":True,
        "timestamp":True,
        "bigint":True,
        "mediumint":True,
        "smallint":True,
        "tinyint":True,
        "int":True,
        "smallint":True,
        "decimal":True,
        "float":True,
        "double":True,
        "longtext":True,
        "tinytext":True,
        "text":True,
        "mediumtext":True
    }

