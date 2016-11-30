#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
产生mysqlbinlog反向的sql的主程序
 v0.1.0  2016/10/20  yilai    created
"""

import traceback
import os,sys
import logging
from func import init_logger,print_stack
from flashback import Parameter,deal_all_event,generate_create_table,convert_datetime_to_timestamp
import codecs
from datetime import datetime,timedelta
from time import mktime
from constant import Constant
from optparse import OptionParser
from mysql_table import MysqlTable


logger = logging.getLogger(__name__)


def get_check_option():
    """
    得到和检查用户输入的参数，返回参数对象
    """
    logger.debug(sys.argv)
    usage = 'usage: python %prog [options]' \
            '\nsample1:python  %prog --host="127.0.0.1" --username="root" --port=43306 --password="" --schema=test --table="test5"' \
            '\n' \
            'sample2:python %prog --host="127.0.0.1" --username="root" --port=43306 --password="" --schema=test --table="test5,test6" ' \
            '--binlog_end_time="2016-11-05 11:27:13" --binlog_start_file_name="mysql-bin.000024"  --binlog_start_file_position=4 ' \
            '--binlog_start_time="2016-11-04 11:27:13"  --skip_delete  --skip_insert --add_schema_name' \
            '\nsample3:python %prog  --host="127.0.0.1" --username="root" --port=43306 --password="" --schema=test --table="test5,test6" --binlog_start_file_name="mysql-bin.000022"'
    parser = OptionParser(usage)
    parser.add_option("-H","--host", type='string', help="mandatory,mysql hostname" )
    parser.add_option("-P","--port", type='int',default=3306,help="mysql port,default 3306" )
    parser.add_option("-u","--username", type='string', help="mandatory,username" )
    parser.add_option("-p","--password", type='string',default="",help="password" )
    #TODO 只能是一个字符串哦，先不支持多个schema?实际能支持，就是文件名有comma
    parser.add_option("-s","--schema", type='string',help="mandatory,mysql schema")
    parser.add_option("-t","--tables", type='string',
                      help="mandatory,mysql tables,suport multiple tables,use comma as separator")
    parser.add_option("-N","--binlog_end_time", type='string',
                      help="binlog end time,format yyyy-mm-dd hh24:mi:ss,default is current time ")
    parser.add_option("-S","--binlog_start_file_name", type='string',
                      help="binlog start file name,default is current logfile of db")
    #TODO 开始位置不能写在这？
    parser.add_option("-L","--binlog_start_file_position", type='int',default=4,
                      help="binlog start file name")
    parser.add_option("-E","--binlog_start_time", type='string',
                      help="binlog start time,format yyyy-mm-dd hh24:mi:ss")
    parser.add_option("-l","--output_file_path", type='string',default="./log",
                      help="file path that sql generated,,default ./log")
    parser.add_option("-I","--skip_insert", action="store_true",default=False,
                      help="skip insert(WriteRowsEvent) event")
    parser.add_option("-U","--skip_update", action="store_true",default=False,
                      help="skip update(UpdateRowsEvent) event")
    parser.add_option("-D","--skip_delete", action="store_true",default=False,
                      help="skip delete(DeleteRowsEvent) event")
    parser.add_option("-a","--add_schema_name", action="store_true",default=False,
                      help="add schema name for flashback sql")
    parser.add_option("-v","--version",action="store_true",default=False,
                      help="version info")
    (options, args) = parser.parse_args()
    if options.version is True:
         logger.info("version is {0}".format(Constant.VERSION))
         exit(0)
    if options.host is None:
        raise ValueError("parameter error:host is mandatory input")
    if options.username is None:
        raise ValueError("parameter error:username is mandatory input")
    if options.schema is None:
        raise ValueError("parameter error:schema is mandatory input")
    if options.tables is None:
        raise ValueError("parameter error:tables is mandatory input")
    if not os.path.exists(options.output_file_path) :
          raise ValueError("parameter error:output {0} dir is not exists".format(options.output_file_path))

    if options.skip_insert and options.skip_delete and options.skip_update:
         raise ValueError("conld choose at least one event")

    if not options.binlog_end_time is  None:
        try:
            end_to_timestamp=convert_datetime_to_timestamp(options.binlog_end_time, '%Y-%m-%d %H:%M:%S')
        except Exception as err:
             raise ValueError("binlog_end_time {0} format error,detail error={1}".format(options.binlog_end_time,err.__str__()))

    if not options.binlog_start_time is  None:
        try:
            start_to_timestamp=convert_datetime_to_timestamp(options.binlog_start_time, '%Y-%m-%d %H:%M:%S')
        except Exception as err:
             raise ValueError("binlog_start_time {0} format error,detail error={1}".format(options.binlog_start_time,err.__str__()))
        if not  options.binlog_end_time is None:
            if  start_to_timestamp>=end_to_timestamp:
                 raise ValueError("binlog_start_time is above binlog_end_time,start_time={0},end_time={1}".
                                  format(options.binlog_start_time,options.binlog_end_time))
    return options



def parse_option():
    """
    分析用户输入的参数，返回参数对象
    """

    opt=get_check_option()
    dict={}
    dict["host"]=opt.host
    dict["username"]=opt.username
    dict["port"]=opt.port
    dict["password"]=opt.password
    dict["start_binlog_file"]=opt.binlog_start_file_name
    dict["start_position"]=opt.binlog_start_file_position
    dict["output_file_path"]=opt.output_file_path
    dict["schema"]=opt.schema
    dict["tablename"]=opt.tables
    dict["keep_data"]=True
    input_end_to_datetime=opt.binlog_end_time
    if not input_end_to_datetime is  None:
         end_to_timestamp=convert_datetime_to_timestamp(input_end_to_datetime, '%Y-%m-%d %H:%M:%S')
         dict["end_to_timestamp"]=int(end_to_timestamp)
    input_start_to_datetime=opt.binlog_start_time
    if not input_start_to_datetime is None:
        start_to_timestamp=convert_datetime_to_timestamp(input_start_to_datetime, '%Y-%m-%d %H:%M:%S')
        dict["start_to_timestamp"]=int(start_to_timestamp)
    dict["skip_insert"]=opt.skip_insert
    dict["skip_update"]=opt.skip_update
    dict["skip_delete"]=opt.skip_delete
    dict["add_schema_name"]=opt.add_schema_name

    parameter=Parameter(**dict)
    parameter.check_tables_exist()
    parameter.set_defaut()

    return parameter

def new_files(parameter):
    """
    建立反向sql文件,保留现场数据文件和保留现场数据文件的建表语句文件
    :parameter 用户输入参数的形成的实例
    :return: 文件描述符，存在parameter实例中
    """
    flash_filename=parameter.get_file_name("flashback")
    flashback=codecs.open(flash_filename, "w", encoding=Constant.FILE_ENCODING)
    parameter.file["flashback"]=flashback
    logger.debug("flashback sql fileno={0}".format(parameter.file["flashback"]))
    if parameter.keep_data:
        data_filename=parameter.get_file_name("save_data_dml")
        data=codecs.open(data_filename, "w", encoding=Constant.FILE_ENCODING)
        parameter.file["data"]=data
        logger.debug("data sql fileno={0}".format(parameter.file["data"]))
        data_create_filename=parameter.get_file_name("save_data_create_table")
        data_create=codecs.open(data_create_filename, "w", encoding=Constant.FILE_ENCODING)
        parameter.file["data_create"]=data_create
        logger.debug("data create sql fileno={0}".format(parameter.file["data_create"]))

def close_files(parameter):
    if parameter.file["data"] is not None:
        parameter.file["data"].close()
    if parameter.file["data_create"] is not None:
        parameter.file["data_create"].close()
    if parameter.file["flashback"] is not None:
        parameter.file["flashback"].close()


def print_stat(parameter):
    logger.info("===statistics===")
    logger.info("scan {0} events ".format(parameter.stream.event_count))
    logger.info(parameter.stat)

def main():
   logfilename="{0}/{1}".format(Constant.LOGFILE_PATH,Constant.LOGFILE_NAME)
   init_logger(logfilename,logging.INFO)
   #init_logger(logfilename)
   #TODO 缺少tablemap的报警没有，会导致丢失数据
   #TODO 如果表被改动了，基本没有办法哦
   try:
       parameter=parse_option()
   except Exception as err:
       logger.error(err.__str__())
       print_stack()
       exit(1)

   try:
       parameter=parse_option()
       logger.info(u"parameter={0}".format(parameter.__dict__))
       new_files(parameter)
       deal_all_event(parameter)
       generate_create_table(parameter)
       print_stat(parameter)
   except Exception as err:
       logger.error("error:"+err.__str__())
       print_stack()
   finally:
       parameter.stream.close()
       close_files(parameter)

if __name__=='__main__':
    main()