#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
flashback的主要处理逻辑和相关的类
 v0.1.0  2016/10/20  yilai    created
"""


from datetime import datetime,timedelta
from time import mktime
import traceback
import logging
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from pymysqlreplication.event import(
    QueryEvent,
    XidEvent
)
from joint_sql import joint_update_sql,joint_insert_sql,joint_delete_sql,joint_keep_data_sql,join_create_table
from mysql_table import MysqlTable
from constant import Constant
from mysql_table import MysqlTable

logger = logging.getLogger("__main__")
#定义保留数据表名的格式

class Parameter(object):
   """
        存用户的命令行的参数和一些全局的变量
        :return:
   """

   def __init__(self,username,password,start_binlog_file,start_position
                ,schema,host="127.0.0.1",start_time=None,output_file_path="./",port=3306,tablename=None
                ,keep_data=False,keep_current_data=False
                ,one_binlog_file=False,dump_event=False,end_to_timestamp=None,start_to_timestamp=None
                ,skip_insert=False,skip_update=False,skip_delete=False,add_schema_name=False
                ):

       """
       :param username: 连接数据库的用户名
       :param password:
       :param port:
       :param start_binlog_file:
       :param start_position:
       :param end_time: 需要解析binlog的结束时间
       :param output_file_prefix:
       :param start_time:
       :param tablename:
       :param schema:
       :param keep_data: 是否生成dml语句的before,after value到单独一个表中
       :param keep_current_data: 是否包括当前值
       :param one_binlog_file: 是否就解析一个binlog文件
       :param dump_event:dump event的信息，用于调试
       :return:
       """
       self.mysql_setting={}
       self.mysql_setting["host"]=host
       self.mysql_setting["port"]=port
       self.mysql_setting["user"]=username
       self.mysql_setting["passwd"]=password
       self.mysql_setting["charset"] = "utf8"
       self.start_binlog_file=start_binlog_file
       self.start_position=start_position
       self.output_file_path=output_file_path
       self.start_time=start_time
       self.schema=schema
       self.schema_array=None
       if self.schema is not None:
           self.schema_array=self.schema.split(",")
       self.table_name=tablename
       self.table_name_array=None
       if self.table_name is not None:
           self.table_name_array=self.table_name.split(",")
       self.keep_data=keep_data
       self.keep_current_data=keep_current_data
       self.one_binlog_file=one_binlog_file
       self.dump_event=dump_event
       self.end_to_timestamp=end_to_timestamp
       self.start_to_timestamp=start_to_timestamp
       self.skip_insert=skip_insert
       self.skip_update=skip_update
       self.skip_delete=skip_delete
       self.add_schema_name=add_schema_name

       #下面是一些公共的变量
       self.stream=None
       self.stat={}
       #self.stat["commit"]=0
       self.stat["flash_sql"]={}
       #输出的文件描述符
       self.file={}
       self.file["flashback"]=None
       self.file["data"]=None
       self.file["data_create"]=None

   def get_file_name(self,filename_type,current_time=None):
       """
        得到文件名
       :param :
       :return:
       """
       if current_time is None:
           current_time=datetime.strftime( datetime.now(), '%Y%m%d_%H%M%S')
       filename="{0}/{1}_{2}_{3}.sql".format(self.output_file_path,filename_type,self.schema,current_time)
       return filename


   def check_tables_exist(self):
       for table in self.table_name_array:
             mysql_table=MysqlTable()
             mysql_table.connect(self.mysql_setting)
             mysql_table.get_columns(self.schema_array[0],table)



   def set_defaut(self):
        """
         访问数据库设置缺省值start_binlog_file，end_to_timestamp
        """
        if self.start_binlog_file is None:
            mysql_table=MysqlTable()
            mysql_table.connect(self.mysql_setting)
            log_name=mysql_table.get_last_binary_log_name()
            self.start_binlog_file=log_name

        if self.end_to_timestamp is None:
            mysql_table=MysqlTable()
            mysql_table.connect(self.mysql_setting)
            current_dt=mysql_table.get_current_datetime()
            current_dt=current_dt - timedelta(minutes = Constant.TIMEDELTA_CURRENT_TIME)
            self.end_to_timestamp=convert_datetime_to_timestamp(current_dt)




#
# class Stat(object):
#    """
#         处理mysqlbinlog形成的统计结果，也会用于产生保留现场数据文件的建表语句
#         :return:
#    """
#    def __init__(self):
#        pass
#



def deal_all_event(parameter):
    """
     读日志，处理mysqlbinlog的日志
    :param parameter: 用户的命令行的参数和一些全局的变量
    :return:
    :except:如果有问题，会抛出异常
    """
    events=[]
    if parameter.skip_insert is False:
        events.append(WriteRowsEvent)
    if parameter.skip_update is False:
        events.append(UpdateRowsEvent)
    if parameter.skip_delete is False:
        events.append(DeleteRowsEvent)
    stream = BinLogStreamReader(
        connection_settings=parameter.mysql_setting,
        server_id=1294967255,
        #blocking=True,
        only_events=events,
        #TODO 事件是否需要配置？先不管吧
        #only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent,QueryEvent,XidEvent],
        only_tables=parameter.table_name_array,
        only_schemas=parameter.schema_array,
        #log_file='mysql-bin.000840',
        log_file=parameter.start_binlog_file,
        log_pos=parameter.start_position,
        end_to_timestamp=parameter.end_to_timestamp,
        #log_pos=6178,
        skip_to_timestamp=parameter.start_to_timestamp,
        #log_pos=226812,
        resume_stream=True,
        process_interval=Constant.PROCESS_INTERVAL
           )
    parameter.stream=stream
    logger.debug("begin to deal event,stream object={0}".format(stream))
    if (parameter.dump_event is True):
        dump_events(stream)
        return
    for event in stream:
        #pdb.set_trace()
        #TODO begin，commit太多需要在dml的前后加合适,没有做了,先不需要俘获这个事件
        logger.debug("event type={0},logfile={1}".format(type(event),stream.log_file))
        if isinstance(event,QueryEvent):
            deal_query(event,parameter.file,parameter.stat)
        elif isinstance(event,XidEvent):
            deal_xid(event,parameter.file,parameter.stat)
        elif isinstance(event, DeleteRowsEvent):
            deal_delete_rows(event,parameter.file,parameter.stat,logfile=stream.log_file,add_schema_name=parameter.add_schema_name)
        elif isinstance(event, UpdateRowsEvent):
            deal_update_rows(event,parameter.file,parameter.stat,logfile=stream.log_file,add_schema_name=parameter.add_schema_name)
        elif isinstance(event, WriteRowsEvent):
            deal_insert_rows(event,parameter.file,parameter.stat,logfile=stream.log_file,add_schema_name=parameter.add_schema_name)

    #logger.debug(stream.table_map[70].__dict__)




def dump_events(stream):
     """
        调试目的，dump event到屏幕上，不会到日志文件中
        :param stream:
        :return:
     """
     for binlogevent in stream:
        #pdb.set_trace()
        #print(type(binlogevent))
        if isinstance(binlogevent,QueryEvent):
            #pass
            print(u"++++{0}".format(binlogevent.query))
        if isinstance(binlogevent,XidEvent):
            print("++++{0}".format("commit"))
        binlogevent.dump()
     stream.close()

def deal_common_event(event,file_dict,logfile=None):
    """
      处理事件的公共函数，写时间戳，位置等公共信息
    :param event: mysql的事件
    :param file_dict: 文件描述符，包括：反向sql文件,保留现场数据文件
    :return:
    """

    content=u"#end_log_pos {0}  {1} {2} {3}".format(event.packet.log_pos,
                                            datetime.fromtimestamp(event.timestamp).isoformat(),event.timestamp,logfile)
    write_file(file_dict["flashback"],content)
    write_file(file_dict["data"],content)


def deal_query(event,file_dict,stat,keep_data=False,keep_current=False):
    """
      处理query事件，仅仅写begin语句到文件中，其他忽略
    :param event: mysql的事件
    :param file_dict: 文件描述符，包括：反向sql文件,保留现场数据文件
    :param stat:mysqlbinlog形成的统计结果
    :param keep_data:产生保留现场数据中的老、值和新值
    :param keep_current：产生保留现场数据中的当前值
    :return:
    """
    content=u"{0}".format(event.query)

    if content=='BEGIN':
      deal_common_event(event,file_dict)
      logger.debug(u"Query event content={0}".format(event.query))
    write_file(file_dict["flashback"],content)
    write_file(file_dict["data"],content)




def deal_xid(event,file_dict,stat,keep_data=False,keep_current=False):
    """
      处理xid事件，仅仅写commit语句到文件中
    :param event: mysql的事件
    :param file_dict: 文件描述符，包括：反向sql文件,保留现场数据文件
    :param stat:mysqlbinlog形成的统计结果
    :return:
    """
    content="commit"
    deal_common_event(event,file_dict)
    logger.debug(u"xid event content")
    stat["commit"] += 1
    write_file(file_dict["flashback"],content)
    write_file(file_dict["data"],content)


def deal_insert_rows(event,file_dict,stat,keep_data=False,keep_current=False,logfile=None,add_schema_name=False):
    """
      处理delete事件，反向生成insert语句，保留现场数据到文件
    :param event: mysql的事件
    :param file_dict: 文件描述符，包括：反向sql文件,保留现场数据文件
    :param stat:mysqlbinlog形成的统计结果
    :return:
    """
    deal_common_event(event,file_dict,logfile)
    for row in event.rows:
        sql=joint_delete_sql(event.schema,event.table,event.primary_key,row,add_schema_name)
        sql_keep=joint_keep_data_sql("insert",event.timestamp,event.table,row)
        write_file(file_dict["flashback"],sql)
        write_file(file_dict["data"],sql_keep)
        add_stat(stat,"delete",event.schema,event.table)

def deal_delete_rows(event,file_dict,stat,keep_data=False,keep_current=False,logfile=None,add_schema_name=False):
    """
      处理delete事件，反向生成insert语句，保留现场数据到文件
    :param event: mysql的事件
    :param file_dict: 文件描述符，包括：反向sql文件,保留现场数据文件
    :param stat:mysqlbinlog形成的统计结果
    :return:
    """
    deal_common_event(event,file_dict,logfile)
    for row in event.rows:
        sql=joint_insert_sql(event.schema,event.table,event.primary_key,row,add_schema_name)
        sql_keep=joint_keep_data_sql("delete",event.timestamp,event.table,row)
        write_file(file_dict["flashback"],sql)
        write_file(file_dict["data"],sql_keep)
        add_stat(stat,"insert",event.schema,event.table)

def deal_update_rows(event,file_dict,stat,keep_data=False,keep_current=False,logfile=None,add_schema_name=False):
    """
      处理update事件，反向生成insert语句，保留现场数据到文件
    :param event: mysql的事件
    :param file_dict: 文件描述符，包括：反向sql文件,保留现场数据文件
    :param stat:mysqlbinlog形成的统计结果
    :return:
    """
    #logger.debug("===logfile {0}".format(logfile))
    deal_common_event(event,file_dict,logfile)
    for row in event.rows:
        sql=joint_update_sql(event.schema,event.table,event.primary_key,row,Constant.UPDATE_PK,add_schema_name)
        sql_keep=joint_keep_data_sql("update",event.timestamp,event.table,row)
        write_file(file_dict["flashback"],sql)
        write_file(file_dict["data"],sql_keep)
        add_stat(stat,"update",event.schema,event.table)

def add_stat(stat,op_type,schema,table):
    """
      统计生成了多少sql
    :param stat:格式为stat["flash_sql"]["schema"]["table"]["操作类型"] ：
    :param op_type: update/delete/insert
    :param schema:
    :param table:
    :return:
    """
    if not stat["flash_sql"].has_key(schema):
       stat["flash_sql"][schema]={}
    if not stat["flash_sql"][schema].has_key(table):
        stat["flash_sql"][schema][table]={}
        stat["flash_sql"][schema][table]["update"]=0
        stat["flash_sql"][schema][table]["insert"]=0
        stat["flash_sql"][schema][table]["delete"]=0
    stat["flash_sql"][schema][table][op_type]+=1


def write_file(file,content):
    """
    写文件
    :param file:
    :param content:
    :return:
    """
    file.write(content)
    file.write(";\n")





def generate_create_table(parameter):
    """
     产生建表语句
    :param parameter:
    :return:
    """
    mysql_table=MysqlTable()
    for schema in parameter.stat["flash_sql"]:
        for table in parameter.stat["flash_sql"][schema]:
           mysql_table.connect(parameter.mysql_setting)
           arr=mysql_table.get_columns(schema,table)
           sql=join_create_table(schema,table,arr)
           write_file(parameter.file["data_create"],sql)

def convert_datetime_to_timestamp(dt,format=None):
    if isinstance(dt,str):
        end_time=datetime.strptime(dt, format)
    else:
        end_time=dt
    end_to_timestamp=mktime(end_time.timetuple())
    return end_to_timestamp