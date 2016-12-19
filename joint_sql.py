#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
 v0.1.2 2016/12/19 yilai 建表改成utf8mb4字符集
 v0.1.1  2016/09/21  yilai    加了些参数
 v0.1.0  2016/07/20  yilai    created
"""
from datetime import datetime,date,timedelta
from decimal import Decimal
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
from constant import Constant

logger = logging.getLogger("__main__")

def joint_insert_sql(schema,table,pk,row,add_schema_name=False):
    """
      拼接insert语句
    :param schema:
    :param table:
    :param pk: 主键列名，数组类型
    :param row: 分析binlog的数据，包括字段名和值
    :return: sql
    """
    logger.debug(u"schema={0},table={1},pk={2},row={3}".format(schema,table,pk,row))
    #sql=u"update {0} set {1} where {2}"
    table_name=generate_table_name(schema,table,False,add_schema_name)

    (columns,values)=generate_two_array_column_and_value(row["values"])
    statment_column=",".join(columns)
    statment_value=",".join(values)
    sql=u"insert into "+table_name+"("+statment_column+ ") values("+statment_value+")"
    logger.debug(sql)
    return sql


def joint_update_sql(schema,table,pk,row,update_pk=True,add_schema_name=False):
    """
      拼接update语句
    :param schema:
    :param table:
    :param pk: 主键列名，数组类型
    :param row: 分析binlog的数据，包括字段名和值
    :param update_pk 是否产生更新pk的语句
    :return: sql
    """
    logger.debug(u"schema={0},table={1},pk={2},row={3}".format(schema,table,pk,row))
    #sql=u"update {0} set {1} where {2}"
    table_name=generate_table_name(schema,table,False,add_schema_name)
    #TODO ,key没有加escape

    if update_pk:
        statment_dict=row["before_values"]
    else:
        (statment_dict,null)=split_dict_column_value_pair(pk,row["before_values"])
    (null,pk_dict)=split_dict_column_value_pair(pk,row["after_values"])
    statment=generate_array_column_value_pair(statment_dict)
    statment=",".join(statment)
    where =generate_array_column_value_pair(pk_dict)
    where = " and ".join(where)
    sql=u"update "+table_name+" set"+statment+ " where "+where
    logger.debug(sql)
    return sql

def joint_delete_sql(schema,table,pk,row,add_schema_name=False):
    """
      拼接delete语句
    :param schema:
    :param table:
    :param pk: 主键列名，数组类型
    :param row: 分析binlog的数据，包括字段名和值
    :return: sql
    """
    logger.debug(u"schema={0},table={1},pk={2},row={3}".format(schema,table,pk,row))
    if pk is None or pk is "":
        raise ValueError(u"could not find primary key for table,schema={0},table={1}".format(schema,table))
    table_name=generate_table_name(schema,table,False,add_schema_name)
    temp=generate_dict_pk(pk,row["values"])
    where =generate_array_column_value_pair(temp)
    where = " and ".join(where)
    sql=u"delete from "+table_name + " where "+where
    logger.debug(sql)
    return sql


def generate_dict_pk(pk,row):
    """
     产生基于主键的dict类型的{列名，列值}
    :param pk: 可能是string或者tuple
    :param row:
    :return:{列名，列值}
    """
    dt={}
    if isinstance(pk,unicode):
        dt[pk]=row[pk]
    else:
       for key in pk :
         dt[key]=row[key]
    return dt

def split_dict_column_value_pair(pk,row):
    """
     产生基于主键的dict类型的{列名，列值}
    :param pk: 可能是string或者tuple
    :param row:
    :return: 其他列字典{列名，列值} ，主键字典{列名，列值}
    """
    pk_orgi={}
    pk_dt={}
    other_dt={}
    if isinstance(pk,unicode):
        pk_orgi[pk]=""
    else:
        for key in pk:
            pk_orgi[key]=""
    for key in row:
       if pk_orgi.has_key(key):
           pk_dt[key]=row[key]
       else:
           other_dt[key]=row[key]
    return (other_dt,pk_dt)


def generate_table_name(schema,table,keep_data=False,add_schema_name=True):
    """
     拼接表名
    :param schema:
    :param table:
    :param keep_data:是否产生保留数据的表名
    :return:
    """
    content=""
    if keep_data:
        content=Constant.KEEP_DATA_TABLE_TEMPLATE.format(table)
    else:
        if add_schema_name:
            content=u'`'+ schema+ '`'+ ".`" + table + "`"
        else:
            content=u'`'+ table + "`"
    return content

def generate_array_column_value_pair(row):
    """
        产生 “列=列值” 的数组
    :param row: 格式为row["列名"]=列值
    :return:“列=列值” 的数组
    """
    logger.debug(u"dump row={0}".format(row))
    content=[]
    for key in row:
        logger.debug(u"dump row ele: {0}={1}".format(key,row[key]))
        ele="`"+key+"`"+"="+to_string(row[key],"'")
        content.append(ele)
    return content

def to_string(val,prefix=""):
    """
    把任何类型转换成string
    :param val:
    :param prefix:对于字符串，加prefix前缀
    :return:
    """
    if isinstance(val,unicode):
        ret= u"%s%s%s" % (prefix,escape_unicode(val),prefix)
        #ret= u"'%s'" % escape_unicode(val)
    elif  isinstance(val,str):
        ret= u"%s%s%s" % (prefix,escape_string(val),prefix)
    elif isinstance(val,datetime) or  isinstance(val,date) or isinstance(val,timedelta):
        ret=u"'"+str(val)+u"'"
    else:
        ret=str(val)
    return ret

def escape_string(value):
        """escape_string escapes *value* but not surround it with quotes.
        """
        value = value.replace('\\', '\\\\')
        value = value.replace('\0', '\\0')
        value = value.replace('\n', '\\n')
        value = value.replace('\r', '\\r')
        value = value.replace('\032', '\\Z')
        value = value.replace("'", "\\'")
        value = value.replace('"', '\\"')
        return value


_escape_table = [unichr(x) for x in range(128)]
_escape_table[0] = u'\\0'
_escape_table[ord('\\')] = u'\\\\'
_escape_table[ord('\n')] = u'\\n'
_escape_table[ord('\r')] = u'\\r'
_escape_table[ord('\032')] = u'\\Z'
_escape_table[ord('"')] = u'\\"'
_escape_table[ord("'")] = u"\\'"
#_escape_table[ord("'")] = u"\'"
def escape_unicode(value, mapping=None):
    """escapes *value* without adding quote.

    Value should be unicode
    """
    return value.translate(_escape_table)



def generate_two_array_column_and_value(row):
     """
        产生 “列  的和“列值”两个数组
    :param row: 格式为row["列名"]=列值
    :return:
    """

     logger.debug(u"dump row={0}".format(row))
     columns=[]
     values=[]
     for key in row:
        logger.debug(u"dump row ele: {0}={1},type={2}".format(key,row[key],type(row[key])))
        columns.append("`"+key+"`")
        values.append(to_string(row[key],"'"))
     return (columns,values)



def joint_keep_data_sql(op,op_timestamp,table,row,keep_current=False):
    """

    :param op: 操作类型，insert,update,delete
    :param op_timestamp:
    :param table
    :param row: 数据
    :param keep_current: 是否去当前值
    :return: insert的语句
    """
    op_datetime=datetime.fromtimestamp(op_timestamp)
    content_dict={}
    content_dict[u"op"]=op
    content_dict[u"op_datetime"]=op_datetime

    if (op=="update"):
        generate_keep_data_dict(content_dict,"bfr",row["before_values"])
        generate_keep_data_dict(content_dict,"aft",row["after_values"])
    elif (op=="insert"):
        generate_keep_data_dict(content_dict,"aft",row["values"])
    else:
        generate_keep_data_dict(content_dict,"bfr",row["values"])
    """
    if (op=="update"):
         new_row=row["before_values"]
         for key in new_row:
            content_dict[u"bfr_"+key]=new_row[key]
         new_row=row["after_values"]
         for key in new_row:
            content_dict["aft"+key]=new_row[key]
    elif (op=="insert"):
         new_row=row["values"]
         for key in new_row:
            content_dict["aft"+key]=new_row[key]
    else:
        new_row=row["values"]
        for key in new_row:
            content_dict["bfr"+key]=new_row[key]
    """
    table_name=generate_table_name("",table,True)
    (columns,values)=generate_two_array_column_and_value(content_dict)
    statment_column=",".join(columns)
    statment_value=",".join(values)
    sql=u"insert into "+table_name+"("+statment_column+ ") values("+statment_value+")"
    logger.debug(sql)
    return sql


def generate_keep_data_dict(target,prefix,row):
    for key in row:
        target[u""+prefix+"_"+key]=row[key]
    return target

def join_create_table(schema,table,colums):
    """
      产生建表语句
    :param schema:
    :param table:
    :param colums:列定义的数组
    :return:
    """
    #TODO timestamp的时区有问题吗？应该不会，python会自动转换为当前的时区
    columns_def=[u"op varchar(64)",u"op_datetime datetime"]
    table_name=generate_table_name("",table,True)
    for row in colums:
        check_mysql_type(row["COLUMN_TYPE"])
        columns_def_ele=u"bfr_"+row["COLUMN_NAME"] + " "+row["COLUMN_TYPE"]
        columns_def.append(columns_def_ele)
    for row in colums:
        columns_def_ele=u"aft_"+row["COLUMN_NAME"] + " "+row["COLUMN_TYPE"]
        columns_def.append(columns_def_ele)
    columns_def_str=",".join(columns_def)

    sql=u"CREATE TABLE "+table_name+" ("+columns_def_str+")"+" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    return sql


def check_mysql_type(mysql_type):
    """
      检查mysql的类型，因为每种类型都需要详细的测试，防止产生的sql有问题
    :param mysql_type:
    :return:
    """
    #mysql_type=""
    ele=mysql_type.split(" ")[0].split("(")[0].lower()
    if not Constant.ALLOW_TYPE.has_key(ele):
        raise ValueError(u"find unsupport mysql type to flash,type={0}".format(mysql_type))


