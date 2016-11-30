#目前运行情况
现在已经在阿里的rds上，db为utf8字符集的生产环境下使用
#工具简介
##概述
mysqlbinlog_back.py 是在线读取row格式的mysqld的binlog,然后生成反向的sql语句的工具。一般用于数据恢复的目的。
所谓反向的sql语句就是如果是insert，则反向的sql为delete。如果delete,反向的sql是insert,如果是update, 反向的sql还是update,但是update的值是原来的值。

最简单的例子为
python mysqlbinlog_back.py --host="127.0.0.1" --username="root" --port=43306 --password="" --schema=test --table="test5" 

下面是程序输出结果 
 ls -l log/* 
 
  -rw-r--r-- 1 root root 2592 Nov 9 15:44 log/save_data_dml_test_20161109_154434.sql  
  -rw-r--r-- 1 root root 1315 Nov 9 15:44 log/flashback_test_20161109_154434.sql <--- 反向sq文件  
  -rw-r--r-- 1 root root 441 Nov 9 15:44 log/save_data_create_table_test_20161109_154434.sql

它会在线连接参数指定mysql,读取binlog,仅仅抽取对schema为test 表名test5的binlog，生成反向sq文件保存在log目录下,其中flash_开头的文件是反向的sql语句。

##详细描述
mysqlbinlog_back.py在线连接参数指定mysql,读取binlog,如果缺省，它通过show binary logs命令找到最近的binlog文件，从文件开头开始解析，一直解析到当前时间退出。

如果指定开始binary log文件名和位置（BINLOG_START_FILE_NAME，BINLOG_START_FILE_POSITION），会从指定binary log文件名和位置开始解析，一直BINLOG_END_TIME结束，中间会自动扫描跨多个binlog.

生成文件目录可以通过OUTPUT_FILE_PATH来指定。目录下有2个类：
一类是反向解析的文件,格式为flashback_schema名_当前时间.sql . 
另一类用于审查数据的sql,审查数据的sql用于记录操作类型，sql的老、新值。其中, save_data_create_table_开头的文件用于生成建表语句，save_data_dml用于插入到新的表中。

##参数说明
python mysqlbinlog_back.py --help 看在线的帮助
另外也可以看一下CHANGELOG.txt

##依赖的包和环境
python2.6 
pymysql

#使用限制
1.支持mysql版本为MySQL 5.5 and 5.6.因为底层使用的是python-mysql-replication包。

2.数据库必须是row格式的。原因看这个链接

3.反向生成的表必须有主键。

4.日志必须在主库存在

5.反向生成的mysql数据类型列出在下面。没有列出的类型没有经过严格的测试，也许有问题

6.支持的类型

允许解析的字段类型，不在里面的会报错

  ALLOW_TYPE={  "varchar":True,  "char":True,  "datetime":True,  "date":True,  "time":True,  "timestamp":True,  "bigint":True,  "mediumint":True, 
    "smallint":True,  "tinyint":True,  "int":True,  "smallint":True,  "decimal":True,  "float":True,  "double":True,  "longtext":True,  "tinytext":True, 
     "text":True,  "mediumtext":True  }

#FAQ
1.mysqlbinlog_back.py 是否对数据库性能造成影响？
基本没有影响。因为代码对mysql的操作就是2种，第一种是伪装成mysql的从库去在线读取日志，对mysql的压力就是传输一下binlog.第二种会读取information_schema.columns系统表

2.对mysql字符集的支持什么
utf8测试通过。gbk方式没有测试，应该问题不大。mtf8m4没有测试
原理角度python都用utf8的方式读出数据，内部转换成unicode的方式，然后写文件输出到utf8编码格式的文件

3.对mysql时间类型的支持是什么
datetime没有时区的概念，所以是啥就是啥。
timestamp经过python转换成datetime,转换成运行程序的环境时区相关的时间

4.底层是用的python-mysql-replication 包，是否可以用原生态的python-mysql-replication替换呢？
不行，因为原生态的包开发的接口不够多，有些功能不具备。所以在它的代码基础上改了部分

5.指定event位置时是否会找出语句的丢失?
一定不能指定位置时指定在dml的位置，位置至少应该在dml之前的table_map的位置，当然更加好的位置应该是在事物开始的位置，也就是begin的位置。
因为一个dml会对应2个event,一个table_map，另一个是dml的event