#目前运行情况
现在已经在阿里的rds上，db为utf8字符集的生产环境下使用
#工具简介
##概述
mysqlbinlog_back.py 是在线读取row格式的mysqld的binlog,然后生成反向的sql语句的工具。一般用于数据恢复的目的。
所谓反向的sql语句就是如果是insert，则反向的sql为delete。如果delete,反向的sql是insert,如果是update, 反向的sql还是update,但是update的值是原来的值。

最简单的例子为
python mysqlbinlog_back.py --host="127.0.0.1" --username="root" --port=43306 --password="" --schema=test --table="test5" 
 下面是程序输出结果  ls -l log/*  -rw-r--r-- 1 root root 2592 Nov 9 15:44 log/save_data_dml_test_20161109_154434.sql   -rw-r--r-- 1 root root 1315 Nov 9 15:44 log/flashback_test_20161109_154434.sql <--- 反向sq文件   -rw-r--r-- 1 root root 441 Nov 9 15:44 log/save_data_create_table_test_20161109_154434.sql

它会在线连接参数指定mysql,读取binlog,仅仅抽取对schema为test 表名test5的binlog，生成反向sq文件保存在log目录下,其中flash_开头的文件是反向的sql语句。

##详细描述
mysqlbinlog_back.py在线连接参数指定mysql,读取binlog,如果缺省，它通过show binary logs命令找到最近的binlog文件，从文件开头开始解析，一直解析到当前时间退出。
如果指定开始binary log文件名和位置（BINLOG_START_FILE_NAME，BINLOG_START_FILE_POSITION），会从指定binary log文件名和位置开始解析，一直BINLOG_END_TIME结束，中间会自动扫描跨多个binlog.
向生成文件目录可以通过OUTPUT_FILE_PATH来指定。目录下有2个类：一类是反向解析的文件,格式为flashback_schema名_当前时间.sql . 另一类用于审查数据的sql,审查数据的sql用于记录操作类型，sql的老、新值。其中, save_data_create_table_开头的文件用于生成建表语句，save_data_dml用于插入到新的表中。