import unittest
import os,sys
import sys
sys.path.append("..")
from mysqlbinlog_back import get_check_option

class TestMysqlbinlogBack(unittest.TestCase):
  def setUp(self):
     pass

  def test_get_check_option(self):
      try:
         sys.argv=['mysqlbinlog_back.py']
         get_check_option()
         self.assertEquals("not go here","go here")
      except ValueError as err:
         self.assertEquals("correct","correct")

      try:
         sys.argv=['mysqlbinlog_back.py', '--host=aaa']
         get_check_option()
         self.assertEquals("not go here","go here")
         sys.argv=['mysqlbinlog_back.py', '--host=aaa','--username=test']
         get_check_option()
         self.assertEquals("not go here","go here")
      except ValueError as err:
         self.assertEquals("correct","correct")

      try:
         sys.argv=['mysqlbinlog_back.py', '--host=aaa','--username=test']
         get_check_option()
         self.assertEquals("not go here","go here")
      except ValueError as err:
         self.assertEquals("correct","correct")

      try:
        sys.argv=['mysqlbinlog_back.py', '--host=127.0.0.1', '--username=test', '--schema=test', '--table=test5', '--binlog_end_time=2013-11-05 11:27:13', '--binlog_start_file_name=mysql-bin.000024', '--password=test', '--binlog_start_file_position=5', '--binlog_start_time=2016-11-04 11:27:13']
        get_check_option()
        self.assertEquals("not go here","go here")
      except ValueError as err:
         print err.__str__()
         self.assertEquals("correct","correct")

      try:
        sys.argv=['mysqlbinlog_back.py', '--host=127.0.0.1', '--username=test', '--schema=test', '--table=test5', '--binlog_end_time=2013-11-05 11:27:13a', '--binlog_start_file_name=mysql-bin.000024', '--password=test', '--binlog_start_file_position=5', '--binlog_start_time=2016-11-04 11:27:13']
        get_check_option()
        self.assertEquals("not go here","go here")
      except ValueError as err:
         print err.__str__()
         self.assertEquals("correct","correct")

      try:
        sys.argv=['mysqlbinlog_back.py', '--host=127.0.0.1', '--username=test', '--schema=test', '--table=test5', '--binlog_end_time=2013-11-05 11:27:13', '--binlog_start_file_name=mysql-bin.000024', '--password=test', '--binlog_start_file_position=5', '--binlog_start_time=a2016-11-04 11:27:13']
        get_check_option()
        self.assertEquals("not go here","go here")
      except ValueError as err:
         print err.__str__()
         self.assertEquals("correct","correct")


      try:
        sys.argv=['mysqlbinlog_back.py', '--host=127.0.0.1', '--username=test', '--schema=test', '--table=test5', '--binlog_end_time=2015-11-05 11:27:13', '--binlog_start_file_name=mysql-bin.000024', '--password=test', '--binlog_start_file_position=5', '--binlog_start_time=2016-11-04 11:27:13']
        get_check_option()
        self.assertEquals("not go here","go here")
      except ValueError as err:
         print err.__str__()
         self.assertEquals("correct","correct")


      sys.argv=['mysqlbinlog_back.py', '--host=127.0.0.1', '--username=test', '--schema=test', '--table=test5', '--binlog_end_time=2016-11-05 11:27:13', '--binlog_start_file_name=mysql-bin.000024']
      opt=get_check_option()
      #print opt
      self.assertEquals({'username': 'test', 'binlog_start_file_position': 4, 'tables': 'test5', 'skip_update': False, 'output_file_path': './log', 'binlog_end_time': '2016-11-05 11:27:13', 'binlog_start_file_name': 'mysql-bin.000024', 'host': '127.0.0.1', 'version': False, 'skip_insert': False, 'binlog_start_time': None, 'password': '', 'skip_delete': False, 'port': 3306, 'add_schema_name': False, 'schema': 'test'},opt)

      sys.argv=['mysqlbinlog_back.py', '--host=127.0.0.1', '--username=test', '--schema=test', '--table=test5', '--binlog_end_time=2016-11-05 11:27:13', '--binlog_start_file_name=mysql-bin.000024', '--password=test', '--binlog_start_file_position=5', '--binlog_start_time=2016-11-04 11:27:13']
      opt=get_check_option()
      #print opt
      self.assertEquals({'username': 'test', 'binlog_start_file_position': 5, 'tables': 'test5', 'skip_update': False, 'output_file_path': './log', 'binlog_end_time': '2016-11-05 11:27:13', 'binlog_start_file_name': 'mysql-bin.000024', 'host': '127.0.0.1', 'version': False, 'skip_insert': False, 'binlog_start_time': '2016-11-04 11:27:13', 'password': 'test', 'skip_delete': False, 'port': 3306, 'add_schema_name': False, 'schema': 'test'},opt)


if __name__ == "__main__":
    unittest.main()