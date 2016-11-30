import unittest
import sys
sys.path.append("..")
from flashback import Parameter,deal_all_event,generate_create_table,add_stat
from datetime import datetime
from mysql_table import MysqlTable

class TestMysqlTable(unittest.TestCase):
  def setUp(self):
     pass

  def test_get_columns(self):
     dict={}
     dict["host"]="127.0.0.1"
     dict["user"]="root"
     dict["port"]=43306
     dict["charset"] = "utf8"
     mysql_table=MysqlTable()
     mysql_table.connect(dict)
     dt=mysql_table.get_columns("test","test5")
     mysql_table.connect(dict)
     dt=mysql_table.get_columns("test","test5")
     expect=[{u'COLUMN_TYPE': u'bigint(20)', u'CHARACTER_SET_NAME': None, u'COLUMN_COMMENT': u'', u'COLUMN_KEY': u'PRI', u'COLLATION_NAME': None, u'COLUMN_NAME': u'id'}, {u'COLUMN_TYPE': u'varchar(64)', u'CHARACTER_SET_NAME': u'utf8', u'COLUMN_COMMENT': u'db\u5b9e\u4f8bid', u'COLUMN_KEY': u'MUL', u'COLLATION_NAME': u'utf8_general_ci', u'COLUMN_NAME': u'tvar'}, {u'COLUMN_TYPE': u'datetime', u'CHARACTER_SET_NAME': None, u'COLUMN_COMMENT': u'sql\u6267\u884c\u65f6\u95f4', u'COLUMN_KEY': u'', u'COLLATION_NAME': None, u'COLUMN_NAME': u'tdatetime'}, {u'COLUMN_TYPE': u'float', u'CHARACTER_SET_NAME': None, u'COLUMN_COMMENT': u'sql\u6d88\u8017\u65f6\u95f4\u5fae\u79d2', u'COLUMN_KEY': u'', u'COLLATION_NAME': None, u'COLUMN_NAME': u'tfload'}, {u'COLUMN_TYPE': u'int(11)', u'CHARACTER_SET_NAME': None, u'COLUMN_COMMENT': u'sql\u8fd4\u56de\u884c\u6570', u'COLUMN_KEY': u'', u'COLLATION_NAME': None, u'COLUMN_NAME': u'tint'}, {u'COLUMN_TYPE': u'decimal(10,2)', u'CHARACTER_SET_NAME': None, u'COLUMN_COMMENT': u'sql\u6d88\u8017\u65f6\u95f4\u5fae\u79d2', u'COLUMN_KEY': u'', u'COLLATION_NAME': None, u'COLUMN_NAME': u'tdec'}, {u'COLUMN_TYPE': u'timestamp', u'CHARACTER_SET_NAME': None, u'COLUMN_COMMENT': u'sql\u6d88\u8017\u65f6\u95f4\u5fae\u79d2', u'COLUMN_KEY': u'', u'COLLATION_NAME': None, u'COLUMN_NAME': u'ttimestamp'}, {u'COLUMN_TYPE': u'longtext', u'CHARACTER_SET_NAME': u'utf8', u'COLUMN_COMMENT': u'SQL\u6587\u672c', u'COLUMN_KEY': u'', u'COLLATION_NAME': u'utf8_general_ci', u'COLUMN_NAME': u'tlongtext'}]
     self.assertEquals(expect,dt)

     try:
       mysql_table.connect(dict)
       dt=mysql_table.get_columns("test","test111")
       self.assertEquals("not go here","go here")
     except ValueError as err:
         print err.__str__()
         self.assertEquals("correct","correct")

     """
    for row in dt:
        print(row["COLUMN_NAME"]+" type "+row["COLUMN_TYPE"])
    """

  def test_get_last_binary_log_name(self):
     dict={}
     dict["host"]="127.0.0.1"
     dict["user"]="root"
     dict["port"]=43306
     dict["charset"] = "utf8"
     mysql_table=MysqlTable()
     mysql_table.connect(dict)
     dt=mysql_table.get_last_binary_log_name()
     print dt

  def test_get_current_datetime(self):
     dict={}
     dict["host"]="127.0.0.1"
     dict["user"]="root"
     dict["port"]=43306
     dict["charset"] = "utf8"
     mysql_table=MysqlTable()
     mysql_table.connect(dict)
     dt=mysql_table.get_current_datetime()
     print dt


if __name__ == "__main__":
    unittest.main()