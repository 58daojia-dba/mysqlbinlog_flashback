import unittest
import sys
sys.path.append("..")
from flashback import Parameter,deal_all_event,generate_create_table,add_stat
from datetime import datetime

class TestParameter(unittest.TestCase):
  def setUp(self):
    dict={}
    dict["host"]="127.0.0.1"
    dict["username"]="root"

    dict["password"]=""
    dict["start_binlog_file"]="mysql-bin.000008"
    dict["start_position"]=3306
    #dict["end_time"]="aaa"
    #dict["output_file_path"]="./"
    dict["schema"]="test"
    dict["keep_data"]=True
    parameter=Parameter(**dict)
    self.parameter=parameter

  def test_get_file_name(self):
    curtime=datetime.strftime(datetime(2016, 10, 26, 12, 30),'%Y%m%d_%H%M%S')
    #print(curtime)
    ret=self.parameter.get_file_name("flashback",curtime)
    self.assertEquals(".//flashback_test_20161026_123000.sql",ret)
    #print(ret)

  def test_add_stat(self):
    stat={}
    stat["flash_sql"]={}
    op_type="update"
    schema=u"test"
    table=u"test5"
    add_stat(stat,op_type,schema,table)
    add_stat(stat,op_type,schema,table)
    add_stat(stat,"insert",schema,table)
    self.assertEquals({'flash_sql': {u'test': {u'test5': {'insert': 1, 'update': 2, 'delete': 0}}}},stat)
    """
    for schema in stat["flash_sql"]:
        for table in stat["flash_sql"][schema]:
           print("{0}.{1}".format(schema,table))
   """
if __name__ == "__main__":
    unittest.main()