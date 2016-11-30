# -*- coding: utf-8 -*-

import unittest
import sys
sys.path.append("..")
from joint_sql import *
from datetime import datetime
from decimal import Decimal
from func import init_logger
from time import mktime
import logging
#from parameter import Pama

class TestGenerate_table_name(unittest.TestCase):
  def setUp(self):
      init_logger("test1.log",logging.INFO)


  def test_generate_table_name(self):
    table_name=generate_table_name("test","test5")
    self.assertEquals(u"`test`.`test5`",table_name)
    #print(ret)

  def test_generate_array_column_value_pair(self):
    row={u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'afer1_tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 3}
    ret=generate_array_column_value_pair(row)
    self.assertEquals([u'`tdec`=15.01', u"`tlongtext`='longtext'", u'`tint`=1', u"`tvar`='afer1_tvar'", u'`tfload`=10.0100002289', u"`tdatetime`='2016-10-12 01:00:00'", u"`ttimestamp`='2016-10-13 01:00:00'", u'`id`=3'],ret)
    #self.assertEquals([1,2],[1,"2"])

  def test_to_string(self):
    val1=u'test'
    self.assertEquals(u"'test'",to_string(val1,"'"))
    self.assertEquals(u"test",to_string(val1))
    val2=u"te'st"
    #print(u"aaa"+to_string(val2,"'"))
    self.assertEquals(u"'te\\'st'",to_string(val2,"'"))
    self.assertEquals("15.01",to_string(Decimal('15.01')))
    self.assertEquals(u"'2016-10-13 01:00:00'",to_string(datetime(2016, 10, 13, 1, 0)))





  def _test_tuple(self):
     pk=u"id"
     pk1=(u'id', u'tvar')
     dt={}
     row={u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'afer1_tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 3}

     if isinstance(pk1,tuple):
        print "tuple1"

     for key in pk1:
        print key
        dt[key]=row[key]
     print dt

  def test_generate_dict_pk(self):
    pk=u"id"
    row={u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'afer1_tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 3}
    ret=generate_dict_pk(pk,row)
    self.assertEquals({u'id': 3},ret)
    pk1=(u'id', u'tvar')
    ret=generate_dict_pk(pk1,row)
    self.assertEquals({u'tvar': u'afer1_tvar', u'id': 3},ret)

  def test_joint_update_sql(self):
    schema="test"
    table="test5"
    pk=u"id"
    row={'before_values': {u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 3}, 'after_values': {u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'afer1_tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 3}}
    ret=joint_update_sql(schema,table,pk,row,False,True)
    self.assertEquals(u"update `test`.`test5` set`tdec`=15.01,`tlongtext`='longtext',`tint`=1,`tvar`='tvar',`tfload`=10.0100002289,`ttimestamp`='2016-10-13 01:00:00',`tdatetime`='2016-10-12 01:00:00' where `id`=3",ret)
    ret=joint_update_sql(schema,table,pk,row,False)
    self.assertEquals(u"update `test5` set`tdec`=15.01,`tlongtext`='longtext',`tint`=1,`tvar`='tvar',`tfload`=10.0100002289,`ttimestamp`='2016-10-13 01:00:00',`tdatetime`='2016-10-12 01:00:00' where `id`=3",ret)



  def test_joint_update_sql_case1(self):
    schema="test"
    table="test5"
    pk=u"id"
    row={'before_values': {u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 3}, 'after_values': {u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'afer1_tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 4}}
    ret=joint_update_sql(schema,table,pk,row,True,True)
    expect=u"update `test`.`test5` set`tdec`=15.01,`tlongtext`='longtext',`tint`=1,`tvar`='tvar',`tfload`=10.0100002289,`tdatetime`='2016-10-12 01:00:00',`ttimestamp`='2016-10-13 01:00:00',`id`=3 where `id`=4"
    self.assertEquals(expect,ret)
    ret=joint_update_sql(schema,table,pk,row,True,False)
    expect=u"update `test5` set`tdec`=15.01,`tlongtext`='longtext',`tint`=1,`tvar`='tvar',`tfload`=10.0100002289,`tdatetime`='2016-10-12 01:00:00',`ttimestamp`='2016-10-13 01:00:00',`id`=3 where `id`=4"
    self.assertEquals(expect,ret)


  def test_generate_two_array_column_and_value(self):
    row={u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'afer1_tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 3}
    #row={u'tdec': Decimal('15.01'),u'tlongtext': u'longtext'}
    (columns,values)=generate_two_array_column_and_value(row)
    #print(columns)
    #print(values)

  def test_joint_delete_sql(self):
    schema="test"
    table="test5"
    pk=u"id"
    row={'values': {u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 3}}
    ret=joint_delete_sql(schema,table,pk,row,True)
    self.assertEquals(u'delete from `test`.`test5` where `id`=3',ret)
    ret=joint_delete_sql(schema,table,pk,row)
    self.assertEquals(u'delete from `test5` where `id`=3',ret)

  def test_joint_insert_sql(self):
    schema="test"
    table="test5"
    pk=u"id"
    row={'values': {u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 3}}
    ret=joint_insert_sql(schema,table,pk,row,True)
    self.assertEquals(u"insert into `test`.`test5`(`tdec`,`tlongtext`,`tint`,`tvar`,`tfload`,`tdatetime`,`ttimestamp`,`id`) values(15.01,'longtext',1,'tvar',10.0100002289,'2016-10-12 01:00:00','2016-10-13 01:00:00',3)",ret)
    ret=joint_insert_sql(schema,table,pk,row)
    self.assertEquals(u"insert into `test5`(`tdec`,`tlongtext`,`tint`,`tvar`,`tfload`,`tdatetime`,`ttimestamp`,`id`) values(15.01,'longtext',1,'tvar',10.0100002289,'2016-10-12 01:00:00','2016-10-13 01:00:00',3)",ret)


  def test_split_dict_column_value_pair(self):
    pk=u"id"
    row={u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'afer1_tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0), u'id': 3}
    (other_dict,pk_dict)=split_dict_column_value_pair(pk,row)
#    print(pk_dict)
#    print(other_dict)
    self.assertEquals({u'id': 3},pk_dict)
    self.assertEquals({u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tvar': u'afer1_tvar', u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0)},other_dict)
    pk1=(u'id', u'tvar')
    (other_dict1,pk_dict1)=split_dict_column_value_pair(pk1,row)
#    print(pk_dict1)
#    print(other_dict1)
    self.assertEquals({u'tvar': u'afer1_tvar', u'id': 3},pk_dict1)
    self.assertEquals({u'tdec': Decimal('15.01'), u'tlongtext': u'longtext', u'tint': 1, u'tfload': 10.010000228881836, u'ttimestamp': datetime(2016, 10, 13, 1, 0), u'tdatetime': datetime(2016, 10, 12, 1, 0)},other_dict1)

  def test_check_mysql_type(self):
      check_mysql_type(u"int(10) unsigned")
      check_mysql_type(u"bigint(20)")
      check_mysql_type(u"BIgINT(20)")

  def _test_date(self):
    dateC=datetime.strptime( "2016-10-31 14:12:08", '%Y-%m-%d %H:%M:%S')
    timestamp=mktime(dateC.timetuple())
    print(type(timestamp))
    print(timestamp)
    dt={}
    dt["aaa"]=1
    print(dt)

  def _test_key(self):
      key="key1"
      dict={
         "key1":1,
         "key2":2
      }


if __name__ == "__main__":
    #unittest.main()
    """
    suite = unittest.TestSuite()
    suite.addTest(TestGenerate_table_name("test_check_mysql_type"))
    unittest.TextTestRunner(verbosity=3).run(suite)
    """

    suite = unittest.TestLoader().loadTestsFromTestCase(TestGenerate_table_name)
    unittest.TextTestRunner(verbosity=3).run(suite)

