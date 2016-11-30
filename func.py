#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
 公共函数
 v0.1.1  2016/09/21  yilai    加了些参数
 v0.1.0  2016/07/20  yilai    created
"""
import logging
from logging.handlers import RotatingFileHandler
import traceback

logger = logging.getLogger("__main__")

def init_logger(name,log_level=logging.DEBUG,screen_output=True):
    """
    ！！注意，这个函数只能一次调用哦
    初始化日志，将来日志会写到文件和打印到屏幕上
    :parameter : name:日志的文件名  log_level:日志的级别  screen_output：日志也输出屏幕上
    :return:
    :exception :需要自己catch异常
    """

    LOGFILE = name
    maxBytes = 5 * 1024 * 1024
    backupCount = 1

    #logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    #logger.setLevel(logging.INFO)
    #设置文件输出到文件中
    #ch = TimedRotatingFileHandler(LOGFILE, 'S', 5, 1)
    ch = RotatingFileHandler(LOGFILE, 'a', maxBytes, backupCount)

    #format='%(asctime)s [%(filename)s][%(process)d][%(levelname)s][%(lineno)d] %(message)s'
    format='%(asctime)s [%(filename)s][%(levelname)s][%(lineno)d] %(message)s'
    formatter = logging.Formatter(format)

    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if screen_output:
        #设置log输出到屏幕上
        chscreen = logging.StreamHandler()
        #chscreen.setLevel(logging.INFO)
        chscreen.setLevel(log_level)
        logger.addHandler(chscreen)
    #print("===log will also  write to {0}===".format(LOGFILE))
    logger.info("===log will also  write to {0}===".format(LOGFILE))

def print_stack():
    """
    打印堆栈
    :return:
    """
    logger.error( "=====Additional info:dump stack to diagnose =======")
    logger.error(traceback.format_exc())
    #sys.exit(1)



