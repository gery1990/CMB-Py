# coding:utf-8
# 合并各个gdb的extent

import os, time, sys
import traceback
from arcpyControl import arcpyOp as ac

runWorkspace = os.path.split(sys.path[0])[0]
sys.path.append(runWorkspace)

from common.logModel import Logger

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

timeStr = time.strftime("%Y%m%d", time.localtime())
logger = None
logOutputPath = os.path.join(runWorkspace, 'workspace', 'logs', timeStr)
if os.path.exists(os.path.join(runWorkspace, 'workspace', 'logs', timeStr)) is not True:
    os.mkdir(os.path.join(runWorkspace, 'workspace', 'logs', timeStr))

if __name__ == '__main__':
    logger = Logger(
        logname=os.path.join(logOutputPath, "updateGDB_main.log"), loglevel=3,
        callfile=__file__).get_logger()
    try:
        strPath = sys.argv[1]
        # strPath = r'D:\5000W\customer1.gdb,D:\5000W\customer2.gdb'
        extentList = strPath.split(',')
        list = []
        outputGDB = ""
        for extent in extentList:
            if extent.endswith('1.gdb'):
                outputGDB = os.path.join(extent, 'extent')
            else:
                list.append(os.path.join(extent, 'extent'))
        ac.appendLayers(list, outputGDB)

        for extent in list:
            ac.deleteLayer(extent)
        print "sshsuccess"
    except Exception as e:
        logger.warning(traceback.format_exc())
        print "ssherror"
