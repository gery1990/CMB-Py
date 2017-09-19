# coding:utf-8
# 全量更新GDB处理程序

import os
import shutil
import sys
import time
import traceback

from arcpyControl import arcpyOp as ac

runWorkspace = os.path.split(sys.path[0])[0]
sys.path.append(runWorkspace)

from common.logModel import Logger

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
# gdb转换
timeStr = time.strftime("%Y%m%d", time.localtime())
# 初始化日志模块

logger = None
logOutputPath = os.path.join(runWorkspace, 'workspace', 'logs', timeStr)
if os.path.exists(logOutputPath) is not True:
    os.makedirs(logOutputPath)

def ConvertGDB(sourceFilePath, outputGDBPath, serverName, templatePath, x, y, sourceFileFields, fileEncode, convertSr):
    try:
        logger.info('insertGDB ** all convert GDB sourceFile:%s' % sourceFilePath)
        gdbPath = os.path.join(outputGDBPath, serverName + '.gdb')
        if os.path.exists(gdbPath):
            shutil.rmtree(gdbPath)
        ac.createFileGDB(outputGDBPath, serverName)
        if os.path.exists(gdbPath) is not True:
            logger.warning('insertGDB ** not exists GDB:%s,create first！' % gdbPath)
            return False
        # 临时GDB图层目录
        layerPath = os.path.join(gdbPath, serverName)
        ac.copyLayers(templatePath, layerPath)  # 复制模板图层

        # 插入记录到gdb
        ac.insertRow_Point(layerPath, sourceFilePath, x, y, sourceFileFields, fileEncode, convertSr, logger)
        return True
    except Exception as e:
        logger.warning(e.message)
        logger.warning(traceback.format_exc())
        return False


if __name__ == '__main__':

    # sourceFilePath = r'C:\Users\LocalUser\Desktop\customer50000000111'
    # outputGDBPath = r'C:\Users\LocalUser\Desktop\test'
    # serverName = 'customertest'
    # templatePath = r'C:\Users\LocalUser\Desktop\template_1.gdb\customertest'
    # x = 13
    # y = 14
    # sourceFileFields ="UID,T1,T7,T30,T90,T180,BANKID1,BANKID2,AGE,SEX,ASSETS_DIS,BANKCARD_D,BRANCHID,X,Y".split( ',')
    # fileEncode = 'utf-8'
    # convertSr = 'wgs84|webmercator'

    sourceFilePath = sys.argv[1]  # 带经纬度坐标信息文件
    outputGDBPath = sys.argv[2]  # 转换GDB输出路径
    serverName = sys.argv[3]  # 数据服务名
    templatePath = sys.argv[4]  # 图层模板路径
    x = int(sys.argv[5])  # 经度序号
    y = int(sys.argv[6])  # 纬度序号
    sourceFileFields = sys.argv[7].split(',')  # 数据源文件表头
    fileEncode = sys.argv[8]  # 数据源文件编码格式
    convertSr = sys.argv[9]

    logger = Logger(
        logname=os.path.join(logOutputPath, "allGDB_main.log"), loglevel=3,
        callfile=__file__).get_logger()

    logger.info('''start convert GDB...
                   source File Path:%s
                   output Path:%s
                   log Path:%s''' % (
        sourceFilePath, outputGDBPath, os.path.join(runWorkspace, 'workspace', "processLog")))

    status = ConvertGDB(sourceFilePath, outputGDBPath, serverName, templatePath, x, y, sourceFileFields, fileEncode,
                        convertSr)
    if status:
        logger.info('insertGDB ** convert success!')
        print "sshsuccess"
    else:
        logger.info('insertGDB ** convert error!')
        print "ssherror"
