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
if os.path.exists(os.path.join(runWorkspace, 'workspace', 'logs', timeStr)) is not True:
    os.mkdir(os.path.join(runWorkspace, 'workspace', 'logs', timeStr))


def ConvertGDB(sourceFilePath, outputGDBPath, serverGDBPath, serverName, templatePath, x, y, uniqueId,
               updateModel, sourceFileFields, fileEncode, convertSr):
    try:
        logger.info('insertUpdate ** update GDB soucefile:%s' % sourceFilePath)
        # 临时gdb目录
        gdbPath = os.path.join(outputGDBPath, serverName + ".gdb")
        gdbServerPath = os.path.join(serverGDBPath, serverName + ".gdb")
        # 图层目录
        layerPath = os.path.join(gdbPath, serverName)
        if os.path.exists(gdbPath):
            shutil.rmtree(gdbPath)
        # 将旧的GDB拷贝到临时gdb路径
        shutil.copytree(gdbServerPath, gdbPath)

        # 删除旧的要素，重新插入新的纪录
        if updateModel == "update":
            ac.deleteRow(layerPath, sourceFilePath, uniqueId, sourceFileFields[uniqueId])
            # 插入记录到gdb
        ac.insertRow_Point(layerPath, sourceFilePath, x, y, sourceFileFields, fileEncode, convertSr, logger)
        return True
    except Exception as e:
        logger.warning(e.message)
        logger.warning(traceback.format_exc())
        return False


if __name__ == '__main__':

    # sourceFilePath = r'C:\Users\LocalUser\Desktop\customer50000000111'  # 带经纬度坐标信息文件
    # outputGDBPath = r'C:\Users\LocalUser\Desktop\test'  # 转换GDB输出路径
    # serverGDBPath = r'C:\Users\LocalUser\Desktop\server'
    # serverName = r'customertest'  # 数据服务名
    # templatePath = r'C:\Users\LocalUser\Desktop\template_1.gdb\customertest'  # 图层模板路径
    # x = 13  # 经度序号
    # y = 14  # 纬度序号
    # uniqueId = 0
    # updateModel = 'add'  # 更新模式：add\update
    # sourceFileFields = "UID,T1,T7,T30,T90,T180,BANKID1,BANKID2,AGE,SEX,ASSETS_DIS,BANKCARD_D,BRANCHID,X,Y".split(
    #     ',')  # 数据源文件表头
    # fileEncode = 'utf-8'
    # convertSr = 'wgs84|webmercator'

    sourceFilePath = sys.argv[1]  # 带经纬度坐标信息文件
    outputGDBPath = sys.argv[2]  # 转换GDB输出路径
    serverGDBPath = sys.argv[3]
    serverName = sys.argv[4]  # 数据服务名
    templatePath = sys.argv[5]  # 图层模板路径
    x = int(sys.argv[6])  # 经度序号
    y = int(sys.argv[7])  # 纬度序号
    uniqueId = int(sys.argv[8])
    updateModel = sys.argv[9]  # 更新模式：add\update
    sourceFileFields = sys.argv[10].split(',')  # 数据源文件表头
    fileEncode = sys.argv[11]
    convertSr = sys.argv[12]
    logger = Logger(
        logname=os.path.join(logOutputPath, "updateGDB_main.log"), loglevel=3,
        callfile=__file__).get_logger()

    logger.info('''start convert GDB...
                   source File Path:%s
                   output Path:%s
                   log Path:%s''' % (
        sourceFilePath, outputGDBPath, os.path.join(runWorkspace, 'workspace', "logs")))

    status = ConvertGDB(sourceFilePath, outputGDBPath, serverGDBPath, serverName, templatePath, x, y, uniqueId,
                        updateModel, sourceFileFields, fileEncode, convertSr)
    if status:
        logger.info('insertUpdate ** convert success!')
        print "sshsuccess"
    else:
        logger.info('insertUpdate ** convert error!')
        print "ssherror"
