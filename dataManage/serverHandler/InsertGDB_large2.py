# coding:utf-8
# 转换大文件GDB，固定图层数版本！

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
timeStr = time.strftime("%Y%m%d", time.localtime())

# 初始化日志模块
logger = None
logOutputPath = os.path.join(runWorkspace, 'workspace', 'logs', timeStr)
if not os.path.exists(logOutputPath):
    os.makedirs(logOutputPath)


def mapProcess(targetFile, serverName, x, y, sourceFileFields, gdbWorkspace, fileEncode, convertSr):
    try:
        fileIndex = os.path.basename(targetFile).split('.')[0].split('_')[-1]
        fileName = "%s%s" % (serverName, fileIndex)
        logger.info('insertLarge_GDB ** convert file:%s' % (fileName))

        layerPath = os.path.join(gdbWorkspace, fileName)
        # 插入记录到gdb
        extent = ac.insertRow_Point(layerPath, targetFile, x, y, sourceFileFields,
                                    fileEncode, convertSr, logger)

        return [fileName, os.path.split(outputGDBPath)[1] + os.path.sep + fileName, extent, fileIndex]

    except:
        logger.warning(traceback.format_exc())
        raise


def buildGDB(sourceFilePath, gdbPath, serverName, x, y, sourceFileFields, fileEncode, convertSr):
    try:
        # 根据线程数创建GDB空间
        extentList = []
        for f in os.listdir(sourceFilePath):
            targetFile = os.path.join(sourceFilePath, f)
            if os.path.isfile(targetFile):
                extent = mapProcess(targetFile, serverName, x, y, sourceFileFields, gdbPath, fileEncode, convertSr)
                extentList.append(extent)

        index = 0
        extentL = []
        for extent in extentList:
            index += 1
            extentL.append([extent[0], extent[2], extent[3]])

        buildExtentLayer(gdbPath, os.path.join(runWorkspace, 'workspace', 'wgs84.prj'), extentL)
        return True
    except Exception as e:
        logger.warning(e.message)
        logger.warning(traceback.format_exc())
        return False


def buildExtentLayer(outputPath, projFile, extentList):
    try:
        fieldsMap = {'LAYERNAME': "TEXT", 'INDEX': "integer", "XMAX": "float", "XMIN": "float", "YMAX": "float",
                     "YMIN": "float"}
        ac.createLayer(outputPath, 'extent', projFile, "POLYGON", fieldsMap)

        # fields = ["LAYERNAME", "XMAX", "XMIN", "YMAX", "YMIN", "SHAPE@"
        for i in xrange(len(extentList)):
            layerName = extentList[i][0]
            extent = extentList[i][1]
            index = extentList[i][2]
            xmax = extent[0]
            ymax = extent[1]
            xmin = extent[2]
            ymin = extent[3]
            ac.insertExtentFeature(os.path.join(outputPath, 'extent'), layerName, index, xmax, xmin, ymax, ymin, logger)
    except Exception as e:
        logger.warning(e.message)
        logger.warning(traceback.format_exc())
        raise


if __name__ == '__main__':
    # sourceFilePath = r'D:\20170701'
    # outputGDBPath = r'C:\Users\LocalUser\Desktop\test\customertest.gdb'
    # serverName = 'customertest'
    # x = 13
    # y = 14
    # sourceFileFields = "UID,T1,T7,T30,T90,T180,BANKID1,BANKID2,AGE,SEX,ASSETS_DIS,BANKCARD_D,BRANCHID,X,Y".split(',')
    # pid = 'werw'
    # fileEncode = 'utf-8'
    # convertSr = 'wgs84|webmercator'

    sourceFilePath = sys.argv[1]  # 带经纬度坐标信息文件
    outputGDBPath = sys.argv[2]  # 转换GDB输出路径
    serverName = sys.argv[3]  # 数据服务名
    templatePath = sys.argv[4]  # 图层模板路径
    x = int(sys.argv[5])  # 经度序号
    y = int(sys.argv[6])  # 纬度序号
    sourceFileFields = sys.argv[7].split(',')  # 数据源文件表头
    pid = sys.argv[8]
    fileEncode = sys.argv[9]
    convertSr = sys.argv[10]

    logger = Logger(
        logname=os.path.join(logOutputPath, "largeGDB_%s.log" % pid), loglevel=3,
        callfile=__file__).get_logger()

    logger.info('''start convert GDB...
                   source File Path:%s
                   output GDBPath:%s
                   log Path:%s''' % (
        sourceFilePath, outputGDBPath, os.path.join(runWorkspace, 'workspace', "logs")))
    status = buildGDB(sourceFilePath, outputGDBPath, serverName, x, y, sourceFileFields, fileEncode, convertSr)

    if status:
        logger.info('insertLarge_GDB ** convert success pid(%s)!' % pid)
        print "sshsuccess"
    else:
        logger.info('insertLarge_GDB ** convert error pid(%s)!' % pid)
        print "ssherror"
