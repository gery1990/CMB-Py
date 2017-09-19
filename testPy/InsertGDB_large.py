# coding:utf-8
# 转换大文件GDB，按自定义最大记录数平均划分图层

import os, shutil, time, sys, logging
import traceback
from  multiprocessing import Manager
from  multiprocessing import Pool
import arcpyOp as ac

runWorkspace = os.path.split(sys.path[0])[0]
sys.path.append(runWorkspace)

from common.logModel import Logger
from common.cutCSV import CutFile

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
timeStr = time.strftime("%Y%m%d", time.localtime())

# 初始化日志模块
logger = None
logOutputPath = os.path.join(runWorkspace, 'workspace', 'logs', timeStr)
if os.path.exists(logOutputPath) is not True:
    os.makedirs(logOutputPath)


def mapProcess(ags):
    fileName = ags[0]
    sourcePath = ags[1]
    x = ags[2]
    y = ags[3]
    sourceFileFields = ags[4]
    gdbPathList = ags[5]
    extentList = ags[6]
    outputPath = gdbPathList.pop()
    mapLog = logging.getLogger(__file__)
    if len(mapLog.handlers) == 0:
        mapLog = Logger(
            logname=os.path.join(logOutputPath, "largeGDB_process_%s.log" % os.getpid()),
            loglevel=3, callfile=__file__).get_logger()  # 感谢匿名网友指正
    try:
        mapLog.info('PID(%s) convert GDB:%s' % (os.getpid(), fileName))

        layerPath = os.path.join(outputPath, fileName)
        ac.copyLayers(os.path.join(outputPath, "tempLayer"), layerPath)
        layerFields = ac.getLayerFields(layerPath)
        # 插入记录到gdb
        extent = ac.insertRow_Point(layerPath, sourcePath, x, y, sourceFileFields,
                                    layerFields, mapLog)
        extentList.append([fileName, os.path.split(outputPath)[1] + os.path.sep + fileName, extent])
        # os.remove(sourcePath)  # 把源文件删除

        gdbPathList.append(outputPath)
        mapLog.info('PID(%s) convert GDB:%s  Done' % (os.getpid(), fileName))

        if len(extent) > 0:
            mapLog.info('PID(%s) GDB extent:%s' % (
                os.getpid(), "%f,%f,%f,%f" % (extent[0], extent[1], extent[2], extent[3])))
    except:
        gdbPathList.append(outputPath)
        mapLog.warning(traceback.format_exc())
        raise


def buildGDB(sourceFile, outputWorkspace, serverName, templatePath, x, y, sourceFileFields, pCount, maxGDBRecodeCount):
    try:
        rowCount = calcRowCount(sourceFile)
        logger.info('file row count:%d' % rowCount)
        # rowCount = 50000000
        if (rowCount / maxGDBRecodeCount) != 0 and (rowCount / maxGDBRecodeCount) < pCount:
            pCount = rowCount / maxGDBRecodeCount
        if rowCount < maxGDBRecodeCount:
            pCount = 1
        if os.path.exists(outputWorkspace) is not True:
            os.makedirs(outputWorkspace)
        # 创建切割文件存放路径
        cutFileOutputWorkspace = os.path.join(outputWorkspace, "cutFile")
        if os.path.exists(cutFileOutputWorkspace):
            shutil.rmtree(cutFileOutputWorkspace)
        os.makedirs(cutFileOutputWorkspace)

        manager = Manager()
        gdbPathList = manager.list()
        extentL = manager.list()
        tempLayerName = 'tempLayer'
        fileList = []

        cutF = CutFile(pCount, rCount=rowCount, maxCount=maxGDBRecodeCount)
        cutCount = cutF.do(sourceFile, cutFileOutputWorkspace)

        for i in xrange(1, pCount + 1):
            gdbPath = os.path.join(outputWorkspace, "%s%d.gdb" % (serverName, i))
            if os.path.exists(gdbPath):
                shutil.rmtree(gdbPath)
            # 根据线程数创建GDB空间
            ac.createFileGDB(outputWorkspace, "%s%d" % (serverName, i))
            ac.copyLayers(templatePath, gdbPath + r'/%s' % tempLayerName)
            gdbPathList.append(gdbPath)

        # for i in xrange(len(gdbPathList)):
        #     queue.put(gdbPathList[i])

        for f in os.listdir(cutFileOutputWorkspace):
            targetFile = os.path.join(cutFileOutputWorkspace, f)
            if os.path.isfile(targetFile):
                fileList.append(
                    [f, targetFile, x, y, sourceFileFields, gdbPathList, extentL])

        pool = Pool(processes=pCount)
        for i in xrange(0, len(fileList)):
            pool.apply_async(mapProcess, args=(fileList[i],))
        pool.close()
        pool.join()
        for gdbPath in gdbPathList:
            gdbPath = gdbPath
            ac.deleteLayer(os.path.join(gdbPath, tempLayerName))

        index = 0
        extentList = []
        for extent in extentL:
            index += 1
            extentList.append([extent[0], extent[2], index])

        buildExtentLayer(os.path.join(outputWorkspace, "%s1.gdb" % (serverName)),
                         os.path.join(runWorkspace, 'workspace', 'wgs84.prj'), extentList)
        return True
    except:
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


def calcRowCount(sourcefile):
    fileObj = file(sourceFilePath, 'rU')
    count = 0
    try:
        while True:
            fileObj.next()
            count += 1
    except StopIteration:
        pass
    return count
    # len(file(sourcefile, 'rU').readlines())


if __name__ == '__main__':
    # sourceFilePath = r'C:\Users\esri\Desktop\customer5000000011'
    # outputGDBPath = r'd:\5000W'
    # serverName = 'customer'
    # templatePath = r'C:\Users\esri\Desktop\customerdata.gdb\customerdata'
    # x = 13
    # y = 14
    # sourceFileFields = "UID,T1,T7,T30,T90,T180,BANKID1,BANKID2,AGE,SEX,ASSETS_DIS,BANKCARD_D,BRANCHID,X,Y".split(',')
    # processCount = 3
    sourceFilePath = sys.argv[1]  # 带经纬度坐标信息文件
    outputGDBPath = sys.argv[2]  # 转换GDB输出路径
    serverName = sys.argv[3]  # 数据服务名
    templatePath = sys.argv[4]  # 图层模板路径
    x = int(sys.argv[5])  # 经度序号
    y = int(sys.argv[6])  # 纬度序号
    sourceFileFields = sys.argv[7].split(',')  # 数据源文件表头
    processCount = 3
    if len(sys.argv) > 8:
        processCount = int(sys.argv[8])  # 进程数量
    maxGDBCount = 1000000
    if len(sys.argv) > 9:
        maxGDBCount = sys.argv[9]

    logger = Logger(
        logname=os.path.join(logOutputPath, "largeGDB_main.log"), loglevel=3,
        callfile=__file__).get_logger()

    logger.info('''start convert GDB...
                   source File Path:%s
                   output Path:%s
                   process Count:%s
                   log Path:%s''' % (
        sourceFilePath, outputGDBPath, processCount, os.path.join(runWorkspace, 'workspace', "logs")))
    status = buildGDB(sourceFilePath, outputGDBPath,
                      serverName, templatePath, x, y, sourceFileFields, processCount, maxGDBCount)

    if status:
        logger.info('inserGDB_large success!')
        print "success"
    else:
        logger.info('inserGDB_large error!')
        print "error"
