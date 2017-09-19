# coding:utf-8
import datetime, sys, os, shutil, time, traceback
import logging.config

runWorkspace = sys.path[0]

from clientHandler.convertGDBHandler import ConvertGDB
import configModule as cm
import clientHandler.geocodeControl as gc
from common.ParamikoHandle import SSHControl
from common.cutCSV import CutFile

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

timeStr = time.strftime("%Y%m%d", time.localtime())
logOutputPath = os.path.join(runWorkspace, 'workspace', 'logs', timeStr)
if os.path.exists(logOutputPath) is not True:
    os.makedirs(logOutputPath)

logger = None
dayLimit = cm.getDayLimit()


def compareFileTime(file):
    time_of_last_mod = os.path.getmtime(file)  # 获取最后修改时间
    days_between = (time.time() - time_of_last_mod) / (24 * 60 * 60)
    if days_between > dayLimit:
        return True
    return False


def clearTimeOutData(filePath):
    if os.path.isdir(filePath):
        for v in os.listdir(filePath):
            if ".gdb" not in v and v.startswith('20'):
                if compareFileTime(filePath + os.path.sep + v):
                    logger.info('delete timeout folder:%s' % filePath + os.path.sep + v)
                    shutil.rmtree(filePath + os.path.sep + v)


def getDataFile(ft, sourcePath):
    for f in os.listdir(sourcePath):
        targetFile = os.path.join(sourcePath, f)
        if os.path.isfile(targetFile):
            if targetFile.endswith('.DAT'):
                if f.startswith(ft):
                    return targetFile


def isEmptyFile(filePath):
    fileObj = file(filePath, 'rU')
    count = 0
    try:
        while True:
            fileObj.next()
            count += 1
    except StopIteration:
        fileObj.close()
        pass
    if count > 0:
        return False
    else:
        return True


if __name__ == '__main__':

    ft = sys.argv[1]  # 获取分析的文件类型
    date = sys.argv[2]  # 获取日期

    # ft = 'customer'  # 获取分析的文件类型
    # fp = r'C:\Users\esri\Desktop\customer5000000011'  # 获取文件路径
    timeStr = time.strftime("%Y%m%d", time.localtime())

    # 初始化日志模块
    logging.config.fileConfig(cm.loggerConfig_client)
    logger = logging.getLogger('main')
    logger.info('=========================================')
    logger.info('start Data handling...')
    logger.info('arguments:(%s,%s)' % (ft, date))

    # --------------------------------------------
    try:
        d1 = datetime.datetime.now()
        fileInfo = cm.getFileType(ft)
        sourcePath = os.path.join(fileInfo["sourceFolder"], date)

        if os.path.exists(sourcePath) is not True:
            logger.warning('file path(%s) not exists' % sourcePath)
            exit(-1)
            raise
        xyFile = getDataFile(ft, sourcePath)
        if isEmptyFile(xyFile):
            logger.warning('file is empty!')
            exit(-1)

        # 如果数据不带经纬度坐标则进行地理编码
        if fileInfo["hasXY"] == 'false':
            outputPath = os.path.join(cm.getGeocodeWorkspace(), fileInfo["type"], timeStr, fileInfo["serverName"])
            if os.path.exists(outputPath):
                shutil.rmtree(outputPath)
            os.makedirs(outputPath)

            logger.info('cut big data File...')
            cutFileOutputPath = os.path.join(outputPath, 'cutFile')

            cutFile = CutFile(cm.getProcessNum(), maxCount=20000)
            fileCount = cutFile.do(xyFile, cutFileOutputPath)
            logger.info('cut bid data file done，total %d' % fileCount)

            logger.info('execute geocoding, file name :%s' % ft)
            xyFile = gc.run(cutFileOutputPath, fileInfo["fields"], fileInfo["addrField"], fileInfo["encode"],
                            cm.getProcessNum(),
                            cm.getThreadNum(), outputPath, cm.getGeocode()["url"],logOutputPath)  # 返回结果文件

            xyFile = cutFile.mergeResultLog(os.path.join(outputPath, "geocodeResult"), outputPath)

            fileInfo["x"] = (len(fileInfo["fields"]) - 1) + 1
            fileInfo["y"] = (len(fileInfo["fields"]) - 1) + 2
            fileInfo["fields"].append("X")
            fileInfo["fields"].append("Y")
            fileInfo["fields"].append("SCORE")

        if xyFile != "":
            sshClient = SSHControl(cm.getServerList(), cm.getBalanceStatus(), logger)
            convertGDB = ConvertGDB(xyFile, fileInfo, cm.getPythonTools(), cm.getServerPythonWorkspace(), sshClient,
                                    logOutputPath,logger)
            logger.info('execute GDB convert, file name :%s' % ft)
            done = convertGDB.convertGDB()
            if done is not True:
                sshClient.ssh_close()
                logger.info('execute GDB convert failed, file name :%s' % ft)
                exit(-1)

            # 执行数据更新，
            logger.info('execute GDB update, file name :%s' % ft)
            # if fileInfo["updateModel"] == "all" and fileInfo["cutGDB"] == "true":
            #     done = convertGDB.MsHandler_publish()
            # else:
            convertGDB.MSHandler_update()
            sshClient.ssh_close()

        # 清理超时的旧文件
        clearTimeOutData(cm.getGeocodeWorkspace() + os.path.sep + fileInfo["type"])  # 清理地理编码结果备份
        clearTimeOutData(cm.getGDBWorkspace() + os.path.sep + fileInfo["type"])  # 清理GDB生成结果备份
        clearTimeOutData(cm.getGISServerGDB() + os.path.sep + fileInfo["type"])  # 清理ArcGIS Server的备份

        d2 = datetime.datetime.now()
        logger.info('data handling done，times：%s' % str(d2 - d1))
        exit(0)
    except Exception as e:
        logger.warning(traceback.format_exc())
        logger.warning('data handling error exit')
        exit(-1)
