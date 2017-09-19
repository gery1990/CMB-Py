# encoding=utf-8
# -*- coding: ascii -*-
# 获取地址编码转换
import os, time, threading, logging, traceback, sys, codecs
from multiprocessing import Pool
from socket import setdefaulttimeout
from EncodeAddrModel import EncodeModel
from common.logModel import Logger

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

encodeCount = 0
logger = logging.getLogger('main')

# 初始化日志模块
mutex = threading.Lock()
FIELD_DEL = chr(0X7C) + chr(0X1C)  # 标准文件字段分隔符


def getFieldValue(title, linestr, field, fileEncode=None):
    try:
        value = linestr[field]  # linestr[field]
        if value == '':
            return ''
        try:
            val = value.strip()
            # return val.decode('GBK')
            if fileEncode != None:
                return val.decode(fileEncode).encode('UTF-8')
            return val.encode('UTF-8')
        except:
            return value
    except Exception as e:
        return ''


def encodeThread(fileIndex, parseObj, title, addrField, fileEncode, geocodeUrl, encodeSuccess, encodeError, mLog):
    global encodeCount
    try:
        while True:
            linestr = parseObj.next()  # 读取下一条记录
            values = linestr.split(FIELD_DEL)
            values.pop()
            oldLen = len(values)
            addr = getFieldValue(title, values, addrField, fileEncode)
            encodeModule = EncodeModel(addr, geocodeUrl)
            # encodeModule.encode()
            try:
                encodeModule.encode()
                if encodeModule.status == "OK":
                    mutex.acquire()
                    if float(encodeModule.x) and float(encodeModule.y):
                        values.append(str(encodeModule.x))
                        values.append(str(encodeModule.y))
                        values.append(str(encodeModule.score))
                    encodeCount += 1
                    if encodeCount % 1000 == 0:
                        mLog.info("%H:%M:%S", time.localtime()), "process%s complate: %d" % (
                            os.getpid(), encodeCount)
                    mutex.release()  # 解除锁'''
                else:
                    encodeError.append(linestr)
                if len(values) > oldLen: encodeSuccess.append(values)
            except StopIteration:
                mLog.info('analysis complete:%s' % fileIndex)
                raise
            except Exception as e:
                encodeError.append(linestr)
                # mLog.warning('analysis error:' + str(e))
                '''if mutex.locked():
                    mutex.release()  # 解除锁'''
                # traceback.print_exc()
                continue
    except StopIteration:
        pass


def encodeProcess(ags):
    # filePath:数据源路径
    # addrField:地址列标题名
    # threadNum：线程数量
    # geocodeOutput：地理编码结果输出路径

    fileIndex = str(ags[0])
    filePath = ags[1]
    fields = ags[2]
    addrField = ags[3]
    fileEncode = ags[4]
    threadNum = ags[5]
    geocodeOutput = ags[6]
    geocodeUrl = ags[7]
    logOutputPath = ags[8]

    # encodeLog = Logger("geocodeLog_" + str(os.getpid()), os.path.join(geocodeOutput, "processLog"))
    logId = 'geocode_%s' % str(os.getpid())
    geocodeLog = logging.getLogger(logId)
    if len(geocodeLog.handlers) == 0:
        geocodeLog = Logger(
            logname=os.path.join(logOutputPath, "geocode_process_%s.log" % os.getpid()),
            loglevel=1, callfile=logId).get_logger()  # 感谢匿名网友指正
    encodeResultPath = os.path.join(geocodeOutput, "geocodeResult")
    if os.path.exists(encodeResultPath) is not True:
        os.mkdir(encodeResultPath)
    try:
        setdefaulttimeout(5)  # 设置超时时间
        try:
            sourceFileObj = file(filePath, 'rb')
            encodeSuccess = []
            encodeError = []
            threads = []

            geocodeLog.info("analysis file: %s" % fileIndex)
            for i in xrange(threadNum):
                th = threading.Thread(target=encodeThread,
                                      args=(fileIndex, sourceFileObj, fields, addrField, fileEncode, geocodeUrl,
                                            encodeSuccess,
                                            encodeError, geocodeLog))
                th.start()
                threads.append(th)
            for process in threads:
                process.join()
            sourceFileObj.close()
            fileName = os.path.basename(filePath).split('.')[0]
            errorFile = file(os.path.join(encodeResultPath, fileName + '_error'), 'wb')
            for i in encodeError:
                errorFile.write(i)
            errorFile.close()

            xyFile = os.path.join(encodeResultPath, fileName + '_success')
            successFile = file(xyFile, 'wb')
            for i in encodeSuccess:
                successFile.write(FIELD_DEL.join(i) + FIELD_DEL + '\n')
            geocodeLog.info("file done: %s" % filePath)
            successFile.close()
        except Exception as e:
            geocodeLog.warning(traceback.format_exc())
            geocodeLog.info("file analysis error: %s--%s" % (filePath, str(e)))
            return ""

    except Exception as e:
        geocodeLog.warning(e.message)
        geocodeLog.warning(traceback.format_exc())
        return ""


def clearFile(path):
    for fl in os.listdir(path):
        targetFile = os.path.join(path, fl)
        if os.path.isfile(targetFile):
            os.remove(targetFile)


def getCSVFileList(cutFilePath, fields, addrField, fileEncode, threadNum, geocodeOutput, geocodeUrl, logOutputPath):
    processFiles = []

    for fl in os.listdir(cutFilePath):
        targetFile = os.path.join(cutFilePath, fl)
        if os.path.isfile(targetFile):
            index = fl.replace('addr', '')
            index = index.replace('.csv', '')
            processFiles.append(
                [index, targetFile, fields, addrField, fileEncode, threadNum, geocodeOutput, geocodeUrl, logOutputPath])
    return processFiles


def run(cutFilePath, fields, addrField, fileEncode, processNum, threadNum, geocodeOutput, geocodeUrl, logOutputPath):
    try:

        processFiles = getCSVFileList(cutFilePath, fields, addrField, fileEncode, threadNum, geocodeOutput,
                                      geocodeUrl, logOutputPath)  # 获取需要分析的文件
        if len(processFiles):
            if processNum > len(processFiles): processNum = len(processFiles)
            pool = Pool(processes=processNum)
            for i in xrange(0, len(processFiles)):
                pool.apply_async(encodeProcess, (processFiles[i],))
            pool.close()
            pool.join()

    except Exception as e:
        logger.warning(e.message)
        traceback.print_exc()
        pass


if __name__ == '__main__':
    run(r'C:\Users\LocalUser\Desktop\address-GBK.txt', ['ADDRESS'], 0, 1, 1,
        r"C:\Users\LocalUser\Desktop", 'http://99.12.95.183:8080/GT/rest/services/singleservice/single?')
