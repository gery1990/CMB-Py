# encoding=utf8
import os, sys, logging
import shutil
import time
import traceback
from  multiprocessing import Pool

import configModule as cm
from common.logModel import Logger
from common.cutCSV import CutFile
from common.ParamikoHandle import SSHControl


def mapLargeProcess(ags):
    mapLog = logging.getLogger(str(os.getpid()))
    logOutputPath = ags[12]
    if len(mapLog.handlers) == 0:
        mapLog = Logger(
            logname=os.path.join(logOutputPath, "largeGDB_process_%s.log" % os.getpid()),
            loglevel=3, callfile=str(os.getpid())).get_logger()
    try:
        sourceFilePath = ags[0]
        outputGDBPath = ags[1]
        serverName = ags[2]
        templatePath = ags[3]
        x = ags[4]
        y = ags[5]
        sourceFileFields = ags[6]
        fileEncode = ags[7]
        convertSr = ags[8]
        serverInfo = ags[9]
        pythonPath = ags[10]
        toolsPyPath = ags[11]
        mapLog.info('process pid(%s) running...' % str(os.getpid()))
        sshConn = SSHControl(None, None, mapLog, serverInfo=serverInfo)
        status = sshConn.ssh_exec_cmd('%s %s %s %s %s %s %s %s %s %s %s %s' % (
            pythonPath, toolsPyPath, sourceFilePath, outputGDBPath, serverName, templatePath, x, y, sourceFileFields,
            str(os.getpid()), fileEncode, convertSr))
        sshConn.ssh_close()
        mapLog.info('proces pid(%s) complate ')
        return status
    except:
        mapLog.warning(traceback.format_exc())
        raise


class ConvertGDB:
    def __init__(self, sourceFile, fileInfo, pythonPath, pyWorkspace, sshClient, logOutPath, logger):
        # balanceStatus:负载均衡状态文件
        # souceFile：带经纬度坐标信息的文件路径
        # fileInfo：文件类型的配置信息
        # pythonPath:arcgis server 自带的python工具路径：/home/arcgis/arcgis/server/tools/pythoon
        # pyWorkspace:python远程启动的脚本目录
        self.sourceFile = sourceFile
        self.fileInfo = fileInfo
        self.logger = logger
        self.pythonPath = pythonPath
        self.pyWorkspace = pyWorkspace

        self.timeStr = time.strftime("%Y%m%d", time.localtime())
        self.sshConn = sshClient  # 初始化远程连接
        '''初始化输出路径'''
        # 临时gdb存储路径，如果同一日期下有数据则删除
        self.outputPath = os.path.join(cm.getGDBWorkspace(), self.fileInfo["type"], self.timeStr,
                                       self.fileInfo["serverName"])
        self.logOutPath = logOutPath
        if os.path.exists(self.outputPath):
            shutil.rmtree(self.outputPath)
        self.sshConn.ssh_exec_cmd('mkdir -p %s' % self.outputPath)
        self.sshConn.ssh_exec_cmd(
            'chmod -R 777 %s' % os.path.join(cm.getGDBWorkspace(), self.fileInfo["type"], self.timeStr))

        # server服务数据源存储位置
        self.serverOutputPath = os.path.join(cm.getGISServerGDB(), fileInfo["type"], fileInfo["serverName"])
        if os.path.exists(self.serverOutputPath) is not True:
            os.makedirs(self.serverOutputPath)

    # gdb转换客户端
    def convertGDB(self):
        try:
            status = False
            if self.fileInfo["updateModel"] == "all":
                if self.fileInfo["cutGDB"] == 'true':
                    status = self.convertLargeGDB2(self.sourceFile)
                else:
                    pyName = 'InsertGDB_all.py'
                    # sourceFilePath, outputGDBPath, serverName, templatePath, x, y, sourceFileFields
                    status = self.sshConn.ssh_exec_cmd(
                        '%s %s %s %s %s %s %s %s %s %s %s' % (
                            self.pythonPath, os.path.join(self.pyWorkspace, pyName), self.sourceFile,
                            self.outputPath, self.fileInfo["serverName"], self.fileInfo["templatePath"],
                            self.fileInfo["x"], self.fileInfo["y"], ','.join(self.fileInfo["fields"]),
                            self.fileInfo["encode"], self.fileInfo["convertSr"]))
            elif self.fileInfo["updateModel"] == "update" or self.fileInfo["updateModel"] == "add":
                if self.fileInfo["cutGDB"] == 'true':
                    status = self.sshConn.ssh_exec_cmd(
                        '%s %s %s %s %s %s %s %s %s %s %s %s %s' % (
                            self.pythonPath, os.path.join(self.pyWorkspace, 'InsertGDB_update_multi.py'), self.sourceFile,
                            self.outputPath, self.serverOutputPath, self.fileInfo["serverName"],
                            self.fileInfo["x"], self.fileInfo["y"], self.fileInfo["uniqueId"],
                            self.fileInfo["updateModel"],
                            ','.join(self.fileInfo["fields"]), self.fileInfo["encode"], self.fileInfo["convertSr"]))
                else:
                    status = self.sshConn.ssh_exec_cmd(
                        '%s %s %s %s %s %s %s %s %s %s %s %s %s %s' % (
                            self.pythonPath, os.path.join(self.pyWorkspace, 'InsertGDB_update.py'), self.sourceFile,
                            self.outputPath, self.serverOutputPath, self.fileInfo["serverName"],
                            self.fileInfo["templatePath"],
                            self.fileInfo["x"], self.fileInfo["y"], self.fileInfo["uniqueId"],
                            self.fileInfo["updateModel"],
                            ','.join(self.fileInfo["fields"]), self.fileInfo["encode"], self.fileInfo["convertSr"]))
            return status
        except:
            self.logger.warning(traceback.format_exc())
            return False

    # gdb转换客户端
    def MSHandler_update(self):
        try:
            arcgisserver = cm.getArcGISServer()
            # type, serverName, gdbPath, serverGDBPath, ip, port, user, password
            status = self.sshConn.ssh_exec_cmd("%s %s %s %s %s %s %s %s %s %s" % (
                self.pythonPath, os.path.join(self.pyWorkspace, 'MsHandler_update.py'), self.fileInfo["type"],
                self.fileInfo["serverName"], self.outputPath, self.serverOutputPath, arcgisserver["ip"],
                arcgisserver["port"], arcgisserver["user"], arcgisserver["password"]))
            return status
        except:
            self.logger.warning(traceback.format_exc())
            return False

    # 根据GDB数据重新发布服务
    def MsHandler_publish(self):
        try:
            arcgisserver = cm.getArcGISServer()
            # type, serverName, gdbPath, serverGDBPath, ip, port, user, password
            status = self.sshConn.ssh_exec_cmd("%s %s %s %s %s %s %s %s %s %s" % (
                self.pythonPath, os.path.join(self.pyWorkspace, 'MsHandler_publish.py'), self.fileInfo["type"],
                self.fileInfo["serverName"], self.outputPath, self.serverOutputPath, arcgisserver["ip"],
                arcgisserver["port"], arcgisserver["user"], arcgisserver["password"]))
            return status
        except:
            self.logger.warning(traceback.format_exc())
            return False

    def close_ssh(self):
        self.sshConn.ssh_close()

    def calcRowCount(self, sourcefile):
        fileObj = file(sourcefile, 'rU')
        count = 0
        try:
            while True:
                fileObj.next()
                count += 1
        except StopIteration:
            fileObj.close()
            pass
        return count
        # len(file(sourcefile, 'rU').readlines())

    def convertLargeGDB(self, sourceFile):
        try:
            rowCount = self.calcRowCount(sourceFile)
            maxGDBRecodeCount = cm.getMaxGDBRecordCount()
            self.logger.info('convertGDBHandler ** file row count:%d' % rowCount)
            pCount = self.fileInfo["processNum"]

            if (rowCount / maxGDBRecodeCount) != 0 and (rowCount / maxGDBRecodeCount) < pCount:
                pCount = rowCount / maxGDBRecodeCount
            if rowCount < maxGDBRecodeCount:
                pCount = 1
            # 创建切割文件存放路径
            cutFileOutputWorkspace = os.path.join(self.outputPath, "cutFile")
            if os.path.exists(cutFileOutputWorkspace):
                shutil.rmtree(cutFileOutputWorkspace)
            os.makedirs(cutFileOutputWorkspace)

            cutF = CutFile(pCount, rCount=rowCount, maxCount=maxGDBRecodeCount)
            cutCount = cutF.do(sourceFile, cutFileOutputWorkspace)

            parameterList = []
            gdbPathList = []
            for i in xrange(1, pCount + 1):
                gdbPath = os.path.join(self.outputPath, "%s%d.gdb" % (self.fileInfo["serverName"], i))
                gdbPathList.append(gdbPath)
                fp = os.path.join(cutFileOutputWorkspace, 'process%d') % i
                os.makedirs(fp)
                parameterList.append(
                    [fp, gdbPath, self.fileInfo["serverName"], self.fileInfo["templatePath"],
                     self.fileInfo["x"], self.fileInfo["y"], ','.join(self.fileInfo["fields"]), self.fileInfo["encode"],
                     self.sshConn.serverInfo, self.pythonPath, os.path.join(self.pyWorkspace, "InsertGDB_large.py"),
                     self.logOutPath])

            gdbIndex = 1
            for f in xrange(1, cutCount + 1):
                fileName = os.path.basename(sourceFile).split('.')[0] + "_%d" % f
                cutFile = os.path.join(cutFileOutputWorkspace, fileName)
                if os.path.exists(cutFile):
                    fp = os.path.join(cutFileOutputWorkspace, 'process%d') % gdbIndex
                    shutil.move(cutFile, os.path.join(fp, os.path.basename(cutFile)))
                gdbIndex += 1
                if gdbIndex > pCount:
                    gdbIndex = 1

            self.logger.info('convertGDBHandler ** create process...')
            pool = Pool(processes=pCount)
            for i in xrange(0, len(parameterList)):
                pool.apply_async(mapLargeProcess, args=(parameterList[i],))
            pool.close()
            pool.join()
            self.logger.info('convertGDBHandler ** process convert done')
            self.logger.info('convertGDBHandler ** start merge extent')
            self.sshConn.ssh_exec_cmd('%s %s %s' % (self.pythonPath,
                                                    os.path.join(self.pyWorkspace, "InsertGDB_large_mergeExtent.py"),
                                                    ','.join(gdbPathList)))
            self.logger.info('convertGDBHandler ** start merge extent done')
            return True
        except:
            self.logger.warning(traceback.format_exc())
            return False

    def convertLargeGDB2(self, sourceFile):
        '''
        固定图层数量，不重新发布服务
        :param sourceFile:
        :return:
        '''
        try:
            rowCount = self.calcRowCount(sourceFile)
            # 默认已划分好的图层数量为100,gdb数量为4，如修改了图层数量要对应修改,图层需要按照顺序存储在不同gdb
            pCount = self.fileInfo["processNum"]
            layerCount = self.fileInfo["layersNum"]

            maxGDBRecodeCount = cm.getMaxGDBRecordCount()

            if rowCount > maxGDBRecodeCount and (rowCount / layerCount) > maxGDBRecodeCount:
                maxGDBRecodeCount = rowCount / layerCount

            self.logger.info('convertGDBHandler ** file row count:%d' % rowCount)

            # 创建切割文件存放路径
            cutFileOutputWorkspace = os.path.join(self.outputPath, "cutFile")
            if os.path.exists(cutFileOutputWorkspace):
                shutil.rmtree(cutFileOutputWorkspace)
            os.makedirs(cutFileOutputWorkspace)

            pc = (rowCount / maxGDBRecodeCount) + 1
            cutF = CutFile(pc, rCount=rowCount, maxCount=maxGDBRecodeCount * 1.5)
            cutCount = cutF.do(sourceFile, cutFileOutputWorkspace)

            parameterList = []
            gdbPathList = []
            for i in xrange(1, pCount + 1):
                gdbPath = os.path.join(self.outputPath, "%s%d.gdb" % (self.fileInfo["serverName"], i))
                gdbPathList.append(gdbPath)
                fp = os.path.join(cutFileOutputWorkspace, 'process%d') % i
                os.mkdir(fp)
                parameterList.append(
                    [fp, gdbPath, self.fileInfo["serverName"], self.fileInfo["templatePath"],
                     self.fileInfo["x"], self.fileInfo["y"], ','.join(self.fileInfo["fields"]), self.fileInfo["encode"],
                     self.fileInfo["convertSr"],
                     self.sshConn.serverInfo, self.pythonPath, os.path.join(self.pyWorkspace, "InsertGDB_large2.py")])
                # 拷贝模板gdb
                self.sshConn.ssh_exec_cmd('cp -r %s %s' % (
                    os.path.join(self.fileInfo["templatePath"], "%s%d.gdb" % (self.fileInfo["serverName"], i)),
                    self.outputPath))

            gdbIndex = 1
            for f in xrange(1, cutCount + 1):
                fileName = os.path.basename(sourceFile).split('.')[0] + "_%d" % f
                cutFile = os.path.join(cutFileOutputWorkspace, fileName)
                if os.path.exists(cutFile):
                    fp = os.path.join(cutFileOutputWorkspace, 'process%d') % gdbIndex
                    shutil.move(cutFile, os.path.join(fp, os.path.basename(cutFile)))
                gdbIndex += 1
                if gdbIndex > pCount:
                    gdbIndex = 1

            self.logger.info('convertGDBHandler ** create process...')
            pool = Pool(processes=pCount)
            for i in xrange(0, len(parameterList)):
                pool.apply_async(mapLargeProcess, args=(parameterList[i],))
            pool.close()
            pool.join()
            self.logger.info('convertGDBHandler ** process convert done')
            self.logger.info('convertGDBHandler ** start merge extent')
            status = self.sshConn.ssh_exec_cmd('%s %s %s' % (self.pythonPath,
                                                             os.path.join(self.pyWorkspace,
                                                                          "InsertGDB_large_mergeExtent.py"),
                                                             ','.join(gdbPathList)))
            self.logger.info('convertGDBHandler ** merge extent %s' % str(status))
            return status
        except:
            self.logger.warning(traceback.format_exc())
            return False
