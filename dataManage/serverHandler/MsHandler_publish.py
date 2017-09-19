# coding:utf-8
# 转换大文件GDB，按自定义最大记录数平均划分图层

import os
import shutil
import sys
import time
import traceback

from arcpyControl.publishMapServer import PublishServer
from arcpyControl import arcpyOp as ac
from  arcpyControl.mapServerOp import MapServerOp

runWorkspace = os.path.split(sys.path[0])[0]
sys.path.append(runWorkspace)

from common.logModel import Logger

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

timeStr = time.strftime("%Y%m%d", time.localtime())

# 初始化日志模块
logger = None
logOutputPath = os.path.join(runWorkspace, 'workspace', 'logs', timeStr)
if os.path.exists(os.path.join(runWorkspace, 'workspace', 'logs', timeStr)) is not True:
    os.mkdir(os.path.join(runWorkspace, 'workspace', 'logs', timeStr))


def mapOp(ip, port, user, password, id, opType):
    try:
        logger.info("publish ** %s MapServer:%s" % (opType, id))
        mServerOp = MapServerOp(ip, port, user, password)  # "ip:port"-ArcGISServer的地址端口，user/password-ArcGISServer用户的信息
        statResult = mServerOp.serverOp(opType, id)
        if (statResult == "error" or statResult == "faild"):
            logger.warning('publish ** %s MapServer failed:%s' % (opType, id))
            return False
        return True
    except:
        logger.warning(traceback.format_exc())
        return False


def sortLayers(layerArry):
    extentPath = ""
    layersPath = {}
    sortList = [''] * len(layerArry)
    for layerPath in layerArry:
        layerName = os.path.split(layerPath)[1]
        if layerName == "extent":
            extentPath = layerPath
        else:
            layersPath[layerName] = layerPath
    sortList[0] = extentPath

    fcList = ac.getLayerFieldValues(extentPath, ("INDEX", "LAYERNAME"))
    for fc in fcList:
        index = int(fc["INDEX"])
        layerName = fc["LAYERNAME"]
        sortList[index] = layersPath[layerName]
    return sortList


def publishServer(type, serverName, gdbPath, serverWorkspace, ip, port, user, password):
    try:
        if os.path.exists(gdbPath) is not True:
            return False
        # 停止地图服务
        if mapOp(ip, port, user, password, type + "/" + serverName, 'stop'):
            try:
                serverGDBWorkspace = os.path.split(serverWorkspace)[0]
                serverGDBPath_bak_output = os.path.join(serverGDBWorkspace, timeStr)
                serverGDBPath_bak = os.path.join(serverGDBWorkspace, timeStr, serverName)

                if os.path.exists(serverGDBPath_bak_output) is not True:
                    os.makedirs(serverGDBPath_bak_output)
                if os.path.exists(serverGDBPath_bak):
                    shutil.rmtree(serverGDBPath_bak)

                # 备份原有的GDB文件，只保留7天数据,先备份-删除-拷贝新数据
                shutil.move(serverGDBPath, serverGDBPath_bak)
                shutil.copytree(gdbPath, serverGDBPath)
                if os.path.exists(os.path.join(serverGDBPath, 'cutFile')):
                    shutil.rmtree(os.path.join(serverGDBPath, 'cutFile'))

                layers = ac.listFolderGDBLayers(serverGDBPath, logger)

                logger.info('publish ** %s has %d layers' % (serverGDBPath, len(layers)))
                sortLs = sortLayers(layers)
                pServer = PublishServer(os.path.join(runWorkspace, 'workspace', 'template.mxd'),
                                        os.path.join(runWorkspace, 'workspace', 'connectTo99.12.100.ags'), type, logger)
                mxdPath = pServer.buildMxd(os.path.join(serverGDBPath, serverName + ".mxd"), sortLs)
                pServer.publishMXD(mxdPath, definitionType="esriServiceDefinitionType_Replacement",
                                   maxRecordCount=300000, maxInstances=8)
                logger.info('publish ** complate pulish MapServer:%s' % serverName)
                return True
            except:
                raise
    except Exception as e:
        mapOp(ip, port, user, password, type + "/" + serverName, 'start')
        logger.warning(traceback.format_exc())
        return False


if __name__ == '__main__':

    type = sys.argv[1]  # 业务类型
    serverName = sys.argv[2]  # 数据服务名
    gdbPath = sys.argv[3]  # 转换GDB输出路径
    serverGDBPath = sys.argv[4]
    ip = sys.argv[5]
    port = sys.argv[6]
    user = sys.argv[7]
    password = sys.argv[8]

    logger = Logger(
        logname=os.path.join(logOutputPath, "publishMs_main.log"), loglevel=3,
        callfile=__file__).get_logger()

    logger.info('''Publish MapServer...
                    MapServer Name:%s/%s
                    sourceGDB Path:%s
                    serverGDB Path:%s''' % (type, serverName, gdbPath, serverGDBPath))

    status = publishServer(type, serverName, gdbPath, serverGDBPath, ip, port, user, password)
    if status:
        logger.info('publish **  success!')
        print "sshsuccess"
    else:
        logger.info('publish **  error!')
        print "ssherror"


        # layers = ac.listFolderGDBLayers(r'D:\5000W')
        # sortLs = sortLayers(layers)
        #
        # publishS = PublishServer(r'C:\Users\esri\Desktop\template.mxd',
        #                          r'C:\Users\esri\Desktop\arcgis on 192.168.119.134_6080 (admin).ags')
        # mxdPath = publishS.buildMxd(r'C:\Users\esri\Desktop\test.mxd', sortLs)
        # publishS.publishMXD(mxdPath, definitionType="esriServiceDefinitionType_Replacement", maxInstances=20,
        #                     maxRecordCount=400000)
