# coding:utf-8
# 更新GDB数据源处理程序,重启地图服务，停止－更换－重启

import os
import shutil
import sys
import time
import traceback

from  arcpyControl.mapServerOp import MapServerOp

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


def mapOp(ip, port, user, password, id, opType):
    try:
        logger.info("update ** %s MapServer:%s" % (opType, id))
        mServerOp = MapServerOp(ip, port, user, password)  # "ip:port"-ArcGISServer的地址端口，user/password-ArcGISServer用户的信息
        statResult = mServerOp.serverOp(opType, id)
        if (statResult == "error" or statResult == "faild"):
            logger.warning('update ** %s MapServer failed:%s' % (opType, id))
            return False
        return True
    except:
        logger.warning(traceback.format_exc())
        return False


def UpdateMS(type, serverName, gdbPath, serverGDBPath, ip, port, user, password):
    '''
    更新数据源
    :param type:
    :param serverName:
    :param gdbPath: 临时存储gdb的路径：/GISData/gdb/crm/customer
    :param serverGDBPath: 地图服务gdb的路径：/GISData/arcgisserver/crm/customer
    :param ip:
    :param port:
    :param user:
    :param password:
    :return:
    '''
    try:
        if os.path.exists(gdbPath) is not True:
            return False
        # 停止地图服务
        if mapOp(ip, port, user, password, type + "/" + serverName, 'stop'):
            try:
                serverGDBWorkspace = os.path.split(serverGDBPath)[0]
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
                logger.info('update ** complate Data update!')
            except:
                logger.warning(traceback.format_exc())
                mapOp(ip, port, user, password, type + "/" + serverName, 'start')
                return False
        # 启动地图服务
        return mapOp(ip, port, user, password, type + "/" + serverName, 'start')
    except Exception as e:
        mapOp(ip, port, user, password, type + "/" + serverName, 'start')
        logger.warning(traceback.format_exc())
        return False


if __name__ == '__main__':

    # sourceFilePath = r'C:\Users\esri\Desktop\customer100005'
    # outputGDBPath = r'C:\Users\esri\Desktop\output'
    # serverName = 'customer'
    # templatePath = r'D:\dataHandle\template.gdb\client'
    # x = 2
    # y = 1
    # sourceFileFields = "UID, T1, X, Y"

    if sys.argv[1].upper()=='NONE':
        type=""
    else:
        type=sys.argv[1]

    serverName = sys.argv[2]  # 数据服务名
    gdbPath = sys.argv[3]  # 转换GDB输出路径
    serverGDBPath = sys.argv[4]  # 图层模板路径
    ip = sys.argv[5]
    port = sys.argv[6]
    user = sys.argv[7]
    password = sys.argv[8]

    logger = Logger(
        logname=os.path.join(logOutputPath, "updateMs_main.log"), loglevel=3,
        callfile=__file__).get_logger()

    logger.info('''start update Data & restart MapServer...
                   MapServer Name:%s/%s
                   sourceGDB Path:%s
                   serverGDB Path:%s''' % (type, serverName, gdbPath, serverGDBPath))

    status = UpdateMS(type, serverName, gdbPath, serverGDBPath, ip, port, user, password)
    if status:
        logger.info('update ** update Data success!')
        print "sshsuccess"
    else:
        logger.info('update ** update Data error!')
        print "ssherror"
