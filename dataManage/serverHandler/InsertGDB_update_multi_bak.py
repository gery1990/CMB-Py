# coding:utf-8
# 增量更新GDB处理程序-分表处理方案

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
FIELD_DEL = chr(0X7C) + chr(0X1C)  # 标准文件字段分隔符


def is_num_by_except(num):
    try:
        float(num)
        return True
    except ValueError:
        return False

def ConvertGDB(sourceFilePath, outputGDBPath, serverGDBPath, serverName, x, y, uniqueId,
               updateModel, sourceFileFields, fileEncode, convertSr):
    try:
        logger.info('insertUpdate ** update GDB soucefile:%s' % sourceFilePath)
        # 临时gdb目录
        gdbPath = os.path.join(outputGDBPath, serverName + ".gdb")
        gdbServerPath = os.path.join(serverGDBPath, serverName + ".gdb")
        # 拷贝服务使用的gdb数据
        if os.path.exists(gdbPath):
            shutil.rmtree(gdbPath)
        # 将旧的GDB拷贝到临时gdb路径
        shutil.copytree(gdbServerPath, gdbPath)

        if updateModel == "update":
            ac.setWorkSpace(gdbPath)
            for sn in ac.getFeaturesList():
                if sn == "extent":
                    continue
                layerPath = os.path.join(gdbPath, sn)
                # 删除旧的要素，重新插入新的纪录
                ac.deleteRow(layerPath, sourceFilePath, uniqueId, sourceFileFields[uniqueId])

        # 字典图层路径，循环图层要素，将新增的数据按范围插入到对应的图层
        extentLayerPath = os.path.join(gdbPath, "extent")
        extentCursor = ac.searchData(extentLayerPath, ["name", "SHAPE@"])
        try:
            inSr, outSr = None, None
            if convertSr.upper() != "NONE":
                v = convertSr.split(',')
                inSr = ac.getSpatialReference(int(v[0]))
                outSr = ac.getSpatialReference(int(v[1]))

            for extentRow in extentCursor:
                polyGeo = extentRow[1]
                name = extentRow[0]
                sourceObj = file(sourceFilePath, 'rb')
                fileObj = file(os.path.join(outputGDBPath, name), 'wb')
                rowCount = 0
                try:
                    while True:
                        lineStr = sourceObj.next()
                        values = lineStr.split(FIELD_DEL)
                        if str(values[x]) != '' and str(values[y]) != "" and str(values[x]) != "0" and str(
                                values[y]) != "0" and is_num_by_except(values[x]) and is_num_by_except(values[y]):
                            geo = ac.createPoint(float(values[x]), float(values[y]))

                            if convertSr.upper() != 'NONE':
                                p = ac.createPointGeometry(geo, inSr)
                                geo = p.projectAs(outSr)
                            # 输出到文件
                            if polyGeo.contains(geo):
                                rowCount += 1
                                fileObj.write(lineStr)
                except StopIteration:
                    fileObj.close()
                    sourceObj.close()
                    pass
                if rowCount>0:
                    ac.insertRow_Point(os.path.join(gdbPath, name), os.path.join(outputGDBPath, name), x, y,
                                       sourceFileFields, fileEncode, convertSr, logger)
                    logger.info('insert %d row to "%s" layer!' % (rowCount, name))
                os.remove(os.path.join(outputGDBPath, name))
        except:
            pass
        return True
    except Exception as e:
        logger.warning(e.message)
        logger.warning(traceback.format_exc())
        return False


if __name__ == '__main__':
    logger = Logger(
        logname=os.path.join(logOutputPath, "updateGDBMulti_main.log"), loglevel=3,
        callfile=__file__).get_logger()
    try:
        # sourceFilePath = r'C:\Users\esri\Desktop\customerupdate'  # 带经纬度坐标信息文件
        # outputGDBPath = r'D:\dataHandle\GISData\gdb\crm\clientdata'  # 转换GDB输出路径
        # serverGDBPath = r'D:\dataHandle\GISData\arcgisserver\gdb\crm\clientdata'
        # serverName = r'clientdata'  # 数据服务名
        # x = 10  # 经度序号
        # y = 11  # 纬度序号
        # uniqueId = 0
        # updateModel = 'add'  # 更新模式：add\update
        # sourceFileFields = "CID,CITY,CNAME,CDATE,CADDRESS,CCAPITAL,CARTIFICIA,CTELEPHONE,CPHONE,CBUSITYPE,X,Y,BZ1,BZ2".split(
        #     ',')  # 数据源文件表头
        # fileEncode = 'utf-8'
        # convertSr = 'NONE'

        sourceFilePath = sys.argv[1]  # 带经纬度坐标信息文件
        outputGDBPath = sys.argv[2]  # 转换GDB输出路径
        serverGDBPath = sys.argv[3]
        serverName = sys.argv[4]  # 数据服务名
        x = int(sys.argv[5])  # 经度序号
        y = int(sys.argv[6])  # 纬度序号
        uniqueId = int(sys.argv[7])
        updateModel = sys.argv[8]  # 更新模式：add\update
        sourceFileFields = sys.argv[9].split(',')  # 数据源文件表头
        fileEncode = sys.argv[10]
        convertSr = sys.argv[11]

        logger.info('''start convert GDB...
                       source File Path:%s
                       output Path:%s
                       log Path:%s''' % (
            sourceFilePath, outputGDBPath, os.path.join(runWorkspace, 'workspace', "logs")))

        status = ConvertGDB(sourceFilePath, outputGDBPath, serverGDBPath, serverName, x, y, uniqueId,
                            updateModel, sourceFileFields, fileEncode, convertSr)
        if status:
            logger.info('insertUpdate ** convert success!')
            print "sshsuccess"
        else:
            logger.info('insertUpdate ** convert error!')
            print "ssherror"

    except:
        print "ssherror"
