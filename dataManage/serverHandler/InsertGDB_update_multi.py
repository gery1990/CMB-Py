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


def buildSQLStr(sourceFilePath, layerPath, uniqueId):
    sourceObj = file(sourceFilePath, 'rb')
    sqlStrs = []
    sql = ac.addFieldDelimiters(layerPath, "CID")
    count = 0
    sqlStr = ""
    try:
        while True:
            lineStr = sourceObj.next()
            values = lineStr.split(FIELD_DEL)
            count += 1
            if count > 0 and count % 30000 == 0:
                sqlStr = sqlStr[0:(len(sqlStr) - 1)]
                sqlStrs.append(sql + " in (%s)" % sqlStr)
                sqlStr = ""
            id = values[uniqueId]
            sqlStr += "'%s'," % id
    except StopIteration:
        sourceObj.close()
        pass
    sqlStr = sqlStr[0:(len(sqlStr) - 1)]
    sqlStrs.append(sql + " in (%s)" % sqlStr)
    return sqlStrs


def deleteFeaturesBySelected(layerPath, layerName, ids):
    tmpName = layerName + "tmp"
    ac.makeFeatureLayer(os.path.join(layerPath, layerName), tmpName)
    sqlStr = ac.addFieldDelimiters(os.path.join(layerPath, layerName), "CID") + " in ("
    for id in ids:
        sqlStr += "'%s'," % id
    sqlStr = sqlStr[0:(len(sqlStr) - 1)] + ")"
    ac.selectLayerByAttribute(tmpName, "NEW_SELECTION", sqlStr)
    ac.deleteFeatures(tmpName)


def ConvertGDB(sourceFilePath, outputGDBPath, serverGDBPath, serverName, x, y, uniqueId,
               updateModel, sourceFileFields, fileEncode, convertSr):
    try:
        logger.info('Step 1: insertUpdate ** update GDB soucefile:%s' % sourceFilePath)

        gdbPath = os.path.join(outputGDBPath, serverName + ".gdb")
        gdbServerPath = os.path.join(serverGDBPath, serverName + ".gdb")
        gdbPath_temp = os.path.join(outputGDBPath, "tempdata.gdb")

        tempSpatialJoinPath = os.path.join(gdbPath_temp, "spatialjoin")
        tempLayerPath = os.path.join(gdbPath_temp, "tempdata")
        tableViewPath = os.path.join(gdbPath, "tableview")

        # 拷贝服务使用的gdb数据
        if os.path.exists(gdbPath):
            shutil.rmtree(gdbPath)
        if os.path.exists(gdbPath_temp):
            shutil.rmtree(gdbPath_temp)
        # 将旧的GDB拷贝到临时gdb路径
        shutil.copytree(gdbServerPath, gdbPath)

        #更新模式，先删除gdb、tableview里旧的记录
        if updateModel == "update":
            logger.info('Step 2: update gdb...')
            tableTemp = serverName + "tabletemp"
            # 获取需要更新的记录具体图层位置并删除
            ac.makeTableView(tableViewPath, tableTemp)
            sqlStrs = buildSQLStr(sourceFilePath, tableViewPath, uniqueId)
            rowGroup = {}
            for sqlStr in sqlStrs:
                ac.selectLayerByAttribute(tableTemp, "NEW_SELECTION", sqlStr)
                searchCursor = ac.searchData(tableTemp, ("CID", "NAME"))
                for row in searchCursor:
                    if rowGroup.has_key(row[1]) is not True:
                        rowGroup[row[1]] = []
                    rowGroup[row[1]].append(row[0])
                del searchCursor
                ac.deleteRows(tableTemp)
            logger.info('Step 3: update gdb layer include : %s' % '|'.join(rowGroup.keys()))
            for key in rowGroup.keys():
                deleteFeaturesBySelected(gdbPath, key, rowGroup[key])
            logger.info('Step 4: update gdb complate')

        # 构建临时存储gdb
        logger.info('Step 5: build temp gdb and insert row...')
        ac.createFileGDB(outputGDBPath, "tempdata.gdb")
        layerFields = ac.getLayerFields(os.path.join(gdbPath, "clientdata130"))
        fieldsMap = {}
        for layerField in layerFields:
            if layerField["name"] == "SHAPE@": continue
            fieldsMap[layerField["name"]] = layerField["type"]
        proNum = 3857
        if convertSr.upper() != "NONE" and convertSr != "":
            proNum = int(convertSr.split(",")[1])
        ac.createLayer(gdbPath_temp, "tempdata", proNum, "POINT", fieldsMap)
        ac.insertRow_Point(tempLayerPath, sourceFilePath, x, y, sourceFileFields, fileEncode, convertSr, logger)

        logger.info('Step 6: temp gdb spatial join extent...')
        ac.spatialJoin(tempLayerPath, os.path.join(gdbPath, "extent"), tempSpatialJoinPath, "JOIN_ONE_TO_ONE",
                       "KEEP_COMMON", None, "INTERSECT", "", "")


        # 插入记录到对应的图层
        logger.info('Step 7: temp gdb append to gdb...')
        fields = []
        for layerField in layerFields:
            if layerField["name"] == "SHAPE@": continue
            fields.append(layerField["name"])

        fields.append("SHAPE@X")
        x = (len(fields) - 1)
        fields.append("SHAPE@Y")
        y = (len(fields) - 1)
        fields.append("CID")
        fields.append("name")
        tempCursor = ac.searchData(tempSpatialJoinPath, fields)
        insertTableCursor = ac.getInsertCursor(tableViewPath, ["CID", "NAME"])
        rowGroup = {}
        for row in tempCursor:
            id = row[-2]
            layerName = row[-1]
            strValue = ""
            for fieldValue in row:
                if type(fieldValue) == int or type(fieldValue) == float:
                    fieldValue = str(fieldValue)
                strValue += (fieldValue.encode('utf-8') + FIELD_DEL)
            strValue += FIELD_DEL + "\n"
            if rowGroup.has_key(layerName) is not True:
                rowGroup[layerName] = []
            rowGroup[layerName].append(strValue)
            insertTableCursor.insertRow([id, layerName])
        del tempCursor
        del insertTableCursor

        for layerName in rowGroup.keys():
            tempFilePath = os.path.join(outputGDBPath, layerName)
            tempFileObj = file(tempFilePath, 'wb')
            for lineStr in rowGroup[layerName]:
                tempFileObj.write(lineStr)
            tempFileObj.close()
            ac.insertRow_Point(os.path.join(gdbPath, layerName), tempFilePath, x, y, fields, 'utf-8',
                               "None", logger)
            os.remove(tempFilePath)

        shutil.rmtree(gdbPath_temp)

        logger.info('Step 8: insertUpdate ** update GDB soucefile:%s  complate！' % sourceFilePath)
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
        sourceFilePath = r'C:\Users\esri\Desktop\customerupdate'  # 带经纬度坐标信息文件
        outputGDBPath = r'D:\dataHandle\GISData\gdb\crm\clientdata'  # 转换GDB输出路径
        serverGDBPath = r'D:\dataHandle\GISData\arcgisserver\gdb\crm\clientdata'
        serverName = r'clientdata'  # 数据服务名
        x = 10  # 经度序号
        y = 11  # 纬度序号
        uniqueId = 0
        updateModel = 'update'  # 更新模式：add\update
        sourceFileFields = "CID,CITY,CNAME,CDATE,CADDRESS,CCAPITAL,CARTIFICIA,CTELEPHONE,CPHONE,CBUSITYPE,X,Y,BZ1,BZ2".split(
            ',')  # 数据源文件表头
        fileEncode = 'utf-8'
        convertSr = '3857,3857'

        # sourceFilePath = sys.argv[1]  # 带经纬度坐标信息文件
        # outputGDBPath = sys.argv[2]  # 转换GDB输出路径
        # serverGDBPath = sys.argv[3]
        # serverName = sys.argv[4]  # 数据服务名
        # x = int(sys.argv[5])  # 经度序号
        # y = int(sys.argv[6])  # 纬度序号
        # uniqueId = int(sys.argv[7])
        # updateModel = sys.argv[8]  # 更新模式：add\update
        # sourceFileFields = sys.argv[9].split(',')  # 数据源文件表头
        # fileEncode = sys.argv[10]
        # convertSr = sys.argv[11]

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
