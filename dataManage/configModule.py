# encoding=utf8
import sys
import os
from xml.etree import ElementTree

rootPath = sys.path[0]
# rootPath = r'D:\python_project\CMB-Py\dataHandle'
configPath = os.path.join(rootPath, "workspace/config.xml")  # 获取配置路径
configXml = ElementTree.parse(configPath)

loggerConfig_client = os.path.join(rootPath, "workspace", 'logging_client.conf')


def getLocalWorkspace():  # 本地工作空间
    return os.path.join(rootPath, "workspace")


def getConfigWorkspace():  # 本地工作空间
    return os.path.join(getLocalWorkspace(), "config")


def getLogWorkspace():  # 本地工作空间
    return os.path.join(getLocalWorkspace(), "log")


def getTemplateMxd():
    return os.path.join(getLocalWorkspace(), configXml.find("templateMxd").text)


def getWGS84proj():
    return os.path.join(getLocalWorkspace(), configXml.find("wgs84proj").text)


def getAgsServerPath():
    return os.path.join(getLocalWorkspace(), configXml.find("agsServerPath").text)


def getMaxGDBRecordCount():
    return int(configXml.find("maxGDBRecodeCount").text)


def getProcessNum():
    return int(configXml.find("processNum").text)  # 进程数量


def getThreadNum():
    return int(configXml.find("threadNum").text)  # 线程数量


def getFileTypes():
    fileTypes = {}
    for f in configXml.find("fileTypes").getchildren():
        fields = f.get("fields").split(',')
        addrField = -1
        x = -1
        y = -1
        if f.get("addrField") != '':
            addrField = int(f.get("addrField"))
        if f.get("x") != '':
            x = int(f.get("x"))
        if f.get("y") != '':
            y = int(f.get("y"))
        fileTypes[f.get("id")] = {"id": f.get("id"), "serverName": f.get("serverName"),
                                  "uniqueId": int(f.get("uniqueId")), "hasXY": f.get("hasXY"),
                                  "addrField": addrField, "x": x, "y": y, "updateModel": f.get("updateModel"),
                                  "type": f.get("type"), "cutGDB": f.get("cutGDB"),
                                  "templatePath": f.get("templatePath"), "fields": fields,
                                  "sourceFolder": f.get("sourceFolder"), 'encode': f.get("encode")}
        try:
            # 获取切割大文件的配置，pnum指按多少个进程进行处理，lnum指gdb的图层数量（多个gdb的总和）
            pnum = int(f.get("processNum"))
            lnum = int(f.get("layersNum"))
            fileTypes[f.get("id")]["processNum"] = pnum
            fileTypes[f.get("id")]["layersNum"] = lnum
        except:
            pass
        try:
            toMercator = f.get("convertSr")
            if toMercator == "":
                toMercator = 'none'
            fileTypes[f.get("id")]["convertSr"] = toMercator
        except:
            fileTypes[f.get("id")]["convertSr"] = "none"
            pass
    return fileTypes


def getServerList():
    serverList = []
    for f in configXml.find("serverList").getchildren():
        serverList.append(
            {"id": f.get("id"), "ip": f.get("ip"), "port": f.get("port"), "user": f.get("user"),
             "password": f.get("password")})
    return serverList


def getBalanceStatus():
    return configXml.find("balanceStatus").text


def getFileType(name):
    fileTypes = getFileTypes()
    return fileTypes[name]


def getGDBWorkspace():
    return configXml.find("gdbWorkspace").text


def getOriginalWorkspace():
    return configXml.find("originalWorkspace").text


def getGeocodeWorkspace():
    return configXml.find("geocodeWorkspace").text


def getDayLimit():
    return int(configXml.find("datalimit").text)


def getGISServerGDB():
    return configXml.find("gisserverGDB").text


def getArcGISServer():
    arcgisserver = {"ip": configXml.find("arcgisserver/ip").text, "port": configXml.find("arcgisserver/port").text,
                    "user": configXml.find("arcgisserver/user").text,
                    "password": configXml.find("arcgisserver/password").text}
    return arcgisserver


def getGeocode():
    geocode = {"url": configXml.find("geocode/url").text, "encode": configXml.find("geocode/encode").text,
               "wkid": configXml.find("geocode/wkid").text}
    return geocode


def getFtpMemberInfo():
    txt = configXml.find("ftppath").text
    ftpInfo = txt.split(' ')
    return {"ip": ftpInfo[0], "port": ftpInfo[1], "user": ftpInfo[2], "pws": ftpInfo[3]}


def getPythonTools():
    return configXml.find("pythonTools").text


def getServerPythonWorkspace():
    return configXml.find("serverPythonWorkspace").text


minScore = configXml.find("minscore").text
