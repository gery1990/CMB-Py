# encoding=utf8
import json, time

import httplib, urllib, traceback

# 地图服务监控模块
monitor_ip = "99.12.95.184"  # 资源监控服务器ip
monitor_port = "8080"  # 资源监控服务器port

arcgisServer_filename = "MsStatus"  # 监控系统存储地图服务状态的文件名
arcgisServer_ip = "99.12.95.182"  # arcgis server服务器IP
arcgisServer_port = "6080"  # arcgis server服务器访问端口
arcgisServer_admin = "arcgis"  # arcgis server 管理员用户名
arcgisServer_user = "arcgis"  # arcgis server 管理员用户密码
arcgisServer_reportUrl = "/arcgis/admin/services/report?f=json"  # 获取arcgis server地图服务状态的rest接口
arcgisServer_tokenUrl = "/arcgis/tokens/generateToken?f=pjson"  # 获取arcgis server用户临时token的rest接口

# 发送地图服务状态到监控系统
def sendMapServerData(mapServerStatus):
    msName = ""
    msStatus = ""
    if mapServerStatus != "":
        for item in mapServerStatus:
            msName += item["serviceName"] + ","
            msStatus += item["status"]["realTimeState"] + ","
        msName = msName[0:(len(msName) - 1)]
        msStatus = msStatus[0:(len(msStatus) - 1)]
    msName = 'time,' + msName
    msStatus = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+ ','+ msStatus
    data = urllib.urlencode(
        {"mssid": arcgisServer_filename, "title": msName, "value": msStatus})
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    conn = httplib.HTTPConnection(monitor_ip, monitor_port)
    # 开始进行数据提交   同时也可以使用get进行
    conn.request("POST", "/CMB-Monitor/updateMsStatus", data, headers)
    # 返回处理后的数据
    response = conn.getresponse()
    print response.read()


# 获取ArcgisServer的访问token
def getArcGISServerToken():
    data = urllib.urlencode(
        {"username": arcgisServer_admin, "password": arcgisServer_user, "client": "requestip", "expiration": 1})

    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    conn = httplib.HTTPConnection(arcgisServer_ip, arcgisServer_port)
    # 开始进行数据提交   同时也可以使用get进行
    conn.request("POST", arcgisServer_tokenUrl, data, headers)
    # 返回处理后的数据
    response = conn.getresponse()
    decodejson = json.loads(response.read())
    return decodejson["token"]


# 获取地图服务状态信息
def getMapServerStatus():
    try:
        token = getArcGISServerToken()
        data = urllib.urlencode(
            {"parameters": "[instances,status,description,iteminfo]", "token": token})

        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        conn = httplib.HTTPConnection(arcgisServer_ip, arcgisServer_port)
        # 开始进行数据提交   同时也可以使用get进行
        conn.request("POST", arcgisServer_reportUrl, data, headers)
        # 返回处理后的数据
        response = conn.getresponse()
        decodejson = json.loads(response.read())
        return decodejson["reports"]
    except:
        return ""


if __name__ == '__main__':
    try:
        mapServerStatus = getMapServerStatus()
        sendMapServerData(mapServerStatus)
    except:
        traceback.print_exc()
