# encoding=utf8
import json

import psutil
import socket
import httplib, urllib, os
from datetime import datetime

monitor_ip = "127.0.0.1"
monitor_port = "8080"
monitor_serverId = "server-182"  # 对应监控系统的配置文件config.js的serverlist子节点id
# sourceFile=r'E:\Java\Visual\CMB-Monitor\WebContent\data\linux02kf.csv'#监控系统读取状态存储文件的路径
arcgisServer_filename="MsStatus"#监控系统存储地图服务状态的文件名
arcgisServer_ip = "192.168.119.134"#arcgis server服务器IP
arcgisServer_port = "6080"#arcgis server服务器访问端口
arcgisServer_admin = "arcgis"#arcgis server 管理员用户名
arcgisServer_user = "arcgis"#arcgis server 管理员用户密码
arcgisServer_reportUrl = "/arcgis/admin/services/report?f=json"#获取arcgis server地图服务状态的rest接口
arcgisServer_tokenUrl = "/arcgis/tokens/generateToken?f=pjson"#获取arcgis server用户临时token的rest接口


# 发送服务器资源状况到监控系统
def sendServerData(value):
    data = urllib.urlencode(
        {"serverid": monitor_serverId, "value": value})
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    conn = httplib.HTTPConnection(monitor_ip, monitor_port)
    # 开始进行数据提交   同时也可以使用get进行
    conn.request("POST", "/CMB-Monitor/updateServerStatus", data, headers)
    # 返回处理后的数据s
    response = conn.getresponse()
    print response.read()

# 发送地图服务状态到监控系统
def sendMapServerData(mapServerStatus):
    msName = ""
    msStatus = ""
    for item in mapServerStatus:
        msName += item["serviceName"] + ","
        msStatus += item["status"]["realTimeState"] + ","

    msName=msName[0:(len(msName)-1)]
    msStatus=msStatus[0:(len(msStatus)-1)]
    data = urllib.urlencode(
        {"mssid":arcgisServer_filename,"title": msName, "value": msStatus})
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


# 获取Server的运行状态
def getArcgisServerStatus():
    ret = os.popen('service arcgisserver status').readlines()  # arcgisserver根据注册的名字相应修改-只适用用于linux
    if len(ret) > 0 and 'running' in ret:
        return 'true'
    else:
        return 'false'


# 获取地理编码的运行状态
def getGeocodeStatus():
    ret = os.popen('service arcgisserver status').readlines()  # arcgisserver根据注册的名字相应修改-只适用用于linux
    if len(ret) > 0 and 'running' in ret:
        return 'true'
    else:
        return 'false'


def getResourceStatus():
    cpu_info1 = psutil.cpu_percent(interval=1, percpu=False)  # 统计1s内每个cpu的使用率
    cpu_info = psutil.cpu_percent(interval=1, percpu=True)  # 统计1s内每个cpu的使用率
    cpu_count = psutil.cpu_count(logical=True)  # 统计cpu核数
    cpu_info_str = []
    for i in cpu_info:
        cpu_info_str.append(str(i))
    vmemory_info = psutil.virtual_memory()  # 虚拟内存情况

    smemory_info = psutil.swap_memory()  # 物理内存情况
    doc = dict()
    doc["ip"] = socket.gethostbyname(socket.gethostname())
    doc['hostname'] = socket.gethostname()
    doc['os'] = os.name  # nt:windows，posix：linux/unix
    doc['cpu'] = ','.join(cpu_info_str)
    doc['cpuTotal'] = cpu_info1
    doc['cpucount'] = cpu_count
    doc['vmemory'] = vmemory_info.percent
    doc['smemory'] = smemory_info.percent
    doc['date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return doc


if __name__ == '__main__':
    gisServerStatus = getArcgisServerStatus()
    resourceStatus = getResourceStatus()
    sendServerData("%s,%s,%s,%s,%s,%s,%s"%(resourceStatus["ip"],resourceStatus["hostname"],resourceStatus["os"],resourceStatus["cpuTotal"],resourceStatus["vmemory"],"true","true"))

    mapServerStatus = getMapServerStatus()
    sendMapServerData(mapServerStatus)
