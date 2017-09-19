# encoding=utf8
import psutil
import socket
import httplib, urllib, os, random, json, time
from datetime import datetime

# 服务器资源、系统服务 监控模块
monitor_ip = "127.0.0.1"
monitor_port = "8080"
monitor_serverId = "server-182"  # 对应监控系统的配置文件config.js的serverlist子节点id
logTitle = "time,ip,hostname,os,cpu,smem,gisservice,clientHandler"

arcgisserver_ip = '99.12.95.182'
arcgisserver_port = '6080'

geocode_ip = "99.12.95.182"
geocode_port = "6080"


# 发送服务器资源状况到监控系统
def sendServerData(value):
    data = urllib.urlencode(
        {"serverid": monitor_serverId, "title": logTitle, "value": value})
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    conn = httplib.HTTPConnection(monitor_ip, monitor_port)
    # 开始进行数据提交   同时也可以使用get进行
    conn.request("POST", "/CMB-Monitor/updateServerStatus", data, headers)
    # 返回处理后的数据s
    response = conn.getresponse()
    print response.read()


# 获取Server的运行状态
def getArcgisServerStatus():
    try:
        f = urllib.urlopen('http://%s:%s/arcgis/rest?f=json' % (arcgisserver_ip, arcgisserver_port))
        if f.code == 200:
            return "true"
        return "false"
    except IOError as e:
        return 'false'


def getGeocodeStatus():
    try:
        data = urllib.urlencode({"queryStr": "深圳市", "f": "json", "random": random.uniform(0, 1)})
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        conn=None
        try:
            conn = httplib.HTTPConnection(geocode_ip, geocode_port)
        except:
            return 'false'
        # 开始进行数据提交   同时也可以使用get进行
        conn.request("POST", "/GT/rest/services/singleservice/single", data, headers)
        # 返回处理后的数据s
        response = conn.getresponse()
        decodejson = json.loads(response.read())
        if decodejson["status"] == "OK":
            return 'true'
        else:
            return 'false'
    except :
        return 'false'


# 获取服务器资源情况
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
    geocodeStatus = getGeocodeStatus()
    resourceStatus = getResourceStatus()
    sendServerData("%s,%s,%s,%s,%s,%s,%s,%s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                                                resourceStatus["ip"], resourceStatus["hostname"], resourceStatus["os"],
                                                resourceStatus["cpuTotal"],
                                                resourceStatus["smemory"], gisServerStatus, geocodeStatus))
