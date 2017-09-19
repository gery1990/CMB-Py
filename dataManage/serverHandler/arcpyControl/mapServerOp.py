# encoding=utf8
import json, httplib, urllib, logging, traceback

logger = logging.getLogger('main')


# arcgis server rest处理类

class MapServerOp():
    servicesUrl = '/arcgis/admin/services/%s?f=pjson'
    stopUrl = r"/arcgis/admin/services/%s.MapServer/stop?f=json"
    startUrl = r"/arcgis/admin/services/%s.MapServer/start?f=json"
    deleteUrl = r'/arcgis/admin/services/%s.MapServer/delete?f=json'
    tokenUrl = r"/arcgis/tokens/generateToken?f=pjson"

    def __init__(self, ip, port, user, password):
        self.ip = ip
        self.port = port
        self.user = user
        self.password = password

    # 获取ArcgisServer的访问token
    def getArcGISServerToken(self):
        data = urllib.urlencode(
            {"username": self.user,
             "password": self.password, "client": "requestip", "expiration": 1})

        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        conn = httplib.HTTPConnection(self.ip, self.port)
        # 开始进行数据提交   同时也可以使用get进行
        conn.request("POST", self.tokenUrl, data, headers)
        # 返回处理后的数据
        response = conn.getresponse()
        decodejson = json.loads(response.read())
        logger.info(decodejson)
        return decodejson["token"]

    # 获取地图服务状态信息
    def mapServerOp(self, url):
        try:
            token = self.getArcGISServerToken()
            data = urllib.urlencode({"token": token})
            # logger.info("%s:%s%s&token=%s" % (self.ip, self.port, url, token))

            headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
            conn = httplib.HTTPConnection(self.ip, self.port)
            # 开始进行数据提交   同时也可以使用get进行
            conn.request("POST", url, data, headers)
            # 返回处理后的数据
            response = conn.getresponse()
            decodejson = json.loads(response.read())

            logger.info(decodejson)
            return decodejson["status"]
        except:
            logger.warning(traceback.format_exc())
            return "faild"

    def hasServer(self, type, serverName):
        services = self.servicesList(type)
        for service in services:
            if service['serviceName'] == serverName:
                return True

    def serverOp(self, opType,serverName):
        items = serverName.split('/')
        type = ''
        sName = ''
        if len(items) > 1:
            type = items[0]
            sName = items[1]
        else:
            sName = items[0]
        if self.hasServer(type, sName) is not True:
            return True
        if opType == 'stop':
            return self.stopServer(serverName)
        elif opType == 'start':
            return self.startServer(serverName)
        elif opType == 'delete':
            return self.deleteServer(serverName)
        else:
            return True

    def stopServer(self, serverName):
        return self.mapServerOp(self.stopUrl % serverName)

    def startServer(self, serverName):
        return self.mapServerOp(self.startUrl % serverName)

    def deleteServer(self, serverName):
        return self.mapServerOp(self.deleteUrl % serverName)

    def servicesList(self, type):
        try:
            token = self.getArcGISServerToken()
            data = urllib.urlencode({"token": token})
            # logger.info("%s:%s%s&token=%s" % (self.ip, self.port, url, token))

            headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
            conn = httplib.HTTPConnection(self.ip, self.port)
            # 开始进行数据提交   同时也可以使用get进行
            conn.request("POST", self.servicesUrl % type, data, headers)
            # 返回处理后的数据
            response = conn.getresponse()
            decodejson = json.loads(response.read())

            logger.info(decodejson)
            return decodejson["services"]
        except:
            logger.warning(traceback.format_exc())
            return ""


if __name__ == '__main__':
    m = MapServerOp('99.12.95.185', '8080', 'arcgis', 'arcgis')
    print m.stopServer('crm/client1')
