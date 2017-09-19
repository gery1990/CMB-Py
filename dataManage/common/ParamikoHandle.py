# coding=utf8
import paramiko
import traceback, os, csv
from socket import setdefaulttimeout


class SSHControl():
    def __init__(self, serverList, balanceStatusFile, logger, serverInfo=None):
        try:
            self.serverList = serverList
            self.balanceStatusFile = balanceStatusFile
            self.logger = logger
            setdefaulttimeout(None)
            ip = ""
            port = ""
            user = ""
            password = ""
            # 检查远端服务器连通性，轮循ArcGIS Server服务器进行空间数据转换
            if serverInfo is not None:
                self.serverInfo = serverInfo
                ip = self.serverInfo[0]
                port = self.serverInfo[1]
                user = self.serverInfo[2]
                password = self.serverInfo[3]
            else:
                for i in xrange(len(serverList)):
                    self.serverInfo = self.selectServer()
                    if self.testConnect(self.serverInfo[0], self.serverInfo[2], self.serverInfo[3]):
                        ip = self.serverInfo[0]
                        port = self.serverInfo[1]
                        user = self.serverInfo[2]
                        password = self.serverInfo[3]
                        break
                    else:
                        self.logger.warning(u"IP：%s:%s can not connect" % (serverInfo[0], serverInfo[1]))
            if ip != "":
                self._ssh_fd = self.ssh_connect(ip, user, password)
        except:
            traceback.print_exc()
            self.logger.warning(traceback.format_exc())

    def ssh_connect(self, _host, _username, _password):
        try:
            _ssh_fd = paramiko.SSHClient()
            _ssh_fd.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            _ssh_fd.connect(_host, username=_username, password=_password)
            return _ssh_fd
        except Exception, e:
            print('ssh %s@%s: %s' % (_username, _host, e))
            self.logger.warning(traceback.format_exc())
            return None

    def ssh_exec_cmd(self, _cmd):
        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(_cmd)
            err_list = stderr.readlines()

            if len(err_list) > 0:
                self.logger.warning(err_list[0])

            for item in stdout.readlines():
                if "ssherror" in item:
                    return False
                    # self.logger.warning(item)
            return True
        except:
            self.logger.warning('exec %s' % _cmd)
            self.logger.warning(traceback.format_exc())

    def ssh_close(self):
        self._ssh_fd.close()

    def selectServer(self):
        # 负载均衡选择空闲服务器
        serverIp = ""
        serverPort = ""
        serverUser = ""
        serverPasswd = ""
        if os.path.exists(self.balanceStatusFile) is not True:
            balanceObj = file(self.balanceStatusFile, 'wb')  # 文件不存在则创建，如果存在则清空
            serverIp = self.serverList[0]["ip"]
            serverPort = self.serverList[0]["port"]
            serverUser = self.serverList[0]["user"]
            serverPasswd = self.serverList[0]["password"]
            balanceW = csv.writer(balanceObj)
            balanceW.writerow([self.serverList[0]["id"]])
            balanceObj.close()
        else:
            balanceObj = file(self.balanceStatusFile, 'rb')
            balanceR = csv.reader(balanceObj)
            lineStr = balanceR.next()
            useId = lineStr[0]
            if len(self.serverList) == 1:
                serverIp = self.serverList[0]["ip"]
                serverPort = self.serverList[0]["port"]
                serverUser = self.serverList[0]["user"]
                serverPasswd = self.serverList[0]["password"]
                useId = self.serverList[0]["id"]
            for item in self.serverList:
                if item["id"] != useId:
                    serverIp = item["ip"]
                    serverPort = item["port"]
                    serverUser = item["user"]
                    serverPasswd = item["password"]
                    useId = item["id"]
                    break
            balanceObj.close()

            balanceObj = file(self.balanceStatusFile, 'wb')  # 文件不存在则创建，如果存在则清空
            balanceW = csv.writer(balanceObj)
            balanceW.writerow([useId])
            balanceObj.close()
        return [serverIp, serverPort, serverUser, serverPasswd]

    def testConnect(self, ip, user, password):
        try:
            ssh_fd = self.ssh_connect(ip, user, password)
            if ssh_fd == None:
                return False
            else:
                ssh_fd.close()
                return True
        except:
            return False
