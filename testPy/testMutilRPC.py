# encoding=utf8
import SocketServer
import arcpy
import thread
from SimpleXMLRPCServer import SimpleXMLRPCServer

import dataManage.serverHandler.arcpyControl.arcpyOp as ac

rpcServer = {"ip": "127.0.0.1", "port": 8081}
# The server object
class Server:
    count = 0

    def __init__(self):
        pass

    # 测试连接是否正常
    def TestConnect(self):
        return True

    def getfields(self,layerpath):
        try:
            return ac.getLayerFields(layerpath)
        except Exception as e:
            print e.message

    def deleteLayer(self,layerpath):
        try:
            ss=arcpy.Delete_management(layerpath)
            return True
        except Exception as e:
            print e.message


# 多线程实现
class RPCThreading(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    pass


def getfields(layerpath):
        try:
            return ac.getLayerFields(layerpath)
        except Exception as e:
            print e.message

# 远程调用服务端
# 参数传输避免用中文
global mutex
mutex = thread.allocate_lock()
server_object = Server()

s = SimpleXMLRPCServer((rpcServer["ip"], int(rpcServer["port"])), allow_none=True)
#s = RPCThreading((rpcServer["ip"], int(rpcServer["port"])))
s.register_function(getfields)
#s.register_instance(server_object)
s.serve_forever()  # 会保持服务端不停止
