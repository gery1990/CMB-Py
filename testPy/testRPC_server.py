# encoding=utf8
from SimpleXMLRPCServer import SimpleXMLRPCServer
import os


# 远程调用服务端

# 测试连接是否正常
def TestConnect():
    return True


def KillConnect():
    s.shutdown()
    s.server_close()


def add(x, y):
    return x + y


if __name__ == '__main__':
    global s
    s = SimpleXMLRPCServer(('127.0.0.1', 6080), allow_none=True)
    s.register_function(TestConnect)
    s.register_function(KillConnect)
    s.register_function(add)
    s.serve_forever(0.1)
