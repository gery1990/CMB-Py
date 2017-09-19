#coding=utf8
from xmlrpclib import ServerProxy
import time

if __name__=="__main__":
    s=ServerProxy("http://127.0.0.1:8081")
    #print s.TestConnect()
    print s.getfields(r'D:\dataHandle\GISData\template.gdb\client')
    #print s.InsertRow(r"D:\dataHandle\template.gdb\client_1",r'C:\Users\esri\Desktop\customer10000',2,1,["UID","T1","X","Y"])
    #s.multiProcess()
    #print s.add(3,4)