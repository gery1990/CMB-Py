from xmlrpclib import ServerProxy
import time
from  multiprocessing import Pool

def add(ags):
    s = ServerProxy("http://99.12.95.185:6080")
    print s.TestConnect()
    #print s.getfields(r'D:\dataHandle\GISData\template.gdb\client')


if __name__ == "__main__":
    #s = ServerProxy("http://127.0.0.1:8080")
    s = ServerProxy("http://192.168.119.134:9000")
    #s2 = ServerProxy("http://127.0.0.1:8081")
    print s.TestConnect()

    # s.InsertRow_Point(r'D:\dataHandle\template.gdb\client', r'C:\Users\esri\Desktop\customer100005', 2, 1, ["UID", "T1", "X", "Y"],
    #           ["SHAPE@", "T1", "X", "Y"])
    s.InsertRow_Point(r'/home/arcgis/customer.gdb/client', '/home/arcgis/customer100005', 2, 1, ["UID", "T1", "X", "Y"],
              ["SHAPE@"])
    #fieldsMap = {'LAYERNAME': "TEXT", "XMAX": "float", "XMIN": "float", "YMAX": "float", "YMIN": "float"}
    #.CreateLayer(r'D:\template.gdb', 'extent', r'D:\python_project\CMB-Py\dataManage\workspace\wgs84.prj', "POLYGON", fieldsMap)
    #print s2.getfields(r'D:\template.gdb\client1')
    # print s.add(2, 2)
    #
    # pool = Pool(processes=5)
    # for i in xrange(0, 5):
    #     pool.apply_async(add, (i,))
    # pool.close()
    # pool.join()

    # s2=ServerProxy("http://127.0.0.1:8080")
    # print s2.add(3,2)
    # s3=ServerProxy("http://127.0.0.1:8080")
    # print s3.add(4,2)
    # s4=ServerProxy("http://127.0.0.1:8080")
    # print s4.add(5,2)
    # s.multiProcess()
    # print s.add(3,4)
