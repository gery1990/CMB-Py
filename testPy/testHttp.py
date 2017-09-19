# encoding=utf8
import httplib,urllib
import os
data = urllib.urlencode({"filepath":'e:\\linux02kf.csv',"value":"99.12.95.182,linux02kf,linux,77.3%,11.11%,true"})

headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
conn = httplib.HTTPConnection('127.0.0.1',8080)
#开始进行数据提交   同时也可以使用get进行
conn.request("POST","/CMB-Monitor/updateStatus",data, headers)
#返回处理后的数据
response = conn.getresponse()
print response.read()