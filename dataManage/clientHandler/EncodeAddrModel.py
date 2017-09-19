# encoding=utf-8
'''
Created on 2016-8-29
@author: esri
'''
import urllib, demjson


class EncodeModel:
    dictFields = {"status": {"type": "TEXT", "sourceFrom": "encode"},
                  "query_address": {"type": "TEXT", "sourceFrom": "encode"},
                  "formatted_address": {"type": "TEXT", "sourceFrom": "encode"},
                  "provincename": {"type": "TEXT", "sourceFrom": "encode"},
                  "citycode": {"type": "TEXT", "sourceFrom": "encode"},
                  "city": {"type": "TEXT", "sourceFrom": "encode"},
                  "x": {"type": "DOUBLE", "sourceFrom": "encode"},
                  "y": {"type": "DOUBLE", "sourceFrom": "encode"},
                  "score": {"type": "LONG", "sourceFrom": "encode"}}
    status = "NO"
    query_address = ""
    formatted_address = ""
    provincename = ""
    citycode = ""
    city = "",
    x = 0.0
    y = 0.0
    score = 0
    address = ''

    def __init__(self, addr, url):
        self.url = url
        # 地址处理，转换编码，去除符号
        self.address = self.clearAddress(addr)

    def convertValue(self, value):
        if type(value) == unicode:
            return value
        elif type(value) == str:
            return value.decode('UTF-8')
        elif type(value) == float:
            return float(value)
        elif type(value) == int:
            return int(value)
        else:
            return value

    def clearAddress(self, addr):
        address = addr.replace('#', "")
        address = address.replace('，', "")
        address = address.replace('·', "")
        address = address.replace('/', "")
        address = address.replace('\\', "")
        address = address.replace('-', "")
        address = address.replace('=', "")
        address = address.replace('（', "")
        address = address.replace('）', "")
        address = address.replace('%', "")
        address = address.replace('.', "")
        address = address.replace(' ', "")
        address = address.replace('', "")
        address = address.replace('', "")
        address = address.replace('•', "")
        address = address.replace('(', "")
        address = address.replace(')', "")
        address = address.replace('@', "")
        return address

    def encode(self):
        result = None
        if self.address != None and self.address != "":
            url = self.url + "queryStr=" + self.address + "&type=address&currentPage=1&pageSize=1&f=json"
            strfromkey = urllib.urlopen(url)
            result = demjson.decode(strfromkey.read())
        if result != None and result["status"].encode("UTF-8").upper() == "OK" and result.has_key("results"):
            if len(result["results"]) > 0:
                self.status = "OK"
                self.query_address = self.convertValue(result["results"][0]["query_address"])
                self.formatted_address = self.convertValue(result["results"][0]["formatted_address"])
                self.provincename = self.convertValue(result["results"][0]["provincename"])
                self.citycode = self.convertValue(result["results"][0]["citycode"])
                self.city = result["results"][0]["cityname"]
                if self.citycode == '':
                    self.city = result["results"][0]["countyname"]
                    self.citycode = self.convertValue(result["results"][0]["countycode"])
                self.x = self.convertValue(result["results"][0]["longitude"])
                self.y = self.convertValue(result["results"][0]["latitude"])
                self.score = self.convertValue(result["results"][0]["score"])
