# coding:utf-8
import os, csv, random, arcpy, copy
import traceback

# 模拟潜在客户数据
gdbPath = r"D:\5000W\clientdata.gdb"
layerName = "clientdata"

# UID,Y,X,T1,T7,T30,T60,T180,BankID,AGE,SEX,Assets_dis,BankCard_d,BranchID
# PC9UVV5AWMN6I,22.550385,114.006947,1,20,20,90,200,755,28,F,15000,010,755492
fields = ["CID", "City", "CName", "CDate", "CAddress", "CCapital", "CArtificia", "CTelephone", "CPhone", "CBusiType",
          "X", "Y", "BZ1", "BZ2", "SHAPE@"]

splitStr = chr(0X7C) + chr(0X1C)

if __name__ == '__main__':
    fileObj = file(r'C:\Users\esri\Desktop\customerupdate', 'wb')

    hasLayer = arcpy.Exists(os.path.join(gdbPath, layerName))
    if hasLayer is not True:
        exit(0)
    # insertCursor = arcpy.da.InsertCursor(os.path.join(gdbPath, layerName), fields)

    for i in xrange(100000):
        uid = "UID%d" % i
        x = random.uniform(8942971,13768960)
        y = random.uniform(2661356,6909497)
        CID = i
        City = random.choice(["杭州", "北京", "上海", "广州", "武汉", "深圳", "长沙", "南京", "苏州", "乌鲁木齐"])
        CName = "new客户%s" % (str(CID))
        CDate = "2017/4/19"
        CAddress = "test%s" % (str(CID))
        CCapital = CID
        CArtificia = "法人%s" % (str(CID))
        CTelephone = "158548484841"
        CPhone = "846848154"
        CBusiType = "经营范围%s" % (str(CID))
        Clng = x
        CLat = y
        BZ1 = ""
        BZ2 = ""

        p = arcpy.Point(x, y)
        # values = [CID, City, CName, CDate, CAddress, CCapital, CArtificia, CTelephone, CPhone, CBusiType,
        #           Clng, CLat, BZ1, BZ2, p]
        # insertCursor.insertRow(values)

        record = "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\n" % (
            CID, splitStr, City, splitStr, CName, splitStr, CDate, splitStr, CAddress, splitStr, CCapital, splitStr,
            CArtificia, splitStr, CTelephone, splitStr, CPhone, splitStr, CBusiType, splitStr,
            Clng, splitStr, CLat, splitStr, BZ1, splitStr, BZ2, splitStr)
        fileObj.write(record)

        if i % 1000 == 0:
            print i

    # del insertCursor
    # fileObj.close()