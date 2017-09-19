# coding:utf-8
import os, csv, random
import traceback

filePath = r'C:\Users\LocalUser\Desktop\\customer10000'
f = file(filePath, 'rb')
d = chr(0X7C) + chr(0X1C)
lineStr= f.readline()
print lineStr.split(d)
#
# UID,Y,X,T1,T7,T30,T60,T180,BankID,AGE,SEX,Assets_dis,BankCard_d,BranchID
# PC9UVV5AWMN6I,22.550385,114.006947,1,20,20,90,200,755,28,F,15000,010,755492
# filePath = u'D:\\业务数据入库测试\\customer4000w.csv'
# title = "UID,Y,X,T1,T7,T30,T60,T180,BankID,AGE,SEX,Assets_dis,BankCard_d,BranchID"
# if __name__ == '__main__':
#     fileObj = file(filePath, 'wb')
#     csvWriter = csv.writer(fileObj)
#     csvWriter.writerow(title)
#     for i in xrange(40000000):
#         uid = "UID%d" % i
#         x = random.uniform(113.79, 114.577)
#         y = random.uniform(22.277, 22.467)
#         T1=random.randint(10,100)
#         T7=random.randint(10,100)
#         T30=random.randint(10,100)
#         T60=random.randint(10,100)
#         T180=random.randint(10,100)
#         BankID="755"
#         AGE=random.randint(20,50)
#         SEX=random.choice(["F","M"])
#         Assets_dis="15000"
#         BankCard_d="010"
#         BranchID="755492"
#         csvWriter.writerow([uid,y,x,T1,T7,T30,T60,T180,BankID,AGE,SEX,Assets_dis,BankCard_d,BranchID])
#     fileObj.close()
