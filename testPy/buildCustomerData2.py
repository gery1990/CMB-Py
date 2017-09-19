# coding:utf-8
import random, arcpy

#
# UID,Y,X,T1,T7,T30,T60,T180,BankID,AGE,SEX,Assets_dis,BankCard_d,BranchID
# PC9UVV5AWMN6I,22.550385,114.006947,1,20,20,90,200,755,28,F,15000,010,755492
filePath = r'C:\Users\esri\Desktop\customer50000000'
title = "UID,T1,T7,T30,T90,T180,BANKID1,BANKID2,AGE,SEX,ASSETS_DIS,BANKCARD_D,BRANCHID,X,Y"
splitStr = chr(0X7C) + chr(0X1C)

if __name__ == '__main__':
    fileObj = file(filePath, 'wb')

    # insertCursor = arcpy.da.InsertCursor(r'C:\Users\LocalUser\Desktop\customerdata.gdb\customerdata',
    #                                      ["UID", "T1", "T7", "T30", "T90", "T180", "BANKID1", "BANKID2", "AGE", "SEX",
    #                                       "ASSETS_DIS",
    #                                       "BANKCARD_D", "BRANCHID", "X", "Y", "SHAPE@"])
    count = 0
    for i in xrange(5000000):
        if i % 2 == 0:
            count += 1
        if i % 10000 == 0:
            print i
        uid = "UID%d" % count
        x = random.uniform(113.900, 114.159)
        y = random.uniform(22.467, 22.678)

        T1 = random.randint(10, 100)
        T7 = random.randint(10, 100)
        T30 = random.randint(10, 100)
        T90 = random.randint(10, 100)
        T180 = random.randint(10, 100)
        BankID1 = random.choice(["755", "127"])
        BankID2 = random.choice(["755", "127"])
        AGE = random.randint(10, 60)
        SEX = random.choice(["F", "M"])
        Assets_dis = random.uniform(0, 10000000)
        BankCard_d = random.choice(["010", "030", "040", "020", "050"])
        BranchID = random.choice(["755519", "755557", "125526", "713542"])
        record = "%s%s%d%s%d%s%d%s%d%s%d%s%s%s%s%s%d%s%s%s%s%s%s%s%s%s%f%s%f%s\n" % (
            uid, splitStr, T1, splitStr, T7, splitStr, T30, splitStr, T90, splitStr, T180,
            splitStr, BankID1, splitStr, BankID2, splitStr, AGE, splitStr, SEX, splitStr, Assets_dis, splitStr,
            BankCard_d, splitStr, BranchID, splitStr, x, splitStr, y, splitStr)
        # record = uid + splitStr + y + splitStr + x + splitStr + T1 + splitStr + T7 + splitStr + T30 + splitStr + T60 + splitStr + T180 + splitStr + BankID + splitStr + AGE + splitStr + SEX + splitStr + Assets_dis + splitStr + BankCard_d + splitStr + BranchID
        fileObj.write(record)
    for i in xrange(50):
        if i % 10000 == 0:
            print i
        x_s = 74 + (i * 1.6)
        x_end = 74 + ((i + 1) * 1.6)
        for i in xrange(1000000):
            if i % 2 == 0:
                count += 1
            uid = "UID%d" % count
            x = random.uniform(x_s, x_end)
            y = random.uniform(22.467, 53)

            T1 = random.randint(10, 100)
            T7 = random.randint(10, 100)
            T30 = random.randint(10, 100)
            T90 = random.randint(10, 100)
            T180 = random.randint(10, 100)
            BankID1 = random.choice(["755", "127"])
            BankID2 = random.choice(["755", "127"])
            AGE = random.randint(10, 60)
            SEX = random.choice(["F", "M"])
            Assets_dis = random.uniform(0, 10000000)
            BankCard_d = random.choice(["010", "030", "040", "020", "050"])
            BranchID = random.choice(["755519", "755557", "125526", "713542"])
            record = "%s%s%d%s%d%s%d%s%d%s%d%s%s%s%s%s%d%s%s%s%s%s%s%s%s%s%f%s%f%s\n" % (
                uid, splitStr, T1, splitStr, T7, splitStr, T30, splitStr, T90, splitStr, T180,
                splitStr, BankID1, splitStr, BankID2, splitStr, AGE, splitStr, SEX, splitStr, Assets_dis, splitStr,
                BankCard_d, splitStr, BranchID, splitStr, x, splitStr, y, splitStr)
            # record = uid + splitStr + y + splitStr + x + splitStr + T1 + splitStr + T7 + splitStr + T30 + splitStr + T60 + splitStr + T180 + splitStr + BankID + splitStr + AGE + splitStr + SEX + splitStr + Assets_dis + splitStr + BankCard_d + splitStr + BranchID
            fileObj.write(record)
    fileObj.close()
