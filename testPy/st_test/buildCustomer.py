# coding:utf-8
import os, csv, random
import traceback

splitStr = chr(0X7C) + chr(0X1C)


def buildBaseData():
    filePath = r'C:\Users\LocalUser\Desktop\\customer'
    fileObj = file(filePath, 'wb')
    values = [
        {"id": "UID0", "y": "22.354872", "x": "114.134929", "T1": "31"},
        {"id": "UID1", "y": "22.390057", "x": "113.942251", "T1": "99"},
        {"id": "UID2", "y": "22.402573", "x": "114.276300", "T1": "94"},
        {"id": "UID3", "y": "22.278914", "x": "113.843986", "T1": "24"},
        {"id": "UID4", "y": "22.349215", "x": "114.211664", "T1": "56"},
        {"id": "UID5", "y": "22.351329", "x": "114.309930", "T1": "38"},
        {"id": "UID6", "y": "22.352957", "x": "114.296494", "T1": "76"},
        {"id": "UID7", "y": "22.402139", "x": "114.507122", "T1": "94"},
        {"id": "UID8", "y": "22.303014", "x": "114.078318", "T1": "87"},
        {"id": "UID9", "y": "22.425124", "x": "114.335466", "T1": "20"},
        {"id": "UID10", "y": "22.311624", "x": "113.800536", "T1": "55"},
        {"id": "UID11", "y": "22.353135", "x": "114.300953", "T1": "49"},
        {"id": "UID12", "y": "22.388742", "x": "114.335693", "T1": "67"},
        {"id": "UID13", "y": "22.388647", "x": "113.869823", "T1": "95"},
        {"id": "UID14", "y": "22.382354", "x": "114.395536", "T1": "15"},
        {"id": "UID15", "y": "22.449544", "x": "113.978114", "T1": "97"},
        {"id": "UID16", "y": "22.442253", "x": "114.464106", "T1": "65"},
        {"id": "UID17", "y": "22.310165", "x": "", "T1": "85"},
        {"id": "UID18", "y": "22.433169", "x": "114.551801", "T1": "95"},
        {"id": "UID19", "y": "22.434742", "x": "114.214386", "T1": "65"},
        {"id": "UID20", "y": "", "x": "", "T1": "86"},
        {"id": "UID21", "y": "22.310165", "x": "114.201848", "T1": "83"},
        {"id": "UID22", "y": "", "x": "114.201848", "T1": "84"},
        {"id": "UID23", "y": "22.310165", "x": "", "T1": "85"},
        {"id": "UID24", "y": "2a.3s165", "x": "1.f018", "T1": "87"},
        {"id": "UID25", "y": "22.#$%", "x": "114.2^&*()8", "T1": "88"},
        {"id": "UID26", "y": "22310165", "x": "114201848", "T1": "89"}]
    for i in values:
        record = "%s%s%s%s%s%s%s%s\n" % (i["id"], splitStr, i["y"], splitStr, i["x"], splitStr, i["T1"], splitStr)
        fileObj.write(record)
    fileObj.close()


def buildAddData():
    #增加数据：5个
    #错误数据：2个
    filePath = r'C:\Users\LocalUser\Desktop\\customer_update'
    fileObj = file(filePath, 'wb')
    values = [
        {"id": "UID0", "y": "22.354872", "x": "116.134929", "T1": "32"},
        {"id": "UID1", "y": "22.390057", "x": "113.942251", "T1": "92"},
        {"id": "UID2", "y": "22.402573", "x": "114.276300", "T1": "92"},
        {"id": "UID3", "y": "22.278914", "x": "113.843986", "T1": "22"},
        {"id": "UID4", "y": "22.349215", "x": "114.211664", "T1": "52"},
        {"id": "UID5", "y": "22.351329", "x": "114.309930", "T1": "32"},
        {"id": "UID6", "y": "22.352957", "x": "114.296494", "T1": "72"},
        {"id": "UID7", "y": "22.402139", "x": "114.507122", "T1": "94"},
        {"id": "UID8", "y": "22.303014", "x": "114.078318", "T1": "87"},
        {"id": "UID9", "y": "22.425124", "x": "114.335466", "T1": "20"},
        {"id": "UID10", "y": "22.311624", "x": "113.800536", "T1": "55"},
        {"id": "UID11", "y": "22.353135", "x": "114.300953", "T1": "49"},
        {"id": "UID12", "y": "22.388742", "x": "114.335693", "T1": "67"},
        {"id": "UID13", "y": "22.388647", "x": "113.869823", "T1": "95"},
        {"id": "UID14", "y": "22.382354", "x": "114.395536", "T1": "15"},
        {"id": "UID15", "y": "22.449544", "x": "113.978114", "T1": "97"},
        {"id": "UID16", "y": "22.442253", "x": "114.464106", "T1": "65"},
        {"id": "UID17", "y": "22.433169", "x": "114.551801", "T1": "95"},
        {"id": "UID18", "y": "22.434742", "x": "114.214386", "T1": "65"},
        {"id": "UID19", "y": "22.310165", "x": "114.201848", "T1": "83"},
        {"id": "UID20", "y": "22.453639", "x": "114.089738", "T1": "10"},
        {"id": "UID21", "y": "22.454106", "x": "114.406282", "T1": "25"},
        {"id": "UID22", "y": "ssdf", "x": "wer", "T1": "65"},
        {"id": "UID23", "y": "22.418954", "x": "114.092166", "T1": "29"},
        {"id": "UID24", "y": "22.385872", "x": "114.511514", "T1": "35"},
        {"id": "UID25", "y": "22.321967", "x": "114.240618", "T1": "20"},
        {"id": "UID26", "y": "", "x": "", "T1": "83"}]
    for i in values:
        record = "%s%s%s%s%s%s%s%s\n" % (i["id"], splitStr, i["y"], splitStr, i["x"], splitStr, i["T1"], splitStr)
        fileObj.write(record)
    fileObj.close()


if __name__ == '__main__':
    buildBaseData()
    buildAddData()