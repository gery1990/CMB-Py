# coding:utf-8
import os, csv, random
import traceback

splitStr = chr(0X7C) + chr(0X1C)


def buildBaseData():
    # 空地址：2
    # 特殊字符：2
    # 地址无效：2
    filePath = r'C:\Users\LocalUser\Desktop\client'
    title = "ID,ADDRESS"
    fileObj = file(filePath, 'wb')
    values = [
        {"id": '1', "addr": "广东省深圳市龙岗区爱联翡翠明珠花园", "bz": "1"},
        {"id": '2', "addr": "广东省深圳市龙岗绿景大公馆6栋3b", "bz": "2"},
        {"id": '3', "addr": "广东省深圳市龙岗区南联鹏达商业街1号", "bz": "3"},
        {"id": '4', "addr": "广东省深圳市福田区华强北路华强广场A座14G", "bz": "4"},
        {"id": '5', "addr": "广东省深圳市布吉一村树山4号", "bz": "5"},
        {"id": '6', "addr": "", "bz": "6"},
        {"id": '7', "addr": "广东省深圳市布■☆吉一村树山4号", "bz": "7"},
        {"id": '8', "addr": "广东省深圳市龙岗区布吉镇✘☀百花一路西七巷3号", "bz": "8"},
        {"id": '9', "addr": "swrwerwr", "bz": "9"},
        {"id": '10', "addr": "你好", "bz": "10"},
        {"id": '11', "addr": "", "bz": "11"},
        {"id": '12', "addr": "达升物流大厦（西门）", "bz": "12"},
        {"id": '13', "addr": "天虹商场(南山常兴店)", "bz": "13"},
        {"id": '14', "addr": "ASas@&*￥", "bz": "14"}]
    for i in values:
        record = "%s%s%s%s%s%s\n" % (i["id"], splitStr, i["addr"], splitStr,i["bz"], splitStr)
        fileObj.write(record)
    fileObj.close()


def buildAddData():
    # 新增数据：5
    filePath = r'C:\Users\LocalUser\Desktop\client_add'
    title = "ID,ADDRESS"
    fileObj = file(filePath, 'wb')
    values = [
        {"id": '20', "addr": "广东省深圳市深圳市福田区金地花园201栋502","bz":"20"},
        {"id": '21', "addr": "广东省深圳市宝安区宝安大道宝安大道","bz":"21"},
        {"id": '22', "addr": "广东省深圳市龙岗区布吉老街翠枫豪园","bz":"22"},
        {"id": '23', "addr": "广东省深圳市福田区深圳技师学院","bz":"23"},
        {"id": '24', "addr": "广东省深圳市广东省深圳市宝安区新安街道办73区布心一村9栋604","bz":"24"},
        {"id": '25', "addr": "你好", "bz": "25"},
        {"id": '26', "addr": "", "bz": "26"},
        {"id": '27', "addr": "达升物流大厦（西门）", "bz": "27"},
        {"id": '28', "addr": "天虹商场(南山常兴店)", "bz": "28"},
        {"id": '29', "addr": "广东省深圳市布吉一村树山4号", "bz": "29"},
        {"id": '30', "addr": "深圳市南山区姚园路86号 天虹商场", "bz": "30"},
        {"id": '31', "addr": "深圳市南山区姚园路86号 天虹商场 南山常兴店", "bz": "31"},
        {"id": '32', "addr": "深圳市南山区姚园路86号 天虹商场(南山常兴店)", "bz": "32"},
        {"id": '33', "addr": "深圳市南山区姚园路86号 天虹商场（南山常兴店）", "bz": "33"}]
    for i in values:
        record = "%s%s%s%s%s%s\n" % (i["id"], splitStr, i["addr"], splitStr,i["bz"], splitStr)
        fileObj.write(record)
    fileObj.close()


def buildUpdateData():
    # 修改数据：广东省深圳市龙岗绿景大公馆6栋3b-》广东省深圳市侨香路鸣泉居1005
    # 修改数据：广东省深圳市龙岗区南联鹏达商业街1号-》广东省深圳市凤凰路凤凰街41号螺岭外国语实验学校
    filePath = r'C:\Users\LocalUser\Desktop\client_update'
    title = "ID,ADDRESS"
    fileObj = file(filePath, 'wb')
    values = [
        {"id": '2', "addr": "广东省深圳市侨香路鸣泉居1005","bz":"2_update"},
        {"id": '3', "addr": "广东省深圳市凤凰路凤凰街41号螺岭外国语实验学校","bz":"3_update"}]
    for i in values:
        record = "%s%s%s%s%s%s\n" % (i["id"], splitStr, i["addr"], splitStr,i["bz"], splitStr)
        fileObj.write(record)
    fileObj.close()


if __name__ == '__main__':
    buildBaseData()
    buildAddData()
    buildUpdateData()
