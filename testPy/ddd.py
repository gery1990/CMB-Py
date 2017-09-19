# encoding=utf8

import os, arcpy,datetime

if __name__ == '__main__':
    print arcpy.AddFieldDelimiters(r'D:\dataHandle\GISData\gdb\crm\clientdata\clientdata.gdb\tableview', "CID")
    count=0
    sqlStr=arcpy.AddFieldDelimiters(r'C:\Users\esri\Desktop\clientdata.gdb\clientdata', "CID") + " in ("
    with arcpy.da.SearchCursor(r"C:\Users\esri\Desktop\clientdata.gdb\clientdata", ("CID",)) as cursor:
        for row in cursor:
            pass
        del cursor
    #         if count<10000:
    #             sqlStr+="'%s',"%row[0]
    #         else:
    #             break
    #         count+=1
    #     sqlStr= sqlStr[0:(len(sqlStr)-1)]+")"
    #     del cursor
    #     d2=datetime.datetime.now()
    #     print d2-d1
    # d1 = datetime.datetime.now()
    # arcpy.MakeTableView_management(r"C:\Users\esri\Desktop\clientdata.gdb\clientdata", "templayer")
    #
    # # Execute SelectLayerByAttribute to determine which rows to delete
    # arcpy.SelectLayerByAttribute_management("templayer", "NEW_SELECTION", sqlStr)
    #
    # d2 = datetime.datetime.now()
    # print int(arcpy.GetCount_management("templayer").getOutput(0))
    # print d2-d1


