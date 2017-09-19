import arcpy
import os

arcpy.env.workspace = r'D:\dataHandle\GISData\gdb\crm\clientdata\tempdata.gdb'
insertTable = arcpy.da.InsertCursor(r'D:\dataHandle\GISData\gdb\crm\clientdata\tempdata.gdb\tableview',["CID","NAME"])
count=0
for fc in arcpy.ListFeatureClasses(feature_type="Point"):
    with arcpy.da.SearchCursor(fc, ["CID"]) as cursor:
        for row in cursor:
            insertTable.insertRow([row[0], fc])
            count+=1
            if count%10000==0:
                print count,fc
del insertTable
