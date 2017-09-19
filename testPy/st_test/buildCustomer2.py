import csv, os, arcpy

if __name__ == "__main__":
    fPath = r'C:\Users\LocalUser\Desktop\bank_geocode_result.csv'
    layerPath = r'D:\branch.gdb'

    csvReader = csv.reader(file(fPath, 'rb'))

    insertCursor = arcpy.da.InsertCursor(os.path.join(layerPath, "branch"),
                                         ["ID", "NAME", "ADDRESS", "X", "Y" ,"SCORE","SHAPE@"])
    for i in csvReader:
        p = arcpy.Point(float(i[3]), float(i[4]))
        values = [i[0], i[1], i[2], i[3], i[4], i[5], p]
        insertCursor.insertRow(values)
    del insertCursor