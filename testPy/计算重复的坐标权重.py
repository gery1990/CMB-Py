import os, csv

filePath = r'C:\Users\LocalUser\Desktop\\targetbeijinglu_1.csv'
outPath = r'C:\Users\LocalUser\Desktop\\target_NEW.csv'

if __name__ == '__main__':
    fileObj = file(filePath, 'rb')
    csvReader = csv.reader(fileObj)

    fileObj2 = file(outPath, 'wb')
    csvWriter = csv.writer(fileObj2)

    values = {}
    for i in csvReader:
        xcoor = i[11]
        ycoor = i[12]
        key = "%s_%s" % (xcoor, ycoor)
        if key not in values:
            values[key] = 0
        values[key] += 1

    csvWriter.writerow(["x", "y", "count"])
    for key in values:
        xy = key.split("_")
        csvWriter.writerow([xy[0], xy[1], values[key]])

    fileObj.close()
    fileObj2.close()
