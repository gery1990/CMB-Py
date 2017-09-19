# encoding=utf8
import arcpy, os, traceback, codecs

FIELD_DEL = chr(0X7C) + chr(0X1C)  # 标准文件字段分隔符


# 设置工作空间
def setWorkSpace(path):
    arcpy.env.workspace = path


def createLayer(workspace, layerName, projFile, featureType, fieldsMap):
    sr = arcpy.SpatialReference(projFile)
    arcpy.CreateFeatureclass_management(workspace, layerName, featureType, spatial_reference=sr)
    for f in fieldsMap:
        arcpy.AddField_management(os.path.join(workspace, layerName), f, fieldsMap[f], "", "", "", f, "NULLABLE")


# 创建GDB文件
def createFileGDB(localWorkspace, name):
    if os.path.exists(os.path.join(localWorkspace, name)) is not True:
        arcpy.CreateFileGDB_management(localWorkspace, name)


# 复制图层
def copyLayers(source, target, input_type=None):
    try:
        arcpy.Copy_management(source, target)
    except:
        print traceback.print_exc()


def getInsertCursor(layerPath, fields):
    return arcpy.da.InsertCursor(layerPath, fields)


# 创建点要素
def createPoint(longitude, latitude):
    return arcpy.Point(longitude, latitude)


def createPointGeometry(point, sr):
    return arcpy.PointGeometry(point, sr)


# 创建面要素
def createPolygon(points):
    return arcpy.Polygon(arcpy.Array([arcpy.Point(*coords) for coords in points]))


# 获取工作空间的图层列表
def getFeaturesList(wild_card=None, feature_type=None, feature_dataset=None):
    # 需要先设置工作空间
    return arcpy.ListFeatureClasses(wild_card, feature_type, feature_dataset)


# 获取文件夹内所有GDB的图层
def listFolderGDBLayers(folderPath, logging):
    try:
        layers = []
        for f in os.listdir(folderPath):
            if ".gdb" in f:
                setWorkSpace(os.path.join(folderPath, f))
                for fc in getFeaturesList():
                    layers.append(os.path.join(folderPath, f, fc))
        return layers
    except:
        logging.warning(traceback.format_exc())


def getLayerFieldValues(layerPath, fields):
    values = []
    with arcpy.da.SearchCursor(layerPath, fields) as cursor:
        for row in cursor:
            v = {}
            for i in xrange(len(fields)):
                v[fields[i]] = row[i]
            values.append(v)
        del cursor
    return values


def getSpatialReference(sr):
    return arcpy.SpatialReference(sr)


def is_num_by_except(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


# 插入要素
def insertRow_Point(layerPath, sourceFile, x, y, csvFields, fileEncode, convertSr, logging):
    xmax = 0
    ymax = 0
    xmin = 0
    ymin = 0
    try:
        fields = []
        sourceObj = file(sourceFile, 'rb')
        arcpy.env.workspace = os.path.split(layerPath)[0]
        layerFields = getLayerFields(layerPath)
        for field in layerFields:
            fields.append(field["name"])
        insertCursor = getInsertCursor(layerPath, fields)
        inSr, outSr = None, None
        if convertSr.upper() != "NONE":
            v = convertSr.split(',')
            inSr = arcpy.SpatialReference(int(v[0]))
            outSr = arcpy.SpatialReference(int(v[1]))
        try:
            while True:
                lineStr = sourceObj.next()
                values = lineStr.split(FIELD_DEL)
                try:
                    if str(values[x]) != '' and str(values[y]) != "" and str(values[x]) != "0" and str(
                            values[y]) != "0" and is_num_by_except(values[x]) and is_num_by_except(values[y]):

                        geo = createPoint(float(values[x]), float(values[y]))
                        geoX, geoY = 0, 0
                        if convertSr.upper() != 'NONE':
                            p = arcpy.PointGeometry(geo, inSr)
                            geo = p.projectAs(outSr)
                            geoX, geoY = geo.firstPoint.X, geo.firstPoint.Y
                        else:
                            geoX, geoY = geo.X, geo.Y

                        if xmax == 0 or xmax < geoX: xmax = geoX
                        if xmin == 0 or xmin > geoX: xmin = geoX
                        if ymax == 0 or ymax < geoY: ymax = geoY
                        if ymin == 0 or ymin > geoY: ymin = geoY

                        insertValues = []
                        for field in layerFields:
                            if field["name"] == "SHAPE@":
                                insertValues.append(geo)
                            elif field["name"] == "X":
                                insertValues.append(geoX)
                            elif field["name"] == "Y":
                                insertValues.append(geoY)
                            else:
                                if field["name"].upper() in csvFields:
                                    fIndex = csvFields.index(field["name"].upper())
                                    if field["type"] == "String" and fileEncode != "":
                                        if values[fIndex] != "":
                                            insertValues.append(values[fIndex].decode(fileEncode))
                                        else:
                                            insertValues.append('')
                                    else:
                                        insertValues.append(values[fIndex])
                                else:
                                    if field["type"] == "String":
                                        insertValues.append('')
                                    elif field["type"] == "Double" or field["type"] == "Float" or field[
                                        "type"] == "Integer":
                                        insertValues.append(0)
                        insertCursor.insertRow(insertValues)
                    else:
                        logging.warning(u"insertRow_Point:lost recode info")
                except:
                    logging.warning(traceback.format_exc())
        except StopIteration:
            pass
        except Exception as e:
            logging.warning(u"insertRow_Point:analysis file failed-%s ！" % sourceFile)
            logging.warning(traceback.format_exc())
            logging.warning(e.message)
            raise
        sourceObj.close()
        del insertCursor
        return [xmax, ymax, xmin, ymin]
    except:
        logging.warning(traceback.format_exc())
        return []


def insertExtentFeature(layerPath, layerName, index, xmax, xmin, ymax, ymin, logging):
    try:
        fields = ["LAYERNAME", "INDEX", "XMAX", "XMIN", "YMAX", "YMIN", "SHAPE@"]
        inserCursor = getInsertCursor(layerPath, fields)

        geo = createPolygon([[xmin, ymin], [xmin, ymax], [xmax, ymax], [xmax, ymin], [xmin, ymin]])

        inserCursor.insertRow([layerName, index, xmax, xmin, ymax, ymin, geo])
        del inserCursor
    except:
        logging.warning(traceback.format_exc())
        return False


def getLayerFields(layerPath):
    fieldsObj = arcpy.ListFields(layerPath)
    fields = []
    for item in fieldsObj:
        if item.name.upper() == "SHAPE":
            fields.append({"name": item.name.upper() + "@", "type": item.type})
        elif item.name.upper() == 'OBJECTID':
            pass
        else:
            fields.append({"name": item.name, "type": item.type})
    return fields


# 删除旧要素
def deleteRow(layerPath, sourceFile, uniqueId, uniqueFieldName):
    sourceObj = file(sourceFile, 'rb')
    idList = []
    try:
        while True:
            lineStr = sourceObj.next()
            values = lineStr.split(FIELD_DEL)
            idList.append(values[uniqueId])
    except StopIteration:
        pass
    sourceObj.close()

    with arcpy.da.UpdateCursor(layerPath, (uniqueFieldName,)) as cursor:
        for row in cursor:
            if row[0] in idList:
                cursor.deleteRow()
        del cursor


def searchData(layerPath, field_names, where_clause=None):
    return arcpy.da.SearchCursor(layerPath, field_names, where_clause)


def appendLayers(layers, output):
    # 追加要素到目标图层
    arcpy.Append_management(layers, output, 'TEST')


def exists(layerName):
    arcpy.Exists(layerName)


def deleteLayer(layerName, data_type=None):
    arcpy.Delete_management(layerName, data_type)


def deleteFeatures(layerPath):
    arcpy.DeleteFeatures_management(layerPath)


def splitLayer(in_features, split_features, split_field, out_workspace):
    arcpy.Split_analysis(in_features, split_features, split_field, out_workspace)


def addFieldDelimiters(datasource, field):
    return arcpy.AddFieldDelimiters(datasource, field)


def makeTableView(tableSource, tableName):
    arcpy.MakeTableView_management(tableSource, tableName)


def makeFeatureLayer(layerPath, layerName):
    arcpy.MakeFeatureLayer_management(layerPath, layerName)


def selectLayerByAttribute(layerName, selection_type, sqlStr=None):
    arcpy.SelectLayerByAttribute_management(layerName, selection_type, sqlStr)


def copyRows(in_rows, out_table):
    arcpy.CopyRows_management(in_rows, out_table)


def sortLayer(in_dataset, out_dataset, sort_field, spatial_sort_method=None):
    arcpy.Sort_management(in_dataset, out_dataset, sort_field, spatial_sort_method)


def deleteRows(in_rows):
    arcpy.DeleteRows_management(in_rows)


if __name__ == '__main__':
    try:
        values = getLayerFields(r'C:\Users\esri\Desktop\clientdata.gdb\clientdata_1')
    except:
        traceback.print_exc()
