# coding:utf-8
import arcpy, traceback, os
import xml.dom.minidom as DOM


class PublishServer():
    def __init__(self, mxdTempPath, agspath, type, logger):
        '''
        :param mxdTempPath: 模板路径
        :param agspath: GIS服务连接文件
        '''
        self.mxdTempPath = mxdTempPath
        self.agspath = agspath
        self.logger = logger
        self.type = type
        self.logger.info('''mxdTempPath: %s
                            agsPath: %s
                            type: %s ''' % (mxdTempPath, agspath, type))

    def buildMxd(self, mxdOutputPath, layers):
        '''
        :param mxdOutputPath: mxd输出路径
        :param layers: 图层路径数组
        :return:
        '''
        try:
            mxd = arcpy.mapping.MapDocument(self.mxdTempPath)
            df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
            for layer in layers:
                layerName = os.path.basename(layer)
                arcpy.MakeFeatureLayer_management(layer, layerName)
                mapLayer = arcpy.mapping.Layer(layerName)
                arcpy.mapping.AddLayer(df, mapLayer, "BOTTOM")

            mxd.saveACopy(mxdOutputPath)
            return mxdOutputPath
        except:
            self.logger.warning(traceback.format_exc())

    # 创建服务定义文档
    def publishMXD(self, mxdPath, definitionType=None, maxRecordCount=None, maxInstances=None):
        '''
        :param mxdPath: 地图文档路径
        :param definitionType: esriServiceDefinitionType_Replacement 覆盖更新
        :param maxRecordCount: 最大返回数
        :param maxInstances: 最大实例数
        :return:
        '''
        try:
            new_mxd = arcpy.mapping.MapDocument(mxdPath)
            mxdName = os.path.basename(mxdPath)
            dotIndex = mxdName.index('.')
            serviceName = mxdName[0:dotIndex]

            sddraft = os.path.abspath(serviceName + '.sddraft')
            sd = os.path.abspath(serviceName + '.sd')
            if os.path.exists(sd):
                os.remove(sd)
            # 创建服务定义草稿draft
            analysis = arcpy.mapping.CreateMapSDDraft(new_mxd, sddraft, serviceName, 'ARCGIS_SERVER',
                                                      self.agspath, False, self.type, None, None)

            if analysis['errors'] == {}:
                self.editSddraft(sddraft, definitionType, maxRecordCount, maxInstances)
                # Execute StageService
                arcpy.StageService_server(sddraft, sd)
                # Execute UploadServiceDefinition
                arcpy.UploadServiceDefinition_server(sd, self.agspath)
            else:
                # if the sddraft analysis contained errors, display them
                print analysis['errors']
        except:
            self.logger.warning(traceback.format_exc())

    def editSddraft(self, xml, definitionType, maxRecordCount, maxInstances):
        try:
            doc = DOM.parse(xml)

            if definitionType != None:
                descriptions = doc.getElementsByTagName("Type")
                desc = descriptions[0]
                if desc.parentNode.tagName == "SVCManifest":
                    if desc.hasChildNodes():
                        desc.firstChild.data = definitionType

            if maxRecordCount != None or maxInstances != None:
                descriptions = doc.getElementsByTagName("Configurations")
                desc = descriptions[0]
                if desc.parentNode.tagName == "SVCManifest":
                    for configuration in desc.childNodes:
                        if configuration.tagName == 'SVCConfiguration':
                            for svgConfig in configuration.childNodes:
                                if svgConfig.tagName == 'Definition':
                                    for definition in svgConfig.childNodes:
                                        if definition.tagName == 'ConfigurationProperties':
                                            for configPro in definition.childNodes:
                                                if configPro.tagName == "PropertyArray":
                                                    for propertyArray in configPro.childNodes:
                                                        if propertyArray.childNodes[
                                                            0].firstChild.nodeValue == "maxRecordCount" and maxRecordCount != None:
                                                            if propertyArray.childNodes[1].hasChildNodes():
                                                                propertyArray.childNodes[
                                                                    1].firstChild.data = maxRecordCount
                                                                break
                                        if definition.tagName == 'Props':
                                            for pros in definition.childNodes:
                                                if pros.tagName == "PropertyArray":
                                                    for propertyArray in pros.childNodes:
                                                        if propertyArray.childNodes[
                                                            0].firstChild.nodeValue == "MaxInstances" and maxInstances != None:
                                                            if propertyArray.childNodes[1].hasChildNodes():
                                                                propertyArray.childNodes[
                                                                    1].firstChild.data = maxInstances
                                                                break

            outXml = xml
            f = open(outXml, 'w')
            doc.writexml(f)
            f.close()
        except:
            self.logger.warning(traceback.format_exc())


if __name__ == '__main__':
    # publishS = PublishServer('/home/arcgis/dataTest/template.mxd',r'/home/arcgis/dataTest/arcgis on 192.168.119.134_6080 (admin).ags')
    # mxdPath=publishS.buildMxd(r'/home/arcgis/dataTest',r'/home/arcgis/dataTest/iseeIVlayers.gdb/fishnet_wgs84')
    # publishS.publishMXD(mxdPath,True)
    # publishS = PublishServer(r'/home/arcgis/dataManage/workspace/template.mxd',
    #                          r'/home/arcgis/dataManage/workspace/connectTo99.12.100.ags',None)
    # mxdPath = publishS.buildMxd(r'/data/dataHandle/GISData/arcgisserver/gdb/sales/customertest/test.mxd', [r'/data/dataHandle/GISData/arcgisserver/gdb/sales/customertest/customertest1.gdb/customertest_1'])
    # publishS.publishMXD(mxdPath, definitionType="esriServiceDefinitionType_Replacement", maxInstances=20,
    #                     maxRecordCount=400000)
    publishS = PublishServer(r'C:\Users\LocalUser\Desktop\template.mxd',
                             r'C:\Users\LocalUser\Desktop\arcgis on 99.12.95.181 (系统管理员).ags', None)
    mxdPath = publishS.buildMxd(r'C:\Users\LocalUser\Desktop\test.mxd', [
        r'D:\branch.gdb\branch'])
    publishS.publishMXD(mxdPath, definitionType="esriServiceDefinitionType_Replacement", maxInstances=20,
                        maxRecordCount=400000)
