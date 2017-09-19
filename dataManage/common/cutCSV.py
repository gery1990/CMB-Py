# encoding=utf8

# 将大文件切割为多个小的文件
import os, logging, traceback, time

logger = logging.getLogger('main')


class CutFile:
    def __init__(self, processCount, rCount=0, maxCount=4000000, minCount=1000):
        logger.info('cutCSV ** begin cut file......')
        self.maxCount = maxCount
        self.minCount = minCount
        self.cpuNum = processCount
        self.rowcount = rCount

    def calcRowCount(self, sourcefile):
        fileObj = file(sourcefile, 'rU')
        count = 0
        try:
            while True:
                fileObj.next()
                count += 1
        except StopIteration:
            fileObj.close()
            pass
        return count

    def calcAvgCount(self, count):
        avgCount = count / self.cpuNum
        if avgCount < self.maxCount:
            return avgCount
        else:
            return self.calcAvgCount(avgCount)

    def do(self, sourceFilePath, outFilePath, startWith=1):
        try:
            if os.path.exists(outFilePath) is not True: os.makedirs(outFilePath)
            totalCount = 0
            rowCount = 0
            fileCount = startWith
            if self.rowcount == 0:
                self.rowcount = self.calcRowCount(sourceFilePath)
            if self.rowcount > 0:
                csvFile = file(sourceFilePath, "rU")
                fileName = os.path.basename(sourceFilePath).split('.')[0]  # 只获取文件名，不要后缀
                avgCount = 0
                if self.rowcount < self.minCount:
                    avgCount = self.minCount
                elif self.cpuNum == 1:
                    avgCount = self.maxCount
                else:
                    avgCount = self.calcAvgCount(self.rowcount)
                cutFileName = os.path.join(outFilePath, '%s_%s' % (fileName, str(fileCount)))
                cutFileObj = file(cutFileName, 'wb')
                try:
                    while True:
                        row = csvFile.next()
                        if rowCount != 0 and rowCount % avgCount == 0:
                            cutFileObj.close()
                            fileCount += 1
                            rowCount = 0
                            cutFileName = os.path.join(outFilePath, '%s_%s' % (fileName, str(fileCount)))
                            cutFileObj = file(cutFileName, 'wb')
                        rowCount += 1
                        cutFileObj.write(row)
                        totalCount += 1
                except:
                    csvFile.close()
                    pass
                cutFileObj.close()
                if totalCount == 0:
                    os.remove(cutFileName)
                logger.info('cutCSV ** cut success, total:%d' % fileCount)
                return fileCount
            return 0
        except Exception as e:
            logger.info('cutCSV ** cut file error ：' + e.message)
            traceback.print_exc()

    def mergeResultLog(self, sourceFilePath, outFilePath):
        try:
            timeStr = time.strftime("%Y%m%d%S", time.localtime())
            successFileObj = file(os.path.join(outFilePath, 'geocode_success_%s.csv' % timeStr), 'wb')
            errorFileObj = file(os.path.join(outFilePath, 'geocode_error_%s.csv' % timeStr), 'wb')
            for f in os.listdir(sourceFilePath):
                if os.path.isfile(os.path.join(sourceFilePath, f)):
                    reader = file(os.path.join(sourceFilePath, f), "rb")
                    if f.find('success') > 0:
                        try:
                            while True:
                                row = reader.next()
                                successFileObj.write(row)
                        except:
                            pass
                    elif f.find('error') > 0:
                        try:
                            while True:
                                row = reader.next()
                                errorFileObj.write(row)
                        except:
                            pass
                    else:
                        continue
            return successFileObj.name
        except Exception as e:
            logger.info("cutCSV ** merge file error：" + e.message)
            return ''


if __name__ == '__main__':
    cutObj = CutFile(2)
    cutObj.do(r'C:\Users\LocalUser\Desktop\crm-data-test-500w', r'C:\Users\LocalUser\Desktop\testCut')
