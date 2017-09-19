# encoding=UTF8
# 日志操作
import logging
import time, os, traceback


class LOGOP:
    def __init__(self, filename, logPath):
        global nowDate
        try:
            self.filename = filename
            nowDate = time.strftime("%Y%m%d", time.localtime())
            if os.path.exists(logPath) is not True:
                os.makedirs(logPath)
            # 设置日志配置,自动创建日志文件
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S',
                                filename=os.path.join(logPath, self.filename + '_' + nowDate + '.log'),
                                filemode='w')
        except:
            traceback.print_exc()

    def warning(self, warnInfo):
        # 标准：日期，错误信息，报错位置
        print warnInfo
        logging.warning(warnInfo)

    def info(self, infoMsg):
        print infoMsg
        logging.info(infoMsg)


if __name__ == '__main__':
    logop = LOGOP('www', r"E:\python_project\CMB-Py\dataManage\workspace\processLog\20170704")
    logop.warning('wewrwr')
