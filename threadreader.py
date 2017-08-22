# -*- coding: utf-8 -*-
import os,time
import datetime as dt
import threading
import pandas as pd

_author_ = 'allen'
_date_ = '2017-08-22'
_describe_ = '多线程读取文件工具'

rlock = threading.RLock()
cur = 0




class Lyreader:
    """
    多线程读取文件
    """
    def __init__(self,file_path,fn,threadnum=4,file_type='r',log=True,log_step=10000):
        self._file_path = file_path
        self._fn = fn
        self._threadnum = threadnum
        self._file_type = file_type
        self._log = log
        self._log_step = log_step

    def run(self):
        starttime = time.clock()

        res = Resource(self._file_path,file_type=self._file_type)
        threads = []
        #初始化线程
        for i in range(self._threadnum):
            rdr = Reader(res,self._fn,file_type=self._file_type)
            threads.append(rdr)
        #开始线程
        for i in range(self._threadnum):
            threads[i].start()
        #结束线程
        for i in range(self._threadnum):
            threads[i].join()
        print('speed time:')
        print(time.clock() - starttime)


    # 存储csv
    @staticmethod
    def store_csv(df, csv_path=''):
        def _save_csv(pdoutput, csv_path):
            if os.path.exists(csv_path) is False:
                return pdoutput.to_csv(csv_path, index=False, encoding='GBK')
            else:
                return pdoutput.to_csv(csv_path, index=False, encoding='GBK', header=False, mode='a')

        if csv_path != '':
           return _save_csv(pdoutput=df, csv_path=csv_path)
        else:
            local_path = os.path.dirname(__file__)
            return _save_csv(pdoutput=df, csv_path=local_path + '/_tmp_store.csv')


class Resource(object):
    def __init__(self, fileName,file_type='r'):
        self.fileName = fileName
        self.file_type = file_type
        self.blockSize = 100000000
        self.getFileSize()
    def getFileSize(self):
        fstream = open(self.fileName, self.file_type)
        fstream.seek(0, os.SEEK_END)
        self.fileSize = fstream.tell()
        fstream.close()


class Reader(threading.Thread):
    def __init__(self,res,fn,file_type='r',log=True,log_step=100000):
        self.res = res
        self.fn = fn
        self.file_type = file_type
        self.log_set = log
        self.log_step = log_step
        super(Reader, self).__init__()
    def run(self):
        global cur
        fstream = open(self.res.fileName, self.file_type)
        while True:
            #锁定共享资源
            rlock.acquire()
            start = cur
            cur = endPosition = (start + self.res.blockSize) if (start + self.res.blockSize) < self.res.fileSize else self.res.fileSize
            #释放共享资源
            rlock.release()
            if start== self.res.fileSize:
                break
            elif start != 0:
                fstream.seek(start)
                fstream.readline()
            pos = fstream.tell()
            step = 1
            while pos < endPosition:
                ##处理line
                for i in fstream.readlines():
                    self.fn(line=i)
                    if self.log_set == True and step %self.log_step == 0:
                        print('[%s] log_step :%s'%(dt.datetime.now(),step))
                    step+=1
                pos = fstream.tell()
        fstream.close()





if __name__ == '__main__':
    import os,sys
    local_path = os.path.dirname(__file__)
    #文件
    fileName =local_path+'/data/groupdata.txt'
    def pprint(line):
        print(line)

    lyr = Lyreader(fileName,file_type='rb',fn=pprint)
    lyr.run()
