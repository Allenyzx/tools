# -*- coding: GBK -*-

import datetime
import os
from multiprocessing import Process, Queue, Array, RLock
import time
import datetime as dt
"""
多进程分块读取文件
"""

# WORKERS = 4
# BLOCKSIZE = 100000000
FILE_SIZE = 0


def getFilesize(file,file_type):
    """
        获取要读取文件的大小
    """
    global FILE_SIZE
    fstream = open(file, file_type)
    fstream.seek(0, os.SEEK_END)
    FILE_SIZE = fstream.tell()
    fstream.close()


def process_found(pid, array, file, rlock,file_type,fn,log_set,log_step):
    global FILE_SIZE
    global JOB
    global PREFIX
    BLOCKSIZE = 100000000
    """
        进程处理
        Args:
            pid:进程编号
            array:进程间共享队列，用于标记各进程所读的文件块结束位置
            file:所读文件名称
        各个进程先从array中获取当前最大的值为起始位置startpossition
        结束的位置endpossition (startpossition+BLOCKSIZE) if (startpossition+BLOCKSIZE)<FILE_SIZE else FILE_SIZE
        if startpossition==FILE_SIZE则进程结束
        if startpossition==0则从0开始读取
        if startpossition!=0为防止行被block截断的情况，先读一行不处理，从下一行开始正式处理
        if 当前位置 <=endpossition 就readline
        否则越过边界，就从新查找array中的最大值
    """
    fstream = open(file,file_type)

    while True:
        rlock.acquire()
        # print('pid%s' % pid, ','.join([str(v) for v in array]))
        startpossition = max(array)
        endpossition = array[pid] = (startpossition + BLOCKSIZE) if (startpossition + BLOCKSIZE) < FILE_SIZE else FILE_SIZE
        rlock.release()

        if startpossition == FILE_SIZE:  # end of the file
            # print('pid%s end' % (pid))
            break
        elif startpossition != 0:
            fstream.seek(startpossition)
            fstream.readline()
        pos = fstream.tell()
        while pos < endpossition:
            # 处理line
            step = 1
            for i in fstream.readlines():
                fn(line=i)
                if log_set == True and step % log_step == 0:
                    print('[%s] log_step :%s' % (dt.datetime.now(), step))
                step += 1

            pos = fstream.tell()

        # print('pid:%s,startposition:%s,endposition:%s,pos:%s' % (pid, ss, pos, pos))

    fstream.close()

class Lyreader:
    """
    多进程读取文件
    """
    def __init__(self,file_path,fn,process=4,file_type='r',log=True,log_step=10000):
        self._file_path = file_path
        self._fn = fn
        self.WORKERS = process
        self._file_type = file_type
        self._log = log
        self._log_step = log_step

    def run(self):
        # starttime = time.clock()
        global FILE_SIZE

        getFilesize(self._file_path,file_type=self._file_type)
        print(FILE_SIZE)

        rlock = RLock()
        array = Array('l', self.WORKERS, lock=rlock)
        threads = []
        for i in range(self.WORKERS):
            p = Process(target=process_found, args=[i, array, self._file_path, rlock,self._file_type,self._fn,self._log,self._log_step])
            threads.append(p)

        for i in range(self.WORKERS):
            threads[i].start()

        for i in range(self.WORKERS):
            threads[i].join()


        #
        # print('speed time:')
        # print(time.clock() - starttime)


if __name__ == '__main__':
    import os,sys
    local_path = os.path.dirname(__file__)
    #文件
    fileName =local_path+'/data/groupdata.txt'
    def pprint(line):
        print(line)

    lyr = Lyreader(fileName,file_type='rb',fn=pprint)
    lyr.run()
