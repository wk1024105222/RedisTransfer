# -*- coding:utf-8 -*-
import sys
import threading
import time
from queue import Queue
import os
from pybloom_live import BloomFilter

path = sys.argv[1]

def readFile(array,fileName):
    '''
    :param array: 将文件内容解析数组返回
    :param fileName: 待解析文件名
    :return:
    '''
    print('%s begin read %s' % (fileName,time.strftime("%Y-%m-%d_%H-%M-%S",time.localtime())))
    file = open(path+'/'+fileName, 'r')
    for line in file:
        array.append(line)
    file.close()
    print('%s end read %s' % (fileName, time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())))


if __name__ == '__main__':
    readThreads = []
    newKeys = []
    for filename in os.listdir(path):
        if not filename.endswith('csv'):
            continue
        newKeys.append([])
        t = threading.Thread(target=readFile, args=(newKeys[-1],filename))
        readThreads.append(t)
        t.start();

    for a in readThreads:
        a.join()
    print('all fiels end read %s' % (time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())))

    filter = BloomFilter(capacity=10000000 * 28, error_rate=0.00001)
    print('current BloomFilter cost memory %dM' % (len(filter.bitarray) / 8 / 1024 / 1024))

    print('begin fill BloomFilter %s' % (time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())))
    for b in newKeys:
        print('array size %d' % (len(b)))
        for c in b:
            line = c.split(',')
            filter.add('%s%s' % (line[0],line[2]))
    print('ens fill BloomFilter %s' % (time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())))

# 多线程读文件内容到N个数组  遍历搜有数据构建一个过滤器 800W*4 的数据 创建过滤器耗时16分钟
# 2.1.1.1_2001.csv begin read 2019-11-07_10-34-35
# 2.1.1.1_2000.csv begin read 2019-11-07_10-34-35
# 2.1.1.1_2002.csv begin read 2019-11-07_10-34-35
# 2.1.1.1_2003.csv begin read 2019-11-07_10-34-35
# 2.1.1.1_2003.csv end read 2019-11-07_10-34-50
# 2.1.1.1_2001.csv end read 2019-11-07_10-34-50
# 2.1.1.1_2000.csv end read 2019-11-07_10-34-50
# 2.1.1.1_2002.csv end read 2019-11-07_10-34-50
# all fiels end read 2019-11-07_10-34-50
# current BloomFilter cost memory 799M
# begin fill BloomFilter 2019-11-07_10-34-51
# array size 7999960
# array size 7999960
# array size 7999960
# array size 7999960
# ens fill BloomFilter 2019-11-07_10-50-05

