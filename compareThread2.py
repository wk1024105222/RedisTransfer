# -*- coding:utf-8 -*-
import sys
import threading
import os
from pybloom_live import BloomFilter
import logging

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s[%(threadName)s] %(message)s',
                filename='compare.log',
                filemode='a')
compareRlt = open('compareResult.csv', 'w')

newClusterPath = sys.argv[1]
oldClusterPath = sys.argv[2]

def compare(filterLens,filterKeys,fileName,path):
    '''
    :param filterLens: 过滤器数组 检查key+length是否存在
    :param filterKeys: 过滤器数组 检查key是否存在
    :param fileName: 待处理的旧集群节点key文件
    :param path: 文件路径
    :return:
    '''
    logging.info('%s begin compare' % (fileName))
    file = open(path+'/'+fileName, 'r')
    failedNum = 0
    notInKeyFilter = 0
    lenError = 0
    for line in file:
        tmp = line.split(',')
        oldKeyLen = '%s%s' % (tmp[0], tmp[2])
        oldKey = tmp[0];
        inKeyLen = False
        inKey = False
        for filter in filterLens:
            if oldKeyLen in filter:
                inKeyLen = True
                break
        if not inKeyLen:
            for filter in filterKeys:
                if oldKey in filter:
                    inKey = True
                    break
            if inKey:
                lenError += 1
                compareRlt.write('%s,%s,%s' % (fileName,'errorLen',line))
            else:
                notInKeyFilter +=1
                compareRlt.write('%s,%s,%s' % (fileName,'notExist', line))
    file.close()
    logging.info('%s end compare, key not in new cluster num %d，value len error num %d' % (fileName,notInKeyFilter,lenError))


def buildFilter(filterKey,filterLen, fileName, path):
    '''
    :param filterKey: 基于当前文件使用key 构建的布隆过滤器
    :param filterLen: 基于当前文件使用key+len 构建的布隆过滤器
    :param fileName: 待读取文件名 新集群的key列表
    :param path:
    :return:
    '''
    logging.info('%s begin bulid filter ' % (fileName))
    file = open(path+'/'+fileName, 'r')
    for line in file:
        tmp = line.split(',')
        # key name + length 合并作为检索值
        filterLen.add('%s%s' % (tmp[0], tmp[2]))
        filterKey.add(tmp[0])
    file.close()
    logging.info('%s end bulid filter' % (fileName))

if __name__ == '__main__':
    # 一个新集群Key文件构建一个布隆过滤器
    logging.info('===================================begin new compare task===================================')
    logging.info('base new cluster key files begin build filter')
    buildFilterThreads = []
    filterKeys = []
    filterLens = []

    for filename in os.listdir(newClusterPath):
        if not filename.endswith('csv'):
            continue
        filterKey = BloomFilter(capacity=1000*10000, error_rate=0.00001)
        filterLen = BloomFilter(capacity=1000 * 10000, error_rate=0.00001)
        filterKeys.append(filterKey)
        filterLens.append(filterLen)
        logging.info('base %s build BloomFilter cost memory %dM' % (filename,len(filterKey.bitarray*2) / 8 / 1024 / 1024))
        t = threading.Thread(target=buildFilter, args=(filterKeys[-1],filterLens[-1],filename, newClusterPath))
        buildFilterThreads.append(t)
        t.start();
        # break

    for a in buildFilterThreads:
        a.join()
    logging.info('base new cluster key files end build filter')

    # 一个旧集群key文件 开一个线程对比
    compareThreads = []
    logging.info('base old cluster key file begin compare')
    for filename in os.listdir(oldClusterPath):
        if not filename.endswith('csv'):
            continue

        t = threading.Thread(target=compare, args=(filterLens,filterKeys,filename, oldClusterPath))
        compareThreads.append(t)
        t.start();

    for a in compareThreads:
        a.join()
    compareRlt.close()
    logging.info('base old cluster key file end compare')

# 四个线程读四个文件800W/个构建四个过滤器 耗时13min
# 四个线程读四个文件比较结果 耗时11minß
# base new cluster key files begin build filter 2019-11-07 10:07:07
# base new cluster key files end build filter 2019-11-07 10:20:12
# base old cluster key file begin compare 2019-11-07 10:20:12
# base old cluster key file end fill BloomFilter 2019-11-07 10:31:13


# 读一个文件创建一个过滤器800W 耗时3min  读四个文件对比结果耗时10min
# base new cluster key files begin build filter 2019-11-07 10:58:02
# base old cluster key file end fill BloomFilter 2019-11-07 11:11:37

# 读一个文件 创建2个过滤器 耗时9min
# base new cluster key files begin build filter 2019-11-09 10:48:54
# base new cluster key files end build filter 2019-11-09 10:58:00

# 四个线程读四个文件800W/个构建8个过滤器 耗时30min
# base new cluster key files begin build filter 2019-11-09 11:07:04
# base new cluster key files end build filter 2019-11-09 11:37:31

