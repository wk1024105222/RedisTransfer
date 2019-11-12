# -*- coding:utf-8 -*-
import random
import string
import threading


def createFile(oldName,newName):
    old = open ( oldName, 'w' )
    new = open ( newName, 'w' )
    num = 0
    for i in range(0,8000000):
        key = ''.join(random.sample(string.ascii_letters + string.digits, 40))
        len = random.randint(1,500)
        old.write ( '%s,string,%d,%d\n' % (key,len,random.randint(1,7*24*3600)))
        if i % 10000 != 0:
            new.write('%s,%d\n' % (key,len))
        elif i % 10000 == 9999 :
            new.write('%s,%d\n' % (key,len+1))

        print(i)
    old.close()
    new.close()

if __name__ == '__main__':
    createThreads = []

    for i in range(1,2):
        t = threading.Thread(target=createFile, args=('%d.%d.%d.%d_%s.csv' % (i,i,i,i,'old'),'%d.%d.%d.%d_%s.csvß' % (i,i,i,i,'new')))
        createThreads.append(t)
        t.start();
    for a in createThreads:
        a.join()



