#!/home/vagrant/script/ntom/bin/python
from __future__ import print_function
import pyinotify
import time
import os
import sys
import pymysql
import json
import urllib.parse

class ProcessTransientFile(pyinotify.ProcessEvent):
    def process_IN_MODIFY(self,event):
        line = file.readline()
        if line:
#            print(line, end='')
#            print(parseLog(line))
            intodb(line)

def intodb(line):
    d = parseLog(line)
    try:
        with conn.cursor() as cur:
            cur.execute("SET NAMES utf8")
            sql = "INSERT INTO appfly_channel(remote_addr,mesc,abc,dff) VALUES(%s,%s,%s,%s)"
            cur.execute(sql, (d['remote_addr'], d['msec'], d.get('abc'), d.get('dff')))
        conn.commit()
    finally:
        conn.close()

def parseLog(line):
    list = line.strip('\n').split('^A^R')[1:]
    m = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(list[2][0:10])))
    if list[1] == '{}':
        d = dict(remote_addr=list[0], mesc=m)
    else:
        j = json.loads(urllib.parse.unquote(list[1]))
        d = dict(remote_addr=list[0], msec=m, **j)
    return d 

def LogMonitor(path):
    wm = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wm)
    wm.watch_transient_file(path, pyinotify.IN_MODIFY, ProcessTransientFile)
    notifier.loop()

if __name__ == '__main__':
    db = {
        'host':'127.0.0.1',
        'port':3306,
        'user':'root',
        'password':'',
        'db':'test',
        'charset':'utf8'
    }
    conn = pymysql.connect(**db)

    ngLog = '/data/webservice/logs/nginx/growth/af_ad.log'
    file = open(ngLog, 'r')
    st_results = os.stat(ngLog)
    st_size = st_results[6]
    file.seek(st_size)
    LogMonitor(ngLog)
