#!/home/vagrant/script/ntom/bin/python
#from __future__ import print_function
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
    table = 'ods_adjust_log'
    key = ['remote_addr','msec','log','click_id','app_id','app_name','app_version','store','tracker','tracker_name',
'network_name','campaign','adgroup','creative','is_organic','gclid','rejection_reason','click_referer','adid','idfa',
'android_id','idfa_android_id','idfv','gps_adid','referrer','user_agent','ip_address','activity_kind','click_time','click_time_hour',
'conversion_duration','engagement_time','engagement_time_hour','install_time','install_time_hour','created_time','created_time_hour','reattributed_time','reattributed_time_hour','region',
'country','city','language','device_name','device_type','os_name','sdk_version','os_version','timezone',
'deeplink','label']
    sql = "INSERT INTO " + table + "("
    sql_value = ") VALUES("
    value = []
    for v in key:
        sql += v + ","
        sql_value += "%s,"
        value.append(d.get(v))
    sql = sql.strip(',') + sql_value.strip(',') + ')'
#    print(sql)
    try:
        with conn.cursor() as cur:
            cur.execute("SET NAMES utf8")
            cur.execute(sql, value)
        conn.commit()
    except pymysql.OperationalError as e:
        print('pymysql error: ' + str(e.args))
#    finally:
#        conn.close()

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
    print('job start!')
    wm = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wm)
    wm.watch_transient_file(path, pyinotify.IN_MODIFY, ProcessTransientFile)
    notifier.loop(daemonize=True, pid_file='/home/vagrant/script/ntom/adjust.pid', stdout='/home/vagrant/script/ntom/log/run.log', stderr='/home/vagrant/script/ntom/log/run_err.log')

if __name__ == '__main__':
    db = {
        'host':'127.0.0.1',
        'port':3306,
        'user':'root',
        'password':'',
        'db':'test',
        'charset':'utf8'
    }
    try:
        conn = pymysql.connect(**db)
    except pymysql.OperationalError as e:
        print('mysql connect error: ' + str(e.args))

    ngLog = '/data/webservice/logs/nginx/growth/adjust.log'
    file = open(ngLog, 'r')
    st_results = os.stat(ngLog)
    st_size = st_results[6]
    file.seek(st_size)
    LogMonitor(ngLog)
