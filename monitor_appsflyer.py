#!/home/vagrant/script/ntom/bin/python
#from __future__ import print_function
import pyinotify
import time
import os
import sys
import pymysql
import json
import urllib.parse
import configparser

class ProcessTransientFile(pyinotify.ProcessEvent):
    def my_init(self):
        self._data = []
        self._insert_time = time.time()

    def process_IN_MODIFY(self, event):
        line = file.readline()
        if line:
#            print(line, end='')
#            print(self.parseLog(line))
            self.intodb(line)

    def intodb(self, line):
        d = self.parseLog(line)
        table = 'ods_appsflyer_log'
        key = ['remote_addr', 'msec', 'clickid', 'site_id', 'device_ip', 'idfa', 'idfv', 'advertiser_id', 'android_id', 'app_name',
               'app_id', 'install_time', 'click_time', 'wifi', 'campaign', 'country_code', 'city', 'language', 'device_brand','device_model',
               'carrier', 'appsflyer_device_id', 'sdk_version', 'app_version_name', 'app_version', 'os_version', 'user_agent', 'event_name', 'event_time', 'event_value',
               'monetary', 'currency', 'eventname', 'eventvalue']
        sql = "INSERT INTO " + table + "(" + ','.join(key) + ') VALUES(' + ('%s,' * len(key)).strip(',') + ')'
        print(sql)
        print(d)
        value = []
        for v in key:
            value.append(d.get(v))
        self._data.append(value)
        if self._insert_time != time.time() and self._insert_time + 5 * 60 <= int(time.time()) or len(self._data) >= 1:
            try:
                with conn.cursor() as cur:
                    cur.execute("SET NAMES utf8")
                    cur.executemany(sql, self._data)
                conn.commit()
            except pymysql.OperationalError as e:
                print('pymysql error: ' + str(e.args))
#            finally:
#                conn.close()
            del self._data[:]
            self._insert_time = time.time()

    def parseLog(self, line):
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
    dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    wm = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wm)
    wm.watch_transient_file(path, pyinotify.IN_MODIFY, ProcessTransientFile)
    notifier.loop(daemonize=False, pid_file=dir+'/adjust.pid', stdout=dir+'/log/run.log', stderr=dir+'/log/run_err.log')

if __name__ == '__main__':
    cf = configparser.ConfigParser()
    cf.read(os.path.abspath(os.path.dirname(__file__) + '/../default.ini'))
    db = {
        'host': cf.get('db', 'host'),
        'port': cf.getint('db', 'port'),
        'user': cf.get('db', 'user'),
        'password': cf.get('db', 'password'),
        'db': cf.get('db', 'db'),
        'charset': cf.get('db', 'charset')
    }
    ngLog = cf.get('log', 'appsflyer')
    data = []
    insert_time = time.time()
    try:
        conn = pymysql.connect(**db)
    except pymysql.OperationalError as e:
        print('mysql connect error: ' + str(e.args))

    file = open(ngLog, 'r')
    st_results = os.stat(ngLog)
    st_size = st_results[6]
    file.seek(st_size)
    LogMonitor(ngLog)
