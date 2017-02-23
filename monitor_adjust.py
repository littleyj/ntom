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
        self._conn = None
        self._init_db()

    def _init_db(self):
        try:
            self._conn = pymysql.connect(**db)
        except pymysql.OperationalError as e:
            print('mysql connect error: ' + str(e.args))

    def _ping(self):
        try:
            cursor = self._conn.cursor()
            cursor.execute('select 1;')
            return True
        except pymysql.OperationalError as e:
            return False

    def process_IN_MODIFY(self, event):
        line = file.readline()
        if line:
#            print(line, end='')
#            print(self.parseLog(line))
            self.intodb(line)

    def intodb(self, line):
        d = self.parseLog(line)
        table = 'ods_adjust_log'
        key = ['remote_addr','msec','log','click_id','app_id','app_name','app_version','store','tracker','tracker_name',
            'network_name','campaign','adgroup','creative','is_organic','gclid','rejection_reason','click_referer','adid','idfa',
            'android_id','idfa_android_id','idfv','gps_adid','referrer','user_agent','ip_address','activity_kind','click_time','click_time_hour',
            'conversion_duration','engagement_time','engagement_time_hour','install_time','install_time_hour','created_time','created_time_hour','reattributed_time','reattributed_time_hour','region',
            'country','city','language','device_name','device_type','os_name','sdk_version','os_version','timezone',
            'deeplink','label']
        sql = "INSERT INTO " + table + "(" + ','.join(key) + ') VALUES(' + ('%s,'*len(key)).strip(',') + ')'
#        print(sql)
        print(d)
        value = []
        for v in key:
            value.append(d.get(v))
        self._data.append(value)
        if self._insert_time != time.time() and self._insert_time + 5*60 <= int(time.time()) or len(self._data) >= 1:
            if not self._ping():
                self._init_db()
            try:
                with self._conn.cursor() as cur:
                    cur.execute("SET NAMES utf8")
                    cur.executemany(sql, self._data)
                self._conn.commit()
            except pymysql.OperationalError as e:
                print('pymysql error: ' + str(e.args))
#            finally:
#                self._conn.close()
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
    print('adjust job start!')
    dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    wm = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wm)
    wm.watch_transient_file(path, pyinotify.IN_MODIFY, ProcessTransientFile)
    notifier.loop(daemonize=True, pid_file=dir+'/adjust.pid', stdout=dir+'/log/run.log', stderr=dir+'/log/run_err.log')

if __name__ == '__main__':
    cf = configparser.ConfigParser()
    cf.read(os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + '/../default.ini'))
    db = {
        'host':cf.get('db', 'host'),
        'port':cf.getint('db', 'port'),
        'user':cf.get('db', 'user'),
        'password':cf.get('db', 'password'),
        'db':cf.get('db', 'db'),
        'charset':cf.get('db', 'charset')
    }
    ngLog = cf.get('log', 'adjust')

    file = open(ngLog, 'r')
    st_results = os.stat(ngLog)
    st_size = st_results[6]
    file.seek(st_size)
    LogMonitor(ngLog)
