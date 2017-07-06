#-*-coding:utf-8-*-


import csv
import codecs
import os

# import third-party modules
from gevent import monkey;monkey.patch_all()
import gevent
# from gevent.pool import Pool
from gevent.queue import Queue

# import my own modules
from tools import *


class Extractor(object):
    def __init__(self,fname,old_dir,goal_dir):
        self.fname = fname
        self.old_dir = old_dir
        self.goal_dir = goal_dir
        self.new_path = os.path.join(self.goal_dir,fname)
    
    #@profile
    def read(self):
        """
        Read a CSV file and parse it into dicts
        
        
        Returns
        -------
        data_list: parsed dict data list
        """
        
        with codecs.open(os.path.join(self.old_dir,self.fname),'rb') as f:
            reader = csv.DictReader(f)
            data_list = []
            for data in reader:
                data_list.append(data)
        return data_list

    #@profile
    def write(self,data_list):
        fieldnames = ['goodsID','catID','keyword','params','date','time','price','price_ave','monthly_sales','comments','pageNum','order']
        
        # if no file, create file and write header.
        if not os.path.exists(self.new_path):
            with codecs.open(self.new_path,'wb') as f:
                writer = csv.DictWriter(f,fieldnames=fieldnames)
                writer.writeheader()
                
        # write data
        with codecs.open(self.new_path,'ab') as f:
            writer = csv.DictWriter(f,fieldnames=fieldnames)
            for data in data_list:
                data = {key:value for key,value in data.items() if key in fieldnames}
                writer.writerow(data)
        
        with codecs.open('extracted','ab') as f:
            f.write(self.fname.encode('utf-8'))
            f.write('\n')
    
    #@profile
    def start(self):
        try:
            data_list = self.read()
            self.write(data_list)
        except Exception,e:
            print('File Name: %s; Error: %s'%(self.fname,e))


class GeventQueue(object):
    def __init__(self,old_path,new_path,gevent_num=100):
        self.old_path = old_path
        self.new_path = new_path
        self.gevent_num = gevent_num
        self.tasks = Queue()
    
    def worker(self,i):
        print('worker %d start'%(i))
        while not self.tasks.empty():
            try:
                # print('worker %d start'%(i))    
                task = self.tasks.get()
    
                # write current task into worker's running log
                f = open(os.path.join('logfile_extractor','%s_running.log'%(i)),'wb')
                f.write(task.encode('utf-8'))
                f.close()
                
                # run the task
                Extractor(task,self.old_path,self.new_path).start()
                
                # write finished work
                fs = open(os.path.join('extracted'),'ab')
                fs.write(task.encode('utf-8'))
                fs.write('\n')
                fs.close()
                gevent.sleep(0)
        
            except Exception,e:
                print(e)
                # write error work into error log
                print('error:%d; task:%s'%(i,task))
                fl = open(os.path.join('logfile','error.log'),'ab')
                fl.write(task.encode('utf-8'))
                fl.write('\n')
                fl.close()
                continue

    def manager(self):
        extracted = record_list('extracted')

        for fname in os.listdir(self.old_path):
            fname = fname.decode('GBK')
            if fname in extracted:
                continue
            self.tasks.put_nowait(fname)
            
        print('added tasks: %d'%(len(self.tasks)))
    
    def start(self):
        create_path('logfile_extractor')
        gevent.spawn(self.manager).join()
        tasks = [gevent.spawn(self.worker,i) for i in range(self.gevent_num)]
        gevent.joinall(tasks)
        # self.start_worker()
        print('finish, data stored in %s'%(self.new_path))


def extracting(old_path,new_path,gevent_num=100):
    create_path(new_path)
    GeventQueue(old_path,new_path,gevent_num).start()


if __name__=='__main__':
    old_path = 'G:\CPPdata_formal'
    new_path = 'G:\CPPdata_extracted'
    extracting(old_path,new_path)

