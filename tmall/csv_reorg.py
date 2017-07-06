# !/usr/bin/env python
# -*- coding: utf-8 -*-    # import system default encoding
from __future__ import print_function,unicode_literals    # import features of Python 3


#----------module document----------

__pyVersion__ = '2.7.9'

__author__ = 'Guo Zhang'

__contributors__ = ''

__last_edit_date__ = '2016-6-3'

__creation_date__ = '2016-5-28'

__moduleVersion__ = '0.1 dev'

__doc__ = '''
This is a Tmall csv data sorting tool for China's Prices Project.
It classfies data by its keywords and params.
JD to be developed.
'''

#----------module document----------


#----------module import----------

# import system modules
import codecs
import csv
import os
import re

# import third-party modules
from gevent import monkey;monkey.patch_all()
import gevent
# from gevent.pool import Pool
from gevent.queue import Queue

# import my own modules
from tools import create_path,parse_fname,parse_goodsURL,runtime,record_list

#----------module import----------


#----------global variables----------

#----------global variables----------


#----------class definition----------


class Reorganizer(object):
    def __init__(self,fname,goal_path):
        self.fname = fname
        
        # information from file name
        self.fileinfo = parse_fname(fname)
        
        # params for new file name
        self.year,self.month,self.day = self.fileinfo['date'].split('-')
        
        try:
            self.catid = self.fileinfo['catID']
        except KeyError:
            self.catid = None
            
        self.keyword = self.fileinfo['keyword']
        try:
            self.params = self.fileinfo['params']
        except KeyError:
            self.params = None
        
        self.retailer = self.fileinfo['retailer']

        
        # goal file name (e.g.)
        self.newfname = os.path.join(goal_path,'_'.join(filter(lambda x:x,[self.retailer,self.catid,self.keyword,self.params,'-'.join([self.year,self.month])])))
    
    def read(self):
        """
        Read a CSV file and parse it into dicts
        
        
        Returns
        -------
        data_list: parsed dict data list
        """
        
        data_list = []
        with codecs.open(self.fname,'rb',encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # return None if no data in the file
            if not reader:
                return None
                
            for i,dic in enumerate(reader):
                
                # supplement goodsID if no 
                try:
                    dic['goodsID']
                except KeyError:
                    dic['goodsID']=parse_goodsURL(dic['goodsURL'])
                
                # add order information
                dic['order']=i+1
                
                # add information from file name
                dic = dict(dic,**self.fileinfo)
                # add into data_list
                data_list.append(dic)
                
        return data_list
    
    def write(self,data_list):
        fieldnames = ['goodsID','goodsURL','catID','catName','goodsName','keyword','params','shopName','shopURL','date','time','price','price_ave','monthly_sales','comments','pageNum','order']
        
        # if no file, create file and write header.
        if not os.path.exists(self.newfname):
            with codecs.open(self.newfname,'wb') as f:
                writer = csv.DictWriter(f,fieldnames=fieldnames)
                writer.writeheader()
                
        # write data
        with codecs.open(self.newfname,'ab') as f:
            writer = csv.DictWriter(f,fieldnames=fieldnames)
            for data in data_list:
                data = {key:value for key,value in data.items() if key in fieldnames}
                writer.writerow(data)
    
    def start(self):
        try:
            data_list = self.read()
            self.write(data_list)
        except Exception,e:
            print('File Name: %s; Error: %s'%(self.fname,e))


class GeventQueue(object):
    def __init__(self,csvdicts,goal_path,gevent_num=100):
        # self.function = function
        self.csvdicts = csvdicts
        self.goal_path = goal_path
        self.gevent_num = gevent_num
        self.tasks = Queue()

    def worker(self,i):
        print('worker %d start'%(i))
        while not self.tasks.empty():
            try:
                # print('worker %d start'%(i))    
                task = self.tasks.get()
                

                # write current task into worker's running log
                f = open(os.path.join('logfile','%s_running.log'%(i)),'wb')
                f.write(task)
                f.close()
                
                # run the task
                Reorganizer(task,self.goal_path).start()
                
                # write finished work
                root,base = os.path.split(task)
                fs = open(os.path.join(root,'success'),'ab')
                fs.write(base)
                fs.write('\n')
                fs.close()
                gevent.sleep(0)
        
            except Exception,e:
                print(e)
                # write error work into error log
                print('error:%d; task:%s'%(i,task))
                fl = open(os.path.join('logfile','error.log'),'ab')
                fl.write(task)
                fl.write('\n')
                fl.close()
                continue
            
                # restart this worker
                # return self.worker(i)
    
    def manager(self):
        for csvdict in self.csvdicts:
            for root,dicnames,fnames in os.walk(csvdict):
                successlist = record_list(os.path.join(root,'success'))
                fnames = list(set(fnames)-set(['success']))
                if not fnames:
                    continue
                for fname in fnames:
                    if fname not in successlist:
                        self.tasks.put_nowait(os.path.join(root,fname))
            print('added tasks: %d'%(len(self.tasks)))
    
    def test_worker(self,i):
        print('worker %d start'%(i))
        while not self.tasks.empty():
            try:
               # test_dict = {'key':'value'}
               task = self.tasks.get()
               print(task)
               gevent.sleep(0)
               # Reorganizer(task,self.goal_path).start()
               # import random
               # test_error = test_dict[random.choice(['key','keykey'])]
            except Exception,e:
               print(e)
               # return self.test_worker(i)

    def start(self):
        create_path('logfile')
        gevent.spawn(self.manager).join()
        tasks = [gevent.spawn(self.worker,i) for i in range(self.gevent_num)]
        gevent.joinall(tasks)
        # self.start_worker()
        print('finish, data stored in %s'%(self.goal_path))

#----------class definition----------


#----------function definition----------
    
        
#----------function definition----------


#----------main function----------

if __name__ == '__main__':
    # test goal path
    # goal_path='/home/xmucpp/Git_lib/cpp-data-cleaning/test-result'

    # formal goal path    
    goal_path = '/home/xmucpp/cppdata/Organized'
    create_path(goal_path)
    
    # test set
    # csvdicts = ['/home/xmucpp/Git_lib/cpp-data-cleaning/TinySet-10']
   
    # formal set
    csvdicts=['/home/xmucpp/cppdata/Sources']
    
    # run it
    # reganizating(csvdicts,goal_path,gevent_num=100)
    GeventQueue(csvdicts,goal_path,gevent_num=100).start()

#----------main function----------
