# !/usr/bin/env python
# -*- coding: utf-8 -*-    # import system default encoding
from __future__ import print_function,unicode_literals    # import features of Python 3


#----------module document----------

__pyVersion__ = '2.7.9'

__author__ = 'Guo Zhang'

__contributors__ = ''

__last_edit_date__ = '2016-6-18'

__creation_date__ = '2016-6-18'

__moduleVersion__ = '1.1'

__doc__ = '''
This is a decorator module for China's Prices Project
'''


#----------module document----------


#----------module import----------

# import system modules
import chardet
import codecs
import csv
import time
import os
import re


#----------module import----------

 
#----------function definition----------

def tmall_kwd2cat():
    """
    Dicts for keyword to category id. 
    """
    kwd2cat = {}
    id2name = {}
    with open('tmall_kwd2cat') as f:
        reader = csv.reader(f)
        for row in reader:
            keyword = row[2].decode('utf-8')
            catID = row[1]
            catName = row[0].decode('utf-8')
            kwd2cat[keyword]=catID
            id2name[catID]=catName
    return kwd2cat,id2name

KWD2CAT,ID2NAME = tmall_kwd2cat()

def check_encoding(fname):
    with open(fname) as f:
        data = f.read(30)
    return chardet.detect(data)['encoding']

def create_path(path): 
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            return path
        except OSError, e:
            if e.errno != 17:
                raise(e)
            
def record_list(fname):
    try:
        with codecs.open(fname,'rb',encoding='utf-8') as f:
            t = f.readlines()
        t = [re.sub('\n','',i) for i in t]
        return t
    except IOError:
        return []
    
def runtime(func):
    def _deco(*args,**kwargs):
        begin = time.time()
        result = func(*args,**kwargs)
        end = time.time()
        print('time:',func.__name__,end-begin)
        return result
    return _deco       

def parse_fname(fname):
    '''
    Split file name to get data information
     Parameters
     ----------
      fname:str
       file name
     Returns
     -------
      data_dict: dict
       data information,including retailer,present_day,present_time,category_name,paras, page_num
    '''
    
    try:
        root,fname = os.path.split(fname)
    except:
        pass
    
    file_list = fname.split('_')
    
    # return page num of this file in this category
    page_num = file_list.pop()
    
    # return retailer
    retailer = file_list.pop(0)
    
    # return date
    date = file_list.pop(0)
    year,month,day = date.split('-')
    
    # return time and keyword
    new_element = file_list.pop(0)
    pattern = re.compile('\d\d-\d\d-\d\d')
    match = re.search(pattern,new_element)
    if match:
        time = new_element
        time = re.sub('-',':',time)
        keyword = file_list.pop(0)
    else:
        time = None
        keyword = new_element
    
    if (year=='2016')&(month in ['04','05','06','07','08']):
        kwd2cat,id2name = tmall_kwd2cat()
        try:
            catID = KWD2CAT[keyword]
            catName = ID2NAME[catID] 
        except KeyError,e:
            # write no-matching keyword into file
            with open('no_matching_keyword','ab') as f:
                 f.write(keyword)
                 f.write('\n')          

            catID = None
            catName = None
    
    # return parameters
    if file_list:
        params = '_'.join(file_list)
    else:
        params = None
            
    data_dict ={'retailer':retailer,'date':date,'time':time,'catID':catID,'catName':catName,'keyword':keyword,'params':params,'pageNum':page_num}
    data_dict = dict(filter(lambda x:x[1],data_dict.items()))
    return data_dict
 
def parse_goodsURL(url,para='id'):
    pattern =  re.compile('\d+')

    id_pattern = re.compile(para+'=\d+')
    id_match = re.search(id_pattern,url)
    if id_match:
        id = re.search(pattern,id_match.group(0)).group(0)
    else:
        id = None
        
    return id   
    
def deal_sales(sales):
    """
    Clean "万" for monthly_sales or comments.
     Params
     ------
      sales: str
       input string
     Returns
     -------
      num: int or None
       output sales number
    """
    
    pattern_ten_th = re.compile('万')
    pattern_num = re.compile('^\d+\.?\d*')
    match = re.search(pattern_ten_th, sales)
    if match:
        match_num = re.search(pattern_num, sales)
        num = int(float(match_num.group(0)) * 10000)
        return num
    else:
        match_num = re.search(pattern_num,sales)
        num = int(match_num.group(0))
        return num
    
def split_params(input):
    """
    Split parameters for Tmall and JD scrapers.
    """
    if type(input)== list:
            keyword = input[0]
            try:
                if type(input[1]==dict):
                    urlparams = input[1]
                else:
                    urlparams = {}
            except IndexError:
                urlparams = {}
    elif (type(input)== unicode or type(input)==str):
        keyword = input
        urlparams = {}
    else:
        print('Error: wrong categories list!!'.encode('utf-8'))
        keyword = ''
        urlparams = {}
    return keyword,urlparams

def join_params(**urlparams):
    """
    Join parameters for the request URL
    """
    
    if not urlparams:
        return None

    paras = ''
    urlparams = urlparams.items()
    for para in urlparams:
        new_para = ''.join([para[0],'=',str(para[1])])
        paras = '&'.join([paras,new_para])
    return paras
    
#----------function definition----------
    
    
