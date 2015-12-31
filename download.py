# -*- coding: utf-8 -*-
"""
Created on Wed Aug 05 12:50:31 2015
@author: asus
"""

import tushare as ts
from datetime import *
import time
import sqlite3 as sql
import numpy as np
import pandas as pd
import types
import pandas.io.sql as sql2
import matplotlib.pyplot as plt
import os
import sys
#import objgraph as og
reload(sys)
sys.setdefaultencoding('utf-8')
import requests
#from WindPy import *

workdir='d:\\\\python\\'
stock_bad_tick_day='stock_bad_tick_day.txt'
#w.start(waitTime=30)

f=open('username_and_password_for_wireless_pku.txt')
a=f.readlines()
username_pku=a[0]
password_pku=a[1]
f.close()

class onestock:

    def __init__(self,stock_number):
        self.stock_number=stock_number
        self.dbdir=workdir
        self.dbdirname=self.dbdir+'Tu'+stock_number+'.db'
        self.record_count=0
        self.con=sql.connect(self.dbdirname)
        self.con.text_factory =str
        self.n_bad_tick_day=0
        #self.basics=ts.get_stock_basics().ix[self.stock_number]
        #self.date_range=pd.date_range(str(self.basics['timeToMarket']),date.today())
        self.date_range=pd.date_range('20100101',date.today())
        self.download_range=self.date_range


        #self.con.isolation_level = None

    def add_record(self,date):
        while True:
            table=[]
            try:
                print self.stock_number,date
                table=ts.get_tick_data(self.stock_number,date=date)
            except:
                repaire_wireless_pku()
            if type(table)!=types.ListType:
                break

        if type(table)==types.NoneType:
            print 'No Data Today, take care'
        else:
            self.record_count+=1
            table['day']=date
            table=table.reindex(columns=np.hstack(('day',table.columns.values[:-1])))
            creat_table_command,insert_table_commmand=generate_sql_command(table,'tick')
            if self.record_count==1:
                self.con.execute(creat_table_command)
                self.con.commit()
            self.con.executemany(insert_table_commmand,table.values)



    def creat_tick_table(self):
        for thisday in [i.date() for i in self.download_range]:
            try:
                self.add_record(thisday)
            except:
                print 'run into a error'
                self.n_bad_tick_day+=1
            #table.to_sql(self.stock_number+' tick',self.con)
        try:
            self.con.execute("create index if not exists index_day on tick(day)")
        except:
            file1=open(workdir+stock_bad_tick_day,'a')
            file1.write(self.stock_number+'no data for this stock'+'\n')
            file1.close()

        print 'indexing ..........'
        self.con.commit()
        file1=open(workdir+stock_bad_tick_day,'a')
        file1.write(self.stock_number+', bad tick day number is:'+str(self.n_bad_tick_day)+'\n')
        file1.close()



    def creat_k_table(self):
        for ktype in ['D','W','M','5','15','30','60']:
            try:
                table=ts.get_hist_data(self.stock_number,ktype=ktype)
                creat_table_command,insert_table_commmand=generate_sql_command(table,'K'+ktype)
                self.con.execute(creat_table_command)
                self.con.executemany(insert_table_commmand,table.values)
            except:
                print 'run into an error with k line'
                file1=open(workdir+stock_bad_tick_day,'a')
                file1.write(self.stock_number+', bad K line type number is        :'+ktype+'\n')
                file1.close()


        self.con.commit()
        print self.con.execute("select name from sqlite_master where type='table' order by name").fetchall()


    def load(self):
        self.tick=sql2.read_sql('select * from '+'tick',self.con)
        self.kd=sql2.read_sql('select * from '+'KD',self.con)
        self.kw=sql2.read_sql('select * from '+'KW',self.con)
        self.km=sql2.read_sql('select * from '+'KM',self.con)
        self.k5=sql2.read_sql('select * from '+'K5',self.con)
        self.k15=sql2.read_sql('select * from '+'K15',self.con)
        self.k30=sql2.read_sql('select * from '+'K30',self.con)
        self.k60=sql2.read_sql('select * from '+'K60',self.con)
        self.con.close()
    def ret_day(self,datebegin,dateend):
        nday=0
        for date in pd.date_range(datebegin,dateend):
            date=str(date.date())
            table=self.tick.ix[self.tick['day']==date].set_index(['time']).sort_index()['price']
            if table.size < 10:
                continue
            else:

                if nday==0:
                    pass
                else:
                    ret=(table-lastprice)/lastprice
                    fig=plt.figure()
                    fig.add_subplot(1,2,1)
                    ret.plot()
                    plt.title(date)

                    fig.add_subplot(1,2,2)
                    table.plot()
                    plt.title('price')
                nday=nday+1
                lastprice=table.values[-1]
                print date,lastprice,table




def generate_sql_command(table,typeofdata):
    columnchar='('
    columnchar2=' VALUES('
    for column_name in table.columns.values:
        columnchar=columnchar+column_name+','
        columnchar2=columnchar2+'?,'
    columnchar=columnchar[0:-1]+')'
    columnchar2=columnchar2[0:-1]+')'
    creat_table_command='create table if not exists '+typeofdata+columnchar
    insert_table_commmand='INSERT INTO '+typeofdata+columnchar2
    return creat_table_command, insert_table_commmand

def repaire_wireless_pku(username=username_pku,password=password_pku):
    print 'the network runinto a problem'
    print 'i am repairing the wireless_pku'

    url='http://its.pku.edu.cn/cas/login'
    data_login={'username1':username,
                'password':password,
                'fwrd':'free',
                'username':username+'|;kiDrqvfi7d$v0p5Fg72Vwbv2;|'+password+'|;kiDrqvfi7d$v0p5Fg72Vwbv2;|12'
                }
    data_logout={'username1':username,
                 'password':password,
                 'pwd_t':'密码',
                 'fwrd':'free',
                 'username':username+'|;kiDrqvfi7d$v0p5Fg72Vwbv2;|'+password+'|;kiDrqvfi7d$v0p5Fg72Vwbv2;|13'
                 }
    s=requests.session()
    response=s.post(url,data=data_logout)
    response=s.get('http://its.pku.edu.cn/netportal/PKUIPGW?cmd=close&type=allconn&fr=0&sid=224')
    time.sleep(0.1)
    response=s.post(url,data=data_login)
    response=s.get('http://its.pku.edu.cn/netportal/PKUIPGW?cmd=open&type=free&fr=0&sid=362')



#all_stock_code=ts.get_stock_basics()['code']
def download_all_date(if_check_before=0):
    all_stock_code=ts.get_industry_classified()
    all_stock_code=all_stock_code.ix[all_stock_code['code'].duplicated()==False]['code']
    n=0
    exist_file=os.listdir(workdir)
    for stock_code in all_stock_code:
        print stock_code
        if ('Tu'+stock_code+'.db') in exist_file:
            print 'it is already done'
            n+=1
            if if_check_before==1:
                test=onestock(stock_code)
                test.download_range=pd.date_range(test.con.execute('select max(rowid),day from tick').fetchall()[0][1],date.today())[1:]
                test.creat_tick_table()
                test.con.close()
            print n,' is done'


        else:
            test=onestock(stock_code)
            test.creat_tick_table()
            test.creat_k_table()
            test.con.close()
            n+=1
            print n,'is done'
def delete_bad_stock():
    w=open(workdir+'stock_bad_tick_day.txt').readlines()
    w2=open(workdir+'only_bad_stock.txt','a')
    for i in range(len(w)):
        if w[i][8:]!='bad tick day number is:0\n':
            path_name=workdir+'Tu'+w[i][:6]+'.db'
            if os.path.exists(path_name):
                os.remove(path_name)
                w2.write('successful deleting '+w[i][:6]+'\n')
            else:
                print 'no this file '
                w2.write('no this file'+w[i][:6]+'\n')

    w2.close()


#test.ret_day('2015-08-01','2015-08-15')




def realtime_write():
    con=sql.connect(workdir+'concept.db')
    concept_all_stock=sql2.read_sql('select * from concept',con)
    con.close()


    con=sql.connect(workdir+'realtime_'+datetime.now().date().strftime('%Y%m%d')+'.db')
    con.execute('PRAGMA journal_mode=WAL')
    #con.execute('PRAGMA cache_size =80000')
    #con.execute('PRAGMA synchronous = off')
    #con.execute('PRAGMA page_size = 8192')
    #con.execute('PRAGMA temp_store = MEMORY')
    #con.execute('PRAGMA wal_autocheckpoint=10000000')

    creat_table_command='create table if not exists realtime(open,pre_close,price,high,low,volume,b1_v,b1_p,b2_v,b2_p,b3_v,b3_p,b4_v,b4_p,b5_v,b5_p,a1_v,a1_p,a2_v,a2_p,a3_v,a3_p,a4_v,a4_p,a5_v,a5_p,time,code)'
    insert_table_command='INSERT INTO realtime VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

    con.execute(creat_table_command)
    #con.execute('create index if not exists code on realtime(code)')


    concept_name=list(set(concept_all_stock.c_name))
    stock_code=list(set(concept_all_stock.code))
    #all_time=pd.DataFrame()
    last_time=pd.DataFrame()
    n=0
    #time1=time.clock()

    while True :

        try:
            one_time=pd.DataFrame()
            for i in range(5):
                temp=ts.get_realtime_quotes(stock_code[i*500:(i+1)*500])
                temp=temp.drop(['name','date','bid','ask','amount'],axis=1)
                one_time=one_time.append(temp)

        except:
            continue


        new_one_time=last_time.append(one_time).drop_duplicates()[len(last_time.index):]
        if len(new_one_time)==0:
            print 'no new data'
            continue

        #sql2.to_sql(new_one_time,'realtime',con,if_exists='append',index=True)
        time1=time.clock()
        con.executemany(insert_table_command,new_one_time.values)
        con.commit()
        time2=time.clock()
        #new_one_time=new_one_time.set_index([pd.to_datetime(new_one_time.time),new_one_time.code])
        #all_time=all_time.append(new_one_time)
        last_time=one_time
        #print len(all_time.index)
        n+=1
        print n,len(new_one_time.index),time2-time1

    #for i in range(len(concept_name)):
     #   concept_i_code=concept_all_stock.ix[concept_all_stock.c_name==concept_name[i]]['code']
      #  ts.get_realtime_quotes(concept_i_code)
       # concept_n+=1
        #print concept_n,concept_name[i]



def concept_read():
    con=sql.connect(workdir+'concept.db')
    concept_all_stock=sql2.read_sql('select * from concept',con)
    con.close()

    guoqigaige=concept_all_stock[concept_all_stock['c_name']==u'国企改革'][['code','name']]
    select_words='select * from realtime where code in %s' %str(tuple(guoqigaige.code)).replace('u','')


    con=sql.connect(workdir+'realtime_'+datetime.now().date().strftime('%Y%m%d')+'.db')
    #con.execute('create index if not exists code on realtime(code)')
    #con.execute('PRAGMA cache_size =80000') try this for writing
    #con.execute('PRAGMA page_size = 8192')
    #con.execute('PRAGMA synchronous = full')
    #con.execute('PRAGMA temp_store = MEMORY')

    n=1
    while True:
        time1=time.clock()
        real_data=sql2.read_sql(select_words,con)
        time2=time.clock()
        n+=1
        print n,len(real_data.index),time2-time1
        #print real_data.head(5)

#download_all_date(if_check_before=1)
#delete_bad_stock()

#time1=time.clock()
realtime_write()
#concept_read()
#time2=time.clock()
#time2-time1




#while True :
 #   df1 = ts.get_realtime_quotes('603616')
  #  df2 = ts.get_realtime_quotes('sh')
   # print df1[['name','price','b1_v','a1_v','volume','time']]
    #print df2[['name','price']]
    #time.sleep(0.5)