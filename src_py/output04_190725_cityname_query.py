#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
'''output04_190725'''
import os
import pandas as pd
from IPy import IP

class CitynameQuery():
    '''单个输入ip地址，输出多个库的city_name查询结果'''
    def __init__(self):
        #in data
        self.__cnc_data='../data/ipb-ips_raw.str/ipb-ips_raw'
        self.__maxmind_ipv4_data="../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv4.csv"
        self.__maxmind_ipv6_data="../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv6.csv"
        self.__maxmind_city_data="../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv"
        #out data
        self.__cnc_out_dir='../fileout/cnc'
        self.__maxmind_out_dir='../fileout/maxmind'
        self.__cnc_out_name='cnc_for_cityName.csv'
        self.__maxmind_out_name='maxmind_for_cityName.csv'

    def __get_cnc_database(self,):
        '''把CNC数据库加载入内存，以供查询，并删除无用部分'''
        print('loading CNC dataframe...')
        cnc = pd.read_csv(self.__cnc_data + '.str', encoding='utf-8', header=1, sep='\t')
        cnc.drop(['<OWNER>', '<ISP>', '<VIEW>', ], axis=1, inplace=True)#仅保留'<IPHEAD>', '<IPTAIL>', '<CITY>'
        # data['country_code'], data['state'], data['city'] = data['<CITY>'].str.split('_', 2).str# 分割出三段
        print('sucess.')
        cnc.to_csv(self.__cnc_out_dir+'/'+self.__cnc_out_name)
        return cnc

    def cnc_ip_query(self,database,ip):
        '''输入单个ip地址，在cnc中查询city_name'''
        print('quering in cnc...')
        #IPy库将lastIP格式的IP地址转换为prefix格式的IP地址时，容易出现bug，因此改用int转换+循环遍历的方式来判断是否处于区间内：
        for head,tail,city in zip(database['<IPHEAD>'],database['<IPTAIL>'],database['<CITY>']):
            ipint=IP(ip).int()
            if ipint>=IP(head).int() and ipint<=IP(tail).int():
                print('CNC result：',city)
                return city

    def __get_maxmind_database(self,):
        '''加载maxmind数据库，删除无用部分，以供查询'''
        print('loading maxmind dataframe ...')
        id_v4=pd.read_csv(self.__maxmind_ipv4_data,encoding='utf-8')
        id_v6=pd.read_csv(self.__maxmind_ipv6_data,encoding='utf-8')
        # print("IPv4_shape=",id_v4.shape,"IPv6_shape=",id_v6.shape)
        bad_cols=[col for col in id_v4.columns if col not in ['network','geoname_id']]#仅保留network和geoname_id
        for df in [id_v4,id_v6]:
            df.drop(bad_cols,axis=1,inplace=True)
        geoID=pd.concat([id_v4,id_v6],axis=0,ignore_index=True)
        del id_v4
        del id_v6
        geoID.drop_duplicates(subset=['geoname_id',],keep='first',inplace=True)
        city=pd.read_csv(self.__maxmind_city_data,encoding='utf-8')
        city.fillna('qita',inplace=True)#填充所有缺失值,并替换源数据
        #仿照cnc的格式，创建新的city_name:continent_code + country_iso_code + subdivision_1_name + city_name
        city['city_name_new']=city['continent_code']+'_'+city['country_iso_code']+'_'\
                              +city['subdivision_1_name']+'_'+city['city_name']
        database=pd.merge(city,geoID,on='geoname_id')
        del city
        del geoID
        bad_cols=[col for col in database.columns if col not in ['network','city_name_new']]
        database.drop(bad_cols, axis=1, inplace=True)
        print('success.')
        database.to_csv(self.__maxmind_out_dir+'/'+self.__maxmind_out_name)
        return database

    def maxmind_ip_query(self,database,ip):
        '''输入单个ip地址，在maxmind中查询city_name'''
        print('quering in maxmind ...')
        #使用apply的方式查询，速度稳定，但不快
        # city_name = database.loc[database['network'].apply(lambda x:ip in IP(x)) == True, 'city_name_new']
        # print('Maxmind 查询结果：',city_name)
        # return city_name
        #使用for循环查询，速度时快时慢
        for network,city in zip(database['network'],database['city_name_new']):
            if ip in IP(network):
                print('MaxMind result：',city)
                return city

    def load_database_faster(self,):
        """判断cnc和maxmind是否已经处理好保存，避免每次查询都要先做处理，以此加快非首次运行的加载速度"""
        files_cnc=os.listdir(self.__cnc_out_dir)
        files_maxmind=os.listdir(self.__maxmind_out_dir)
        if self.__cnc_out_name not in files_cnc and self.__maxmind_out_name not in files_maxmind:
            print('reload database：')
            cnc=self.__get_cnc_database()
            maxmind=self.__get_maxmind_database()
        else:
            cnc=pd.read_csv(self.__cnc_out_dir+'/'+self.__cnc_out_name)
            maxmind=pd.read_csv(self.__maxmind_out_dir+'/'+self.__maxmind_out_name)
        return cnc,maxmind

    def console_quering(self,cnc,maxmind):
        """在控制台输入ip地址，以直接进行多次查询,按q键退出查询"""
        print("press 'q' to quit.")
        while True:
            print('_' * 50)
            ip=str(input('Input IP address：'))
            if ip=='q' or ip =='Q':
                break
            else:
                self.cnc_ip_query(cnc,ip)
                print('_'*50)
                self.maxmind_ip_query(maxmind,ip)

if __name__=='__main__':
    cq=CitynameQuery()
    cnc,maxmind=cq.load_database_faster()#载入cnc和maxmind数据库

    #方式1：以控制台输入ip地址的方式查询，如输入1.2.2.0
    cq.console_quering(cnc,maxmind)
    #方式2：直接调用api查询
    cq.cnc_ip_query(cnc,ip='1.0.0.0')
    cq.maxmind_ip_query(maxmind,ip='1.0.0.0')

