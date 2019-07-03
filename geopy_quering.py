#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
import geopy.geocoders  # doc：https://geopy.readthedocs.io/en/stable/
from geopy.geocoders import Nominatim
from geopy.geocoders import GoogleV3, Yandex, Photon
from geopy.extra.rate_limiter import RateLimiter  # for dataframe quering
import pandas as pd
from tqdm import tqdm
import re
import functools

UA = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0"

# default param
geopy.geocoders.options.default_user_agent = UA
geopy.geocoders.options.default_timeout = 7  # 抛出异常前等待地理编码服务响应的时间(s)
# set Geo-coder
# geolocator=GoogleV3(user_agent=UA)#自2018/07起，Google要求每个请求都需要API秘钥
# geolocator=Nominatim(user_agent=UA)
# geolocator = Yandex(user_agent=UA, lang='en')
geolocator=Photon()#此平台无需秘钥

# 给定单个维度经度lat-lon，获取其location
def geo_reverse_coding(latitude, longitude, UA=UA):
    address = str(latitude) + ',' + str(longitude)
    location = geolocator.reverse(address, language='en')
    print('Lat&Lon:', address)
    print('location:', location.address)
    print('location.raw', location.raw)
    return location

# 判断字符串是否为{空、英文、非英文}
def isEnglish(s):
    if s=='nan':return 0
    else:
        ans = re.search(r"[a-zA-Z\']+$", s)
        return 1 if ans else 2

#获取geoname_id和lat&lon的索引关系
def get_geoID():
    id_v4=pd.read_csv("../data/GeoIP2-City-CSV-part/GeoIP2-City-Blocks-IPv4.csv",encoding='utf-8')
    id_v6=pd.read_csv("../data/GeoIP2-City-CSV-part/GeoIP2-City-Blocks-IPv6.csv",encoding='utf-8')
    print("IPv4_shape=",id_v4.shape,"IPv6_shape=",id_v6.shape)
    #为降低数据量，仅保留geoname_id、latitude、longitude三个特征列来作为索引依据
    bad_cols=['network','registered_country_geoname_id','represented_country_geoname_id','is_anonymous_proxy',
              'is_satellite_provider','postal_code','accuracy_radius']
    for df in [id_v4,id_v6]:
        df.drop(bad_cols,axis=1,inplace=True)
    print('remaining_columns:',id_v4.columns)
    geoID=pd.concat([id_v4,id_v6],axis=0,ignore_index=True)
    del id_v4
    del id_v6
    print("get geoID!")
    print("geoID shape:", geoID.shape)
    #删除geoname_id中重复的行
    geoID.drop_duplicates(subset=['geoname_id',],keep='first',inplace=True)
    print("geoID shape after duplicates:",geoID.shape)
    return geoID

#将lat&lon（纬度和经度）merge到data中
def merge_geoID_data(geoID,data="../output/GeoIP2-City-Locations-abnormal.csv"):
    data=pd.read_csv(data,encoding='utf-8')
    print('merging...')
    res=pd.merge(data,geoID,on='geoname_id')
    res.to_csv('../output/GeoIP2-City-Locations-abnormal-vsLatLot.csv')
    del data
    del geoID
    return res

#批量逆编码
def batch_geo_reverse_coding(data):
    #使用偏函数对默认参数进行修改
    print('ds',data.shape)
    georeverse = functools.partial(geolocator.reverse, language='en')
    # 自动添加请求延迟，减少负载，且能够重试失败的请求并过滤各行的错误提示
    location = RateLimiter(georeverse, min_delay_seconds=1, )
    data['latitude'] = data['latitude'].astype(str)
    data['longitude'] = data['longitude'].astype(str)
    data['LatLon'] = data['latitude'].str.cat(data['longitude'], sep=',')
    tqdm.pandas()# 调用tqdm进度条库显示进度,需要改用progress_apply
    data['location_raw'] = data['LatLon'].progress_apply(location)
    # data['city']=data['location_raw'].apply(lambda x:re.findall(r"[(](.*?)[(]",str(x)))
    data.to_csv('../output/GeoIP2-City-Locations-abnormal-resRaw.csv')
    return data

#删除临时列，将dataframe调整至标准状态
def get_finall_result(data='../output/GeoIP2-City-Locations-abnormal-resRaw.csv'):
    data=pd.read_csv(data,encoding='utf-8')
    print(data.columns)
    good_cols=['geoname_id','locale_code','continent_code','continent_name','country_iso_code','country_name',
               'subdivision_1_iso_code','subdivision_1_name','subdivision_2_iso_code','subdivision_2_name','city_name',
               'metro_code','time_zone','is_in_european_union',]+['location_raw',]#需要保留的列
    for col in data.columns:
        if col not in good_cols:
            data.drop(col,axis=1,inplace=True)
    #print(data.shape,data.columns)
    #将city_name覆盖重写
    data['city']=data['location_raw'].str.split(',').apply(lambda x:x[0][10:])#[10:]
    # data['city'] = data['location_raw'].str.split(',')# [10:]
    # data['city']=data['location_raw'].str.split(',')[0][10:]
    data.drop('location_raw',axis=1,inplace=True)
    data.to_csv('../output/GeoIP2-City-Locations-abnormal-res.csv')

if __name__ == '__main__':
    #单条转换
    #geo_reverse_coding(latitude='39.956981',longitude='116.375523')#latitude='13.6967',longitude='44.7308'
    #批量转换
    geoID=get_geoID()#获取IPv4、IPv6的geoname_id -- Lat&Lon关系表
    data=merge_geoID_data(geoID,)#将表merge到需要逆编码的data中，使data具有lat&lon列
    batch_geo_reverse_coding(data)#批量进行逆编码
    get_finall_result()#调整列使其与原始数据data格式一致
