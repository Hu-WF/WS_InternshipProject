#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
import pandas as pd
from IPy import IP
from tqdm import tqdm

def get_cnc_base(path='../data/ipb-ips_raw.str/ipb-ips_raw'):
    '''把CNC city_name数据库加载入内存，以供查询，并删除无用部分'''
    '''由于cnc较为标准，因此获取cnc的city_name作为基准，并拆分为contry_code、state、city'''
    df=pd.read_csv(path+'.str',encoding='utf-8',header=1,sep='\t')
    # print(df.columns,)
    # print('cnc原始数据量', df.shape)
    # df.drop(['<IPHEAD>','<IPTAIL>','<OWNER>','<ISP>','<VIEW>',],axis=1,inplace=True)
    df.drop([ '<OWNER>', '<ISP>', '<VIEW>', ], axis=1, inplace=True)
    df.drop_duplicates(keep='first',inplace=True)
    #分割出三段
    df['country_code'],df['state'],df['city']=df['<CITY>'].str.split('_',2).str
    # print(df.columns,df.shape)
    df['state'] = df['state'].apply(lambda x: str(x).capitalize())#cnc的中国state、city首字母为小写，因此先转换
    df['city']=df['city'].apply(lambda x:str(x).capitalize())
    df.to_csv('../fileout/cnc_expand/cnc01_base.csv')
    return df

def get_continent_country_from_maxmind(data='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv'):
    '''通过maxmind获取continent-country关系'''
    df = pd.read_csv(data)
    # print(df.columns)
    # print('maxmind原始数据量', df.shape)
    # cols = ['continent_code', 'continent_name','country_iso_code','country_name',
    #         'subdivision_1_iso_code','subdivision_1_name','subdivision_2_iso_code','subdivision_2_name','city_name']
    cols = ['continent_code','country_iso_code',]
    bad_cols = [col for col in df.columns if col not in cols]
    df.drop(bad_cols, axis=1, inplace=True)#删除无用特征
    df.drop_duplicates(keep='first',inplace=True)#删除重复行
    df.dropna(subset=['country_iso_code'],inplace=True)#丢弃country_iso_code为空值的行
    df.fillna('NA',inplace=True)#pandas读入北美洲NA时，会变成空缺值，因此填充回NA
    df.to_csv('../fileout/cnc_expand/maxmind_continent-country.csv')
    return df

def expand_cnc_with_continent():
    '''从maxmind导出continent-country表，merge到cnc中'''
    cnc=get_cnc_base()
    continent_country=get_continent_country_from_maxmind()
    #为cnc赋予continent信息
    std=pd.merge(left=cnc,right=continent_country,left_on='country_code',right_on='country_iso_code')
    std.drop('country_iso_code',axis=1,inplace=True)#丢弃合并后的重复键值列
    # std.replace('qita',np.nan,inplace=True)
    # 创建新的FSN
    std['cnc_city'] = std['continent_code'] + '_' + std['<CITY>']
    bad_cols = [col for col in std.columns if col not in ['<IPHEAD>','<IPTAIL>','cnc_city']]
    std.drop(bad_cols, axis=1, inplace=True)  #仅保留<IPHEAD>、<IPTAIL>、cnc_city
    std.to_csv('../fileout/cnc_expand/cnc02_continent.csv')
    return std
#===========以上为cnc continent字段扩充处理函数,经过以上函数后，输出完整的待处理cnc_base dataframe===============

#接下来处理maxmind，使输出标准的待处理maxmind的ip-city查询库===========================
def get_maxmind_cityDatabase():
    '''加载maxmind数据库，删除无用部分，以供查询'''
    print('load maxmind dataframe.')
    id_v4 = pd.read_csv("../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv4.csv", encoding='utf-8')
    id_v6 = pd.read_csv( "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv6.csv", encoding='utf-8')
    bad_cols = [col for col in id_v4.columns if col not in ['network', 'geoname_id']]  # 仅保留network和geoname_id
    for df in [id_v4, id_v6]:
        df.drop(bad_cols, axis=1, inplace=True)
        df.drop_duplicates(subset=['geoname_id', ], keep='first', inplace=True)
    city = pd.read_csv("../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv", encoding='utf-8')
    city.fillna('Qita', inplace=True)  # 填充所有缺失值,并替换源数据
    # 仿照cnc的格式，创建新的city_name:continent_code + country_iso_code + subdivision_1_name + city_name
    city['maxmind_city'] = city['continent_code'] + '_' + city['country_iso_code'] + '_' \
                            + city['subdivision_1_name'] + '_' + city['city_name']
    database_v4 = pd.merge(id_v4, city, on='geoname_id')
    database_v6 = pd.merge(id_v6, city, on='geoname_id')
    bad_cols = [col for col in database_v4.columns if col not in ['network', 'maxmind_city']]
    for df, save_name in zip([database_v4, database_v6], ['maxmind_for_cityName_v4.csv', 'maxmind_for_cityName_v6.csv']):
        df.drop(bad_cols, axis=1, inplace=True)
        df.to_csv('../fileout/cnc_expand' + '/' + save_name)
    return (database_v4, database_v6)


#经过以上两步操作后，有了cnc_base和（maxmind_v4，maxmind_v6）两个标准IP-city_name关联数据库，下一将以cnc_base为基础，用maxmind扩充之
def maxmind_query(database,ip):
    '''输入单个ip地址，在maxmind中二分查询city_name'''
    def binary_query(database, ip):  # 二分查找函数
        low,high = 0,database.shape[0]
        while low <= high:
            mid = (low + high) // 2
            ip_,ip_mid=IP(ip),IP(database['network'][mid])
            if ip_ in ip_mid:
                return database['maxmind_city'][mid]
            elif ip_mid >= ip_:
                high = mid - 1
            else:
                low = mid + 1
        return ''
    if IP(ip).version() == 4:#判断IP地址类型，选择不同database
        return binary_query(database[0], ip)
    else:
        return binary_query(database[1], ip)

def expand_cnc_with_maxmind():
    cnc=pd.read_csv('../fileout/cnc_expand/cnc02_continent.csv')
    mmv4=pd.read_csv('../fileout/cnc_expand/maxmind_for_cityName_v4.csv')
    mmv6 = pd.read_csv('../fileout/cnc_expand/maxmind_for_cityName_v6.csv')
    database=(mmv4,mmv6,)
    print(cnc.shape,cnc.columns)
    print(mmv4.shape,mmv4.columns)
    print(mmv6.shape,mmv6.columns)
    '''二分查找将maxmind匹配到cnc中'''
    print('matching...')
    tqdm.pandas()#显示匹配进度
    cn=cnc.loc[:100000].copy()
    print(cn.shape,cn.columns)
    cn['maxmind_city'] = cn['<IPHEAD>'].progress_apply(lambda x:maxmind_query(database,x))
    print(cn.shape,cn.columns)
    cn.to_csv('../fileout/cnc_expand/cnc03_expand_with_maxmind.csv')


if __name__=='__main__':
    # expand_cnc_with_continent()#为含有IP段的cnc添加continent字段，并组合为cnc_city字段
    # get_maxmind_cityDatabase()#整合maxmind数据库，使其仅含network，maxmind_city两个字段
    expand_cnc_with_maxmind()#以cnc为基础，合并入maxmind的cnty_name信息
