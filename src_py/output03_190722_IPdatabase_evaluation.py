#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
import pandas as pd
import xml.etree.ElementTree as ET

def xml_to_csv(data="../data/QQ-LocList/LocList.xml"):
    '''载入qq的LocList城市名标准库，打印基本信息，并解析成csv文件'''
    tree=ET.parse(data)
    root=tree.getroot()
    result=[]
    for country in root.iter('CountryRegion'):#country
        country_name, country_code = country.attrib["Name"], country.attrib["Code"]
        for state in country:#state
            try:
                state_name, state_code = state.attrib['Name'], state.attrib['Code']
            except:
                state_name = state_code = None
            for city in state:#city
                city_name, city_code = city.attrib['Name'], city.attrib['Code']
                result.append({'country_name':country_name,'country_code':country_code,
                               'state_name':state_name,'state_code':state_code,
                               'city_name':city_name,'city_code':city_code})
    result=pd.DataFrame(result,columns=['country_name','country_code','state_name','state_code','city_name','city_code'])
    print('已将data/QQ-LocList/LocList.xml转换为fileout/qqloc/FSN_fromQQ.csv.')
    result.to_csv('../fileout/qqloc/FSN_fromQQ.csv')
    print("qqLoc总数据量有：",result.shape[0])
    country=result.drop_duplicates(subset=['country_name', ], keep='first', inplace=False)
    print("qqLoc国家数量有：", country.shape[0])
    state=result.drop_duplicates(subset=['state_name', ], keep='first', inplace=False)
    print("qqLoc省/州数量有：", state.shape[0])
    city=result.drop_duplicates(subset=['city_name', ], keep='first', inplace=False)
    print("qqLoc城市数量有：", city.shape[0])
    return 0

def maxmind_eval(data='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv'):
    '''分析maxmind基本信息'''
    data=pd.read_csv(data)
    print(data.columns)
    print('maxmind原始数据量',data.shape)
    cols=['continent_code','continent_name','country_iso_code','country_name','city_name']
    bad_cols=[col for col in data.columns if col not in cols]
    data.drop(bad_cols,axis=1,inplace=True)
    # data.drop_duplicates(subset=['continent_code','continent_name','country_iso_code','country_name',],keep='first',inplace=True)
    continent=data.drop_duplicates(subset=['continent_name', ],keep='first', inplace=False)
    print('maxmind洲数量',continent.shape)
    country=data.drop_duplicates(subset=['country_name', ],keep='first', inplace=False)
    print('maxmind国家数量',country.shape)
    city=data.drop_duplicates(subset=['city_name', ],keep='first', inplace=False)
    print('maxmind城市数量',city.shape)

def cnc_eval(path='../data/ipb-ips_raw.str/ipb-ips_raw'):
    '''分析CNC基本信息'''
    data=pd.read_csv(path+'.str',encoding='utf-8',header=1,sep='\t')
    print(data.columns,)
    print('cnc原始数据量', data.shape)
    data.drop(['<IPHEAD>','<IPTAIL>','<OWNER>','<ISP>','<VIEW>',],axis=1,inplace=True)
    data.to_csv(path+'.csv',encoding='utf-8')#将.str格式文件转换为.csv文件，存回原始文件夹
    #分割出三段
    data['country_code'],data['state'],data['city']=data['<CITY>'].str.split('_',2).str
    country=data.drop_duplicates(subset=['country_code'],keep='first', inplace=False)
    print(country)
    # country.to_csv('../output/cnc/cnc_country.csv')
    print("cnc国家数量",country.shape)
    state=data.drop_duplicates(subset=['state'],keep='first', inplace=False)
    print("cnc省/州数量",state.shape)
    city=data.drop_duplicates(subset=['city'],keep='first', inplace=False)
    print("cnc城市数量",city.shape)
    print('各城市出现次数：',data.city.value_counts())
    data.to_csv('../fileout/cnc/ipb-ips_raw_splited.csv')

if __name__=='__main__':
    #xml_to_csv()
    # maxmind_eval()
    cnc_eval()
