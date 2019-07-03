#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
import re
import pandas as pd

# 判断字符串是否为{空、英文、非英文}
def isEnglish(s):
    if s=='nan':return 0
    else:
        ans = re.search(r"[a-zA-Z\']+$", s)
        return 1 if ans else 2

def city_location_ansys(data="../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv"):
    data=pd.read_csv(data,encoding='utf-8')
    print('原始数据大小',data.shape)
    #判断是否为英文表达，{空值、英文、非英文(包括异常值)}依次对应{0,1,2}
    data['isEnglish']=data['city_name'].astype(str).apply(isEnglish)
    #输出{空值、英文、非英文(包括异常值)}信息
    num_0=list(data['isEnglish']).count(0)
    print("city_name为空的有",num_0)
    num_1=list(data['isEnglish']).count(1)
    print("city_name为英文的有",num_1)
    num_2=list(data['isEnglish']).count(2)
    print("city_name为非英文(包括异常字符)的有",num_2)
    #将非英文部分提取出，以用于数据爬取转换：
    abnormal=data[data['isEnglish'] ==2]
    print(abnormal.shape)
    abnormal.to_csv('../output/GeoIP2-City-Locations-abnormal.csv')

if __name__=='__main__':
    city_location_ansys()
