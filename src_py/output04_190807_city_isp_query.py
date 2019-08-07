#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
'''output04_190807，输入单个ip地址，二分查询输出IP库的city_name和ISP'''
import os
import pandas as pd
from IPy import IP

class BaseQuery():
    '''city_name和ISP查询类的父类'''
    def __init__(self):
        #out data
        self.cnc_out_dir = '../fileout/cnc'
        self.maxmind_out_dir = '../fileout/maxmind'

    def cnc_query(self, database, ip, query_target):
        '''输入单个ip地址，在cnc中二分查询给定target（city_name或ISP）'''
        low = 0
        high = database.shape[0]
        while low <= high:
            mid = (low + high) // 2
            iptail_mid = database['<IPTAIL>'][mid]
            iphead_mid = database['<IPHEAD>'][mid]
            if IP(iptail_mid) >= IP(ip) and IP(iphead_mid) <= IP(ip):
                print('CNC query result：', database[query_target][mid])
                return database[query_target][mid]
            elif IP(iptail_mid) >= IP(ip) and IP(iphead_mid) >= IP(ip):
                high = mid - 1
            else:
                low = mid + 1
        print('No fond in CNC!')
        return -1

    def maxmind_query(self, database, ip, query_target):
        '''输入单个ip地址，在maxmind中二分查询给定target（city_name或ISP）'''
        def binary_query(database, ip):  # 二分查找函数
            low = 0
            high = database.shape[0]
            while low <= high:
                mid = (low + high) // 2
                ip_mid = database['network'][mid]
                if IP(ip) in IP(ip_mid):
                    print('MaxMind query result：', database[query_target][mid])
                    return database[query_target][mid]
                elif IP(ip_mid) >= IP(ip):
                    high = mid - 1
                else:
                    low = mid + 1
            print('No fond in MaxMind!')
            return -1
        if IP(ip).version() == 4:#判断IP地址类型，选择不同database
            binary_query(database[0], ip)
        else:
            binary_query(database[1], ip)


class CitynameQuery(BaseQuery):
    '''输入单个ip地址，二分查找输出cnc和MaxMind两个库的city_name查询结果'''
    def __init__(self):
        super(CitynameQuery,self).__init__()
        # in data
        self.__cnc_data = '../data/ipb-ips_raw.str/ipb-ips_raw'
        self.__maxmind_ipv4_data = "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv4.csv"
        self.__maxmind_ipv6_data = "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv6.csv"
        self.__maxmind_city_data = "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv"
        # out data
        self.__cnc_out_name = 'cnc_for_cityName.csv'
        self.__maxmind_out_name_v4 = 'maxmind_for_cityName_v4.csv'
        self.__maxmind_out_name_v6 = 'maxmind_for_cityName_v6.csv'

    def __get_cnc_cityDatabase(self, ):
        '''把CNC city_name数据库加载入内存，以供查询，并删除无用部分'''
        print('load CNC dataframe.')
        cnc = pd.read_csv(self.__cnc_data + '.str', encoding='utf-8', header=1, sep='\t')
        cnc.drop(['<OWNER>', '<ISP>', '<VIEW>', ], axis=1, inplace=True)  # 仅保留'<IPHEAD>', '<IPTAIL>', '<CITY>'
        cnc.to_csv(self.cnc_out_dir + '/' + self.__cnc_out_name)
        return cnc

    def __get_maxmind_cityDatabase(self, ):
        '''加载maxmind数据库，删除无用部分，以供查询'''
        print('load maxmind dataframe.')
        id_v4 = pd.read_csv(self.__maxmind_ipv4_data, encoding='utf-8')
        id_v6 = pd.read_csv(self.__maxmind_ipv6_data, encoding='utf-8')
        bad_cols = [col for col in id_v4.columns if col not in ['network', 'geoname_id']]  # 仅保留network和geoname_id
        for df in [id_v4, id_v6]:
            df.drop(bad_cols, axis=1, inplace=True)
            df.drop_duplicates(subset=['geoname_id', ], keep='first', inplace=True)
        city = pd.read_csv(self.__maxmind_city_data, encoding='utf-8')
        city.fillna('qita', inplace=True)  # 填充所有缺失值,并替换源数据
        # 仿照cnc的格式，创建新的city_name:continent_code + country_iso_code + subdivision_1_name + city_name
        city['city_name_new'] = city['continent_code'] + '_' + city['country_iso_code'] + '_' \
                                + city['subdivision_1_name'] + '_' + city['city_name']
        database_v4 = pd.merge(id_v4, city, on='geoname_id')
        database_v6 = pd.merge(id_v6, city, on='geoname_id')
        bad_cols = [col for col in database_v4.columns if col not in ['network', 'city_name_new']]
        for df, save_name in zip([database_v4, database_v6], [self.__maxmind_out_name_v4, self.__maxmind_out_name_v6]):
            df.drop(bad_cols, axis=1, inplace=True)
            df.to_csv(self.maxmind_out_dir + '/' + save_name)
        return (database_v4, database_v6)

    def cnc_city_query(self, database, ip):
        """继承父类cnc查询，查询目标为city"""
        super().cnc_query(database,ip,query_target='<CITY>')

    def maxmind_city_query(self, database, ip):
        """继承父类maxmind查询，查询目标city_name"""
        super().maxmind_query(database,ip,query_target='city_name_new')

    def load_citynameDatabase_faster(self, ):
        """判断cnc和maxmind是否已经处理好保存，避免每次查询都要先做处理，以此加快非首次运行的加载速度"""
        files_cnc = os.listdir(self.cnc_out_dir)
        files_maxmind = os.listdir(self.maxmind_out_dir)
        if self.__cnc_out_name not in files_cnc or self.__maxmind_out_name_v4 not in files_maxmind \
                or self.__maxmind_out_name_v6 not in files_maxmind:
            print('First run, reloading database...')
            cnc = self.__get_cnc_cityDatabase()
            maxmind = self.__get_maxmind_cityDatabase()
        else:
            cnc = pd.read_csv(self.cnc_out_dir + '/' + self.__cnc_out_name)
            maxmind_v4 = pd.read_csv(self.maxmind_out_dir + '/' + self.__maxmind_out_name_v4)
            maxmind_v6 = pd.read_csv(self.maxmind_out_dir + '/' + self.__maxmind_out_name_v6)
            maxmind = (maxmind_v4, maxmind_v6)
        return cnc, maxmind

    def console_quering(self, cnc, maxmind):
        """在控制台输入ip地址，以直接进行多次查询,按q键退出查询"""
        while True:
            print('_' * 50)
            ip = str(input('Input IP to query city_name,or q/Q to quit：'))
            if ip == 'q' or ip == 'Q':
                break
            else:
                self.cnc_city_query(cnc, ip)
                self.maxmind_city_query(maxmind, ip)


class ISPQuery(BaseQuery):
    '''输入单个ip地址，二分查找输出cnc和MaxMind两个库的ISP结果'''
    def __init__(self):
        super(ISPQuery,self).__init__()
        # in data
        self.__cnc_isp = '../data/ipb-ips_raw.str/ipb-ips_raw.str'
        self.__maxmind_ispv4 = '../data/GeoIP2-ISP-CSV/GeoIP2-ISP-CSV_20190618/GeoIP2-ISP-Blocks-IPv4.csv'
        self.__maxmind_ispv6 = '../data/GeoIP2-ISP-CSV/GeoIP2-ISP-CSV_20190618/GeoIP2-ISP-Blocks-IPv6.csv'
        # out data
        self.__cnc_ispData = 'cnc_for_isp.csv'
        self.__maxmind_ispData_v4 = 'maxmind_ipv4_for_isp.csv'
        self.__maxmind_ispData_v6 = 'maxmind_ipv6_for_isp.csv'

    def get_cnc_ispDatabase(self, ):
        '''cnc isp分析、处理后保存'''
        print('load cnc dataframe.')
        cnc = pd.read_csv(self.__cnc_isp, encoding='utf-8', header=1, sep='\t')
        cnc.drop(['<OWNER>', '<CITY>', '<VIEW>', ], axis=1, inplace=True)  # 仅保留'<IPHEAD>', '<IPTAIL>', '<ISP>'
        print('CNC ISP describe：')
        print(cnc.describe())
        cnc.to_csv(self.cnc_out_dir + '/' + self.__cnc_ispData)
        return cnc

    def get_maxmind_ispDatabase(self, ):
        '''maxmind isp分析、处理后保存'''
        print('load maxmind dataframe.')
        ispv4 = pd.read_csv(self.__maxmind_ispv4, encoding='utf-8')
        ispv6 = pd.read_csv(self.__maxmind_ispv6, encoding='utf-8')
        bad_cols = [col for col in ispv4.columns if col not in ['network', 'isp']]  # 仅保留network和isp
        for df in [ispv4, ispv6]:
            df.drop(bad_cols, axis=1, inplace=True)
        maxmind = pd.concat([ispv4, ispv6], axis=0, ignore_index=True)
        print('MaxMind ISP describe：')
        print(maxmind.describe())
        ispv4.to_csv(self.maxmind_out_dir + '/' + self.__maxmind_ispData_v4)
        ispv6.to_csv(self.maxmind_out_dir + '/' + self.__maxmind_ispData_v6)
        return (ispv4, ispv6)

    def load_ispdatabase_faster(self, ):
        files_cnc = os.listdir(self.cnc_out_dir)
        files_maxmind = os.listdir(self.maxmind_out_dir)
        if self.__cnc_ispData not in files_cnc or self.__maxmind_ispData_v4 not in files_maxmind \
                or self.__maxmind_ispData_v6 not in files_maxmind:
            print('First run, reloading database...')
            cnc = self.get_cnc_ispDatabase()
            maxmind = self.get_maxmind_ispDatabase()
        else:
            cnc = pd.read_csv(self.cnc_out_dir + '/' + self.__cnc_ispData)
            maxmind_v4 = pd.read_csv(self.maxmind_out_dir + '/' + self.__maxmind_ispData_v4)
            maxmind_v6 = pd.read_csv(self.maxmind_out_dir + '/' + self.__maxmind_ispData_v6)
            maxmind=(maxmind_v4,maxmind_v6)
        return cnc, maxmind

    def cnc_isp_query(self, database, ip):
        '''继承父类cnc查询，查询目标为ISP'''
        super().cnc_query(database,ip,query_target='<ISP>')

    def maxmind_isp_query(self, database, ip):
        '''继承父类maxmind查询，查询目标为ISP'''
        super().maxmind_query(database,ip,query_target='isp')

    def console_quering(self, cnc, maxmind):
        """在控制台输入ip地址，以直接进行多次查询,按q键退出查询"""
        while True:
            print('_' * 50)
            ip = str(input('Input IP to query ISP,or q/Q to quit：'))
            if ip == 'q' or ip == 'Q':
                break
            else:
                self.cnc_isp_query(cnc, ip)
                self.maxmind_isp_query(maxmind, ip)


if __name__ == '__main__':
    '''city_name查询'''
    cq = CitynameQuery()
    cnc, maxmind = cq.load_citynameDatabase_faster()  # 载入cnc和maxmind city_name数据库
    # 方式1：以控制台输入ip地址的方式查询，如输入1.2.2.0
    cq.console_quering(cnc, maxmind)
    # 方式2：调用api查询
    # cq.cnc_city_query(cnc,ip='1.0.0.0')
    # cq.maxmind_city_query(maxmind,ip='1.0.0.0')

    '''ISP查询'''
    iq = ISPQuery()
    cnc, maxmind = iq.load_ispdatabase_faster()  # 载入cnc和maxmind ISP数据库
    # 方式1：以控制台输入ip地址的方式查询
    iq.console_quering(cnc, maxmind)
    # 方式2，调用api查询
    # iq.cnc_isp_query(cnc,ip='1.0.0.0')
    # iq.maxmind_isp_query(maxmind,ip='1.0.0.0')
