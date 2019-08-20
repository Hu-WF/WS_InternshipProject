#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
'''output04_190807_city_isp_query
Input IP address,output city_name and ISP.'''
import os
import pandas as pd
from IPy import IP
# from tqdm import tqdm


class BaseQuery():
    """The parent class of city_name and ISP query classes."""
    def __init__(self):
        # out data
        self.cnc_out_dir = '../fileout'
        self.maxmind_out_dir = '../fileout'

    def cnc_query(self, database, ip, query_target):
        """Input IP address,get target（city_name or ISP）from CNC."""
        low = 0
        high = database.shape[0]-1
        while low <= high:
            mid = (low + high) // 2
            iptail_mid = database['<IPTAIL>'][mid]
            iphead_mid = database['<IPHEAD>'][mid]
            if IP(iptail_mid) >= IP(ip) and IP(iphead_mid) <= IP(ip):
                print('CNC IP:', database['<IPHEAD>'][mid])
                print('CNC：', database[query_target][mid])
                return database[query_target][mid]
            elif IP(iptail_mid) >= IP(ip) and IP(iphead_mid) >= IP(ip):
                high = mid - 1
            else:
                low = mid + 1
        print('No fond in CNC!')
        return -1

    def maxmind_query(self, database, ip, query_target):
        """Input IP address,get target（city_name or ISP）from MaxMind."""
        def binary_query(database, ip):
            low = 0
            high = database.shape[0]-1
            while low <= high:
                mid = (low + high) // 2
                ip_mid = database['network'][mid]
                if IP(ip) in IP(ip_mid):
                    print('Maxmind IP:', database['network'][mid])
                    print('MaxMind：', database[query_target][mid])
                    return database[query_target][mid]
                elif IP(ip_mid) >= IP(ip):
                    high = mid - 1
                else:
                    low = mid + 1
            print('No fond in MaxMind!')
            return -1
        if IP(ip).version() == 4:
            binary_query(database[0], ip)
        else:
            binary_query(database[1], ip)


class CitynameQuery(BaseQuery):
    """Input IP address,get city_name from CNC and MaxMind."""
    def __init__(self):
        super(CitynameQuery, self).__init__()
        # In data
        self._cnc_data = '../data/ipb-ips_raw.str/ipb-ips_raw'
        self._maxmind_ipv4_data = "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv4.csv"
        self._maxmind_ipv6_data = "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv6.csv"
        self._maxmind_city_data = "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv"
        # Out data
        self._cnc_out_name = 'cnc_for_cityName.csv'
        self._maxmind_out_name_v4 = 'maxmind_for_cityName_v4.csv'
        self._maxmind_out_name_v6 = 'maxmind_for_cityName_v6.csv'

    def _get_cnc_cityDatabase(self, ):
        """Load CNC."""
        print('load CNC dataframe ...')
        cnc = pd.read_csv(self._cnc_data + '.str',encoding='utf-8', header=1, sep='\t')
        cnc.drop(['<OWNER>', '<ISP>', '<VIEW>', ], axis=1, inplace=True)
        cnc.to_csv(self.cnc_out_dir + '/' + self._cnc_out_name)
        return cnc

    def get_maxmind_cityDatabase(self, ):
        """Load MaxMind."""
        print('load maxmind dataframe ...')
        id_v4 = pd.read_csv(self._maxmind_ipv4_data, encoding='utf-8')
        id_v6 = pd.read_csv(self._maxmind_ipv6_data, encoding='utf-8')
        bad_cols = [col for col in id_v4.columns if col not in ['network', 'geoname_id']]
        for df in [id_v4, id_v6]:
            df.drop(bad_cols, axis=1, inplace=True)
            df.drop_duplicates(subset=['geoname_id', ],keep='first', inplace=True)
        city = pd.read_csv(self._maxmind_city_data, encoding='utf-8')
        city.fillna('qita', inplace=True)
        # Creat new city_name:continent_code + country_iso_code + subdivision_1_name + city_name
        city['city_name_new'] = city['continent_code'] + '_' + city['country_iso_code'] + '_' \
            + city['subdivision_1_name'] + '_' + city['city_name']
        database_v4 = pd.merge(id_v4, city, on='geoname_id')
        database_v6 = pd.merge(id_v6, city, on='geoname_id')
        bad_cols = [col for col in database_v4.columns if col not in ['network', 'city_name_new']]
        for df, save_name in zip(
                [database_v4, database_v6], [self._maxmind_out_name_v4, self._maxmind_out_name_v6]):
            df.drop(bad_cols, axis=1, inplace=True)
            df.to_csv(self.maxmind_out_dir + '/' + save_name, index=None)
        return (database_v4, database_v6)

    def cnc_city_query(self, database, ip):
        """Set query target=<CITY> in CNC."""
        super().cnc_query(database, ip, query_target='<CITY>')

    def maxmind_city_query(self, database, ip):
        """Set query target target=city_name in MaxMind."""
        super().maxmind_query(database, ip, query_target='city_name_new')

    def load_citynameDatabase_faster(self, ):
        """Determine whether CNC and MaxMind have been processed to speed up non-first run loading."""
        files_cnc = os.listdir(self.cnc_out_dir)
        files_maxmind = os.listdir(self.maxmind_out_dir)
        if self._cnc_out_name not in files_cnc or self._maxmind_out_name_v4 not in files_maxmind \
                or self._maxmind_out_name_v6 not in files_maxmind:
            print('First run, reloading database...')
            cnc = self._get_cnc_cityDatabase()
            maxmind = self.get_maxmind_cityDatabase()
        else:
            cnc = pd.read_csv(self.cnc_out_dir + '/' + self._cnc_out_name)
            maxmind_v4 = pd.read_csv(
                self.maxmind_out_dir + '/' + self._maxmind_out_name_v4)
            maxmind_v6 = pd.read_csv(
                self.maxmind_out_dir + '/' + self._maxmind_out_name_v6)
            maxmind = (maxmind_v4, maxmind_v6)
        return cnc, maxmind

    def console_quering(self, cnc, maxmind):
        """Query in console,press q/Q to quit."""
        while True:
            print('_' * 50)
            ip = str(input('Input IP to query city_name,or q/Q to quit：'))
            if ip == 'q' or ip == 'Q':
                break
            else:
                self.cnc_city_query(cnc, ip)
                self.maxmind_city_query(maxmind, ip)


class ISPQuery(BaseQuery):
    """Input IP address,get ISP from CNC and MaxMind."""
    def __init__(self):
        super(ISPQuery, self).__init__()
        # In data
        self._cnc_isp = '../data/ipb-ips_raw.str/ipb-ips_raw.str'
        self._maxmind_ispv4 = '../data/GeoIP2-ISP-CSV/GeoIP2-ISP-CSV_20190618/GeoIP2-ISP-Blocks-IPv4.csv'
        self._maxmind_ispv6 = '../data/GeoIP2-ISP-CSV/GeoIP2-ISP-CSV_20190618/GeoIP2-ISP-Blocks-IPv6.csv'
        # Out data
        self._cnc_ispData = 'cnc_for_isp.csv'
        self._maxmind_ispData_v4 = 'maxmind_ipv4_for_isp.csv'
        self._maxmind_ispData_v6 = 'maxmind_ipv6_for_isp.csv'

    def get_cnc_ispDatabase(self, ):
        """Load CNC-ISP."""
        print('load cnc dataframe ...')
        cnc = pd.read_csv(self._cnc_isp, encoding='utf-8', header=1, sep='\t')
        cnc.drop(['<OWNER>', '<CITY>', '<VIEW>', ], axis=1, inplace=True)
        print(cnc.describe())
        cnc.to_csv(self.cnc_out_dir + '/' + self._cnc_ispData)
        return cnc

    def get_maxmind_ispDatabase(self, ):
        """Load MaxMind-ISP."""
        print('load maxmind dataframe ...')
        ispv4 = pd.read_csv(self._maxmind_ispv4, encoding='utf-8')
        ispv6 = pd.read_csv(self._maxmind_ispv6, encoding='utf-8')
        bad_cols = [col for col in ispv4.columns if col not in ['network', 'isp']]
        for df in [ispv4, ispv6]:
            df.drop(bad_cols, axis=1, inplace=True)
        maxmind = pd.concat([ispv4, ispv6], axis=0, ignore_index=True)
        print(maxmind.describe())
        ispv4.to_csv(self.maxmind_out_dir + '/' + self._maxmind_ispData_v4)
        ispv6.to_csv(self.maxmind_out_dir + '/' + self._maxmind_ispData_v6)
        return (ispv4, ispv6)

    def load_ispdatabase_faster(self, ):
        """Determine whether CNC-ISP and MaxMind-ISP have been processed to speed up non-first run loading."""
        files_cnc = os.listdir(self.cnc_out_dir)
        files_maxmind = os.listdir(self.maxmind_out_dir)
        if self._cnc_ispData not in files_cnc or self._maxmind_ispData_v4 not in files_maxmind \
                or self._maxmind_ispData_v6 not in files_maxmind:
            print('First run, reloading database...')
            cnc = self.get_cnc_ispDatabase()
            maxmind = self.get_maxmind_ispDatabase()
        else:
            cnc = pd.read_csv(self.cnc_out_dir + '/' + self._cnc_ispData)
            maxmind_v4 = pd.read_csv(
                self.maxmind_out_dir + '/' + self._maxmind_ispData_v4)
            maxmind_v6 = pd.read_csv(
                self.maxmind_out_dir + '/' + self._maxmind_ispData_v6)
            maxmind = (maxmind_v4, maxmind_v6)
        return cnc, maxmind

    def cnc_isp_query(self, database, ip):
        """Set query target = ISP in CNC."""
        super().cnc_query(database, ip, query_target='<ISP>')

    def maxmind_isp_query(self, database, ip):
        """Set query targer = ISP in MaxMInd."""
        super().maxmind_query(database, ip, query_target='isp')

    def console_quering(self, cnc, maxmind):
        """Query ISP in console,press q/Q to quit."""
        while True:
            print('_' * 50)
            ip = str(input('Input IP to query ISP,or q/Q to quit：'))
            if ip == 'q' or ip == 'Q':
                break
            else:
                self.cnc_isp_query(cnc, ip)
                self.maxmind_isp_query(maxmind, ip)


def query_in_console():
    """Query city_name and ISP in console at the same time."""
    cq = CitynameQuery()
    iq = ISPQuery()
    print('Loading data...')
    cnc_city, maxmind_city = cq.load_citynameDatabase_faster()
    cnc_isp, maxmind_isp = iq.load_ispdatabase_faster()
    while True:
        print('='*50)
        ip = str(input('Input IP to query city_name and ISP,or press q/Q to quit:'))
        if ip == 'q' or ip == 'Q':
            break
        else:
            print('city_name:')
            cq.cnc_city_query(cnc_city, ip)  # cnc city
            cq.maxmind_city_query(maxmind_city, ip)  # maxmind city
            print('ISP:')
            iq.cnc_isp_query(cnc_isp, ip)  # cnc isp
            iq.maxmind_isp_query(maxmind_isp, ip)  # maxmind isp


if __name__ == '__main__':
    # city_name query
    # cq = CitynameQuery()
    # cnc, maxmind = cq.load_citynameDatabase_faster()
    # Way-1:
    # cq.console_quering(cnc, maxmind)
    # Way-2:
    # cq.cnc_city_query(cnc,ip='1.0.0.0')
    # cq.maxmind_city_query(maxmind,ip='1.0.0.0')

    # ISP query
    # iq = ISPQuery()
    # cnc, maxmind = iq.load_ispdatabase_faster()
    # Way-1:
    # iq.console_quering(cnc, maxmind)
    # Way-2:
    # iq.cnc_isp_query(cnc,ip='1.0.0.0')
    # iq.maxmind_isp_query(maxmind,ip='1.0.0.0')

    # city_name and ISP query.'''
    query_in_console()
