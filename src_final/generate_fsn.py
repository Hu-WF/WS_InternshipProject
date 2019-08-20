#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
"""
|-generate_fsn.py
    |-GenerateFSN  # Generate standard FSN.csv from CNC and MaxMind.
    |-FSNProcessing  # Generate FSN_with_IP.csv from CNC-IP and MaxMind-IP.
    |-generate_fsn_fsnip  # Generate both FSN.csv and FSN_with_IP.csv.
"""
import os
import re
import time
import pandas as pd
import unihandecode
from IPy import IP

OUTPUT_DIR = '../fileout'
try:
    os.makedirs(OUTPUT_DIR)
except:
    pass


class GenerateFSN():
    """Establish city FSN through CNC and MaxMind.
    FSN=<continent_code><country_code><subdivision_1_iso_code><city_name>
    Contains the following operations:
        ①.Obtain the union part of CNC and MaxMind: CNC_=[(CNC)U(MaxMind)]-(MaxMind)；
        ②.Get <continent_code>-<country_code> from MaxMind,and merge to CNC；
        ③.Get <subdivision_1_iso_code>-<subdivision_1_name> from MaxMind,and merge to CNC；
        ④.Generate CNC-FSN and MaxMind-FSN,merge into one FSN.
        ⑤.Convert FSN-Latin into FSN-English to standardize FSN
    """

    def __init__(self, cnc_ips_path='../data/ipb-ips_raw.str/ipb-ips_raw.str',
                 maxmind_city_path='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv'):
        self._cnc = pd.read_csv(
            cnc_ips_path, encoding='utf-8', header=1, sep='\t')
        self._maxmind = self._load_maxmind(maxmind_city_path)
        self._fsn_path = OUTPUT_DIR+'/'+'FSN_'+time.strftime("%Y-%m-%d", time.localtime())+'.csv'

    def _load_maxmind(self, path):
        """Load and process MaxMind"""
        df = pd.read_csv(path,)
        df = df[['continent_code', 'country_iso_code',
                 'subdivision_1_iso_code', 'subdivision_1_name', 'city_name', ]]
        # North America(NA) was misidentified as NAN
        df['continent_code'].fillna('NA', inplace=True)
        df.dropna(inplace=True)
        return df

    def _load_cnc(self, ignore_ip=True):
        """Load CNC and separate 'contry_code', 'state', 'city' from '<CITY>'"""
        pd.set_option('mode.chained_assignment', None)  # Close Warning
        df = self._cnc
        if ignore_ip:  # For GenerateFSN
            df.drop(['<IPHEAD>', '<IPTAIL>', '<OWNER>',
                     '<ISP>', '<VIEW>', ], axis=1, inplace=True)
        else:  # For FSNProcessing
            df.drop(['<OWNER>', '<ISP>', '<VIEW>', ], axis=1, inplace=True)
        df.drop_duplicates(keep='first', inplace=True)
        df['country_code'], df['state'], df['city'] = df['<CITY>'].str.split(
            '_', 2).str
        for col in ['country_code', 'state', 'city']:
            df = df[df[col] != 'qita']
        df['state'] = df['state'].apply(lambda x: str(x).capitalize())
        df['city'] = df['city'].apply(lambda x: str(x).capitalize())
        return df

    def _filter_cnc_based_on_maxmind(self, cnc):
        """Obtain the union part of CNC and MaxMind: CNC_=[(CNC)U(MaxMind)]-(MaxMind)"""
        maxmind = self._maxmind

        # CNC 'city' processing
        def cnc_city_processing(city):
            city_ = city.split('-')
            city__ = map(lambda s: s.capitalize(), city_)
            return ' '.join(city__)
        cnc['city'] = cnc['city'].apply(lambda city: cnc_city_processing(city))
        # CNC 'state' processing,convert some abnormal 'state'
        cnc['state'] = cnc['state'].replace('Rio-de-janeiro', 'Rio de Janeiro')
        cnc['state'] = cnc['state'].replace(
            'Newfoundland-and-labrador', 'Newfoundland and Labrador')
        cnc['state'] = cnc['state'].replace('Fukushima', 'Fukushima-ken')
        cnc['state'] = cnc['state'].replace('Bucureti', 'Bucuresti')
        cnc['state'] = cnc['state'].replace('Tatarstan', 'Tatarstan Republic')
        cnc['state'] = cnc['state'].replace(
            'Districtofcolumbia', 'District of Columbia')
        cnc['state'] = cnc['state'].replace('Newyork', 'New York')
        cnc['state'] = cnc['state'].replace(
            'Neimenggu', 'Inner Mongolia Autonomous Region')
        cnc['state'] = cnc['state'].replace('Xizang', 'Tibet')

        # Mark the part of CNC_=[(CNC)U(MaxMind)]-(MaxMind) as False
        def whether_in_maxmind(ele, sheet):
            if ele in sheet['city_name'].values:
                return True
            else:
                return False
        cnc['in_maxmind'] = cnc['city'].apply(
            lambda x: whether_in_maxmind(x, maxmind))
        cnc = cnc[cnc['in_maxmind'] == False]
        return cnc

    def _get_key_value_from_maxmind(self, key, value):
        """Obtain <key>-<value> pair through MaxMind.
        Sample input:
            key='continent_code', value='country_iso_code';
            key='subdivision_1_code', value='subdivision_1_name';
        """
        df = self._maxmind[[key, value, ]]
        df.drop_duplicates(keep='first', inplace=True)
        return df

    def _get_continent_country_from_maxmind(self,):
        """Obtain <continent>-<country> pair through MaxMind."""
        return self._get_key_value_from_maxmind('continent_code', 'country_iso_code')

    def _get_sub1code_sub1name_from_maxmind(self,):
        """Obtain <subdivision_1_code>-<subdivision_1_name> pair through MaxMind."""
        return self._get_key_value_from_maxmind('subdivision_1_iso_code', 'subdivision_1_name')

    def _expand_cnc_with_maxmind(self, cnc, continent_country, sub1code_sub1name):
        """Expand CNC.columns into
        ['continent_code', 'country_code', 'state', 'subdivision_1_iso_code', 'city']
        through MaxMind's key-value
        """
        std = pd.merge(left=cnc, right=continent_country,
                       left_on='country_code', right_on='country_iso_code')
        std = pd.merge(left=std, right=sub1code_sub1name,
                       left_on='state', right_on='subdivision_1_name', how='left')
        # Finally, if 'subdivision_1_iso_code'is still partially Nan, fill it with 'state'.
        std.loc[std['subdivision_1_iso_code'].isnull(),
                'subdivision_1_iso_code'] = std.loc[std['subdivision_1_iso_code'].isnull(), 'state']
        if '<IPHEAD>' in std.columns:
            std = std[['<IPHEAD>', '<IPTAIL>', 'continent_code',
                       'country_code', 'state', 'subdivision_1_iso_code', 'city']]
        else:
            std = std[['continent_code', 'country_code',
                       'state', 'subdivision_1_iso_code', 'city']]
        return std

    def _generate_and_merge_fsn(self, cnc):
        """Generate CNC-FSN and MaxMind-FSN, then merge into one FSN"""
        cnc['FSN'] = cnc['continent_code'] + '.' + cnc['country_code'] + '.' + cnc[
            'subdivision_1_iso_code'] + '.' + cnc['city']
        cnc = cnc[['FSN', ]]
        print('CNC_FSN_shape:', cnc.shape)
        maxmind = self._maxmind[[
            'continent_code', 'country_iso_code', 'subdivision_1_iso_code', 'city_name']]
        maxmind['continent_code'].fillna('NA', inplace=True)
        maxmind['FSN'] = maxmind['continent_code'] + '.' + maxmind['country_iso_code'] + '.' + maxmind[
            'subdivision_1_iso_code'] + '.' + maxmind['city_name']
        maxmind = maxmind[['FSN', ]]
        maxmind.drop_duplicates(subset=['FSN'], keep='first', inplace=True)
        print('MaxMind_FSN_shape', maxmind.shape)
        fsn = pd.concat([cnc, maxmind], axis=0, ignore_index=True)
        fsn.drop_duplicates(keep='first', inplace=True)
        print('FSN_shape', fsn.shape)
        fsn.sort_values(axis=0, by="FSN", kind='quicksort',
                        inplace=True)  # Sort FSN
        fsn['FSN'] = fsn['FSN'].str.upper()  # Converting FSN to uppercase
        fsn['continent_code'], fsn[
            'country_iso_code'], fsn['subdivision_1_iso_code'], fsn['city'] = fsn['FSN'].str.split('.', 3).str
        return fsn

    def _standardized_fsn(self, fsn):
        """Convert FSN-Latin part into FSN-English to standardize FSN:input original FSN，output FSN_std."""
        # Determine whether the FSN is (English or numeric).
        def is_English(s):
            ans = re.search(r"[0-9A-Z\']+$", s)
            return True if ans else False
        fsn['isEnglish'] = fsn['FSN'].astype(str).apply(is_English)
        print("FSN_English&Num shape=", list(fsn['isEnglish']).count(True))
        print("FSN_Latin shape=", list(fsn['isEnglish']).count(False))
        fsn_en = fsn[fsn['isEnglish'] == True]
        fsn_latin = fsn[fsn['isEnglish'] == False]
        for df in [fsn_latin, fsn_en]:
            df.drop(['isEnglish', ], axis=1, inplace=True)
        # Convert FSN_Latin into FSN_English
        fsn_latin['FSN_std'] = fsn_latin['FSN'].apply(
            lambda x: unihandecode.unidecode(x))
        fsn_latin = fsn_latin[['FSN', 'FSN_std', 'continent_code',
                               'country_iso_code', 'subdivision_1_iso_code', 'city']]
        for i in "()/'`!- _":
            fsn_latin['FSN_std'] = fsn_latin['FSN_std'].str.replace(i, '')
        # fsn_latin.to_csv(OUTPUT_DIR+'/'+'FSN-latin_part_stded.csv',index=None)
        fsn_en['FSN_std'] = fsn_en['FSN']
        fsn_std = pd.concat([fsn_latin, fsn_en], axis=0,
                            ignore_index=True, sort=False)
        fsn_std.to_csv(self._fsn_path, index=None)

    def load_cnc_ip_city(self):
        """Convert CNC into standard CNC_FSN through MaxMind."""
        print('Expanding cnc...')
        cnc = self._load_cnc(ignore_ip=False)  # Load with IP
        continent_country = self._get_continent_country_from_maxmind()
        sub1code_sub1name = self._get_sub1code_sub1name_from_maxmind()
        cnc_std = self._expand_cnc_with_maxmind(
            cnc, continent_country, sub1code_sub1name)
        return cnc_std

    def get_std_fsn(self):
        """Generate std-FSN from CNC and MaxMind."""
        print('Generating std FSN...')
        cnc = self._load_cnc(ignore_ip=True)  # Load CNC without IP.
        cnc_ = self._filter_cnc_based_on_maxmind(cnc)
        c_c = self._get_continent_country_from_maxmind()
        s_s = self._get_sub1code_sub1name_from_maxmind()
        cnc_std = self._expand_cnc_with_maxmind(cnc_, c_c, s_s)
        fsn = self._generate_and_merge_fsn(cnc_std,)
        self._standardized_fsn(fsn)  # generate std FSN.
        print('The generated std FSN is in the following file path:', self._fsn_path)
        return 0


class FSNProcessing():
    """Merge MaxMind_IPv4-FSN, MaxMind_IPv6-FSN and CNC-FSN with FSN, generate FSN_with_IP.csv as follows:
    <IPHEAD>|<IPTAIL>|FSN|FSN_std|continent_code|country_iso_code|subdivision_1_iso_code|city
    eg:1.0.0.0|1.0.0.255|OC.AU.SA.PLYMPTON|OC.AU.SA.PLYMPTON|OC|AU|SA|PLYMPTON
    """
    def __init__(self, fsn_path=OUTPUT_DIR+'/'+'FSN_'+time.strftime("%Y-%m-%d", time.localtime())+'.csv',
                 maxmind_ipv4_path="../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv4.csv",
                 maxmind_ipv6_path="../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv6.csv",
                 maxmind_city_path="../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv",
                 ):
        self._fsn = pd.read_csv(fsn_path,)
        self._maxmind_ipv4_path = maxmind_ipv4_path
        self._maxmind_ipv6_path = maxmind_ipv6_path
        self._maxmind_city_path = maxmind_city_path
        self._fsn_with_ip_path = OUTPUT_DIR+'/'+'FSN_with_IP_' + time.strftime("%Y-%m-%d", time.localtime()) + '.csv'

    def _load_maxmind_ip_fsn(self):
        """Load MaxMind IP-FSN，drop useless columns"""
        print("Loading MaxMind 'GeoIP2-City-IPv4/IPv6.csv' and 'GeoIP2-City-Locations-en.csv' ...")
        mmv4 = pd.read_csv(self._maxmind_ipv4_path,
                           encoding='utf-8', low_memory=False)
        mmv6 = pd.read_csv(self._maxmind_ipv6_path,
                           encoding='utf-8', low_memory=False)
        city = pd.read_csv(self._maxmind_city_path, encoding='utf-8',)
        mmv4 = mmv4[['network', 'geoname_id']]
        mmv6 = mmv6[['network', 'geoname_id']]
        city = city[['geoname_id', 'continent_code', 'country_iso_code',
                     'subdivision_1_iso_code', 'subdivision_1_name', 'city_name', ]]
        city['continent_code'].fillna('NA', inplace=True)
        city.dropna(inplace=True)
        city['FSN'] = city['continent_code'] + '.' + city['country_iso_code'] + '.' + city[
            'subdivision_1_iso_code'] + '.' + city['city_name']
        city = city[['geoname_id', 'FSN']]
        ipv4_fsn = pd.merge(mmv4, city, on='geoname_id')
        ipv6_fsn = pd.merge(mmv6, city, on='geoname_id')
        ipv4_fsn = ipv4_fsn[['network', 'FSN']]
        ipv6_fsn = ipv6_fsn[['network', 'FSN']]
        print("Converting 'network' to '<IPHEAD>-<IPTAIL>' in MaxMind with IPy module, takes about 4 minutes... ")
        for df in [ipv4_fsn, ipv6_fsn]:
            # Convert network into <IPHEAD>-<IPTAIL>
            df['network'] = df['network'].apply(lambda ip: IP(ip).strNormal(3))
            df['<IPHEAD>'], df['<IPTAIL>'] = df['network'].str.split('-', 1).str
            df['FSN'] = df['FSN'].str.upper()  # Converting FSN to uppercase
        ipv4_fsn = ipv4_fsn[['<IPHEAD>', '<IPTAIL>', 'FSN']]
        ipv6_fsn = ipv6_fsn[['<IPHEAD>', '<IPTAIL>', 'FSN']]
        print('Create IPv4_FSN,IPv6_FSN sucess.')
        return ipv4_fsn, ipv6_fsn

    def _load_cnc_ip_fsn(self):
        """Load and generate CNC IP-FSN through GenerateFSN module."""
        print('Loading CNC IP-FSN...')
        gf = GenerateFSN()
        cnc = gf.load_cnc_ip_city()
        cnc['FSN'] = cnc['continent_code'] + '.' + cnc[
            'country_code'] + '.'+ cnc['subdivision_1_iso_code'] + '.' + cnc['city']
        cnc['FSN'] = cnc['FSN'].str.upper()
        cnc_std = cnc[['<IPHEAD>', '<IPTAIL>', 'FSN', ]]
        print('load cnc_fsn sucess.')
        return cnc_std

    def _merge_ip_fsn(self, fsn, mm_ipv4_fsn, mm_ipv6_fsn, cnc_ip_fsn):
        """Merge MaxMind_IPv4-FSN, MaxMind_IPv6-FSN and CNC-FSN with FSN, generate FSN_with_IP.csv."""
        print('MaxMind_IPv4-FSN.shape:', mm_ipv4_fsn.shape,)
        print('MaxMind_IPv6-FSN.shape:', mm_ipv6_fsn.shape, )
        print('CNC_IPs-FSN.shape:', cnc_ip_fsn.shape,)
        print('FSN-std.shape:', fsn.shape,)
        print('Merging...')
        std1 = pd.merge(left=mm_ipv4_fsn, right=fsn, left_on='FSN',
                        right_on='FSN', how='inner')  # maxmind_IPv4_fsn
        std2 = pd.merge(left=mm_ipv6_fsn, right=fsn, left_on='FSN',
                        right_on='FSN', how='inner')  # maxmind_IPv6_fsn
        std3 = pd.merge(left=cnc_ip_fsn, right=fsn, left_on='FSN',
                        right_on='FSN', how='inner')  # cnc_fsn
        std = pd.concat([std1, std2, std3], axis=0,)
        print('FSN_with_IP.shape:', std.shape)
        print('Saving...')
        std.to_csv(self._fsn_with_ip_path, index=None)

    def get_std_fsn_with_ip(self,):
        """Generate FSN_with_IP.csv, as shown below:
         <IPHEAD>|<IPTAIL>|FSN|FSN_std|continent_code|country_iso_code|subdivision_1_iso_code|city
        eg:1.0.0.0|1.0.0.255|OC.AU.SA.PLYMPTON|OC.AU.SA.PLYMPTON|OC|AU|SA|PLYMPTON
        """
        print('Generating FSN_with_IP...')
        fsn = self._fsn
        mm_ipv4_fsn, mm_ipv6_fsn = self._load_maxmind_ip_fsn()
        cnc_ip_fsn = self._load_cnc_ip_fsn()
        self._merge_ip_fsn(fsn, mm_ipv4_fsn, mm_ipv6_fsn, cnc_ip_fsn)
        print('The generated FSN_with_IP is in the following file path:',self._fsn_with_ip_path)


def generate_fsn_fsnip(cnc_ips_path, maxmind_city_path, maxmind_IPv4_path, maxmind_IPv6_path,ignore_fsnip=False):
    """Generate FSN.csv and FSN_with_IP.csv through GenerateFSN and FSNProcessing module.
    default ignore_fsnip =False,if True, FSN_with_IP.csv will not be generated.
     Format of Code and Data:
        |-data
            |-...
                |-ipb-ips_raw.str
                |-GeoIP2-City-Locations-en.csv
                |-GeoIP2-City-Blocks-IPv4.csv
                |-GeoIP2-City-Blocks-IPv6.csv
        |-src
            |-generate_fsn.py
        |-fileout
            |-FSN_20xx-xx-xx.csv
            |-FSN_with_IP_20xx-xx-xx.csv
    """
    gf = GenerateFSN(cnc_ips_path=cnc_ips_path,
                     maxmind_city_path=maxmind_city_path,)
    gf.get_std_fsn()
    if ignore_fsnip == False:  # Do not generate FSN_with_IP.csv
        fp = FSNProcessing(maxmind_ipv4_path=maxmind_IPv4_path,
                           maxmind_ipv6_path=maxmind_IPv6_path,
                           maxmind_city_path=maxmind_city_path)
        fp.get_std_fsn_with_ip()
    return 0


if __name__ == '__main__':
    # # Generate FSN through CNC and MaxMind-city：
    # gf=GenerateFSN(cnc_ips_path='../data/ipb-ips_raw.str/ipb-ips_raw.str',
    #                maxmind_city_path='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv')
    # print(gf.__doc__)
    # gf.get_std_fsn()
    #
    #
    # #Generate FSN_with_IP through CNC, MaxMind-IPv4 and MaxMind-IPv6：
    # fp = FSNProcessing(maxmind_ipv4_path='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv4.csv',
    #                    maxmind_ipv6_path='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv6.csv',
    #                    maxmind_city_path="../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv",)
    # print(fp.__doc__)
    # fp.get_std_fsn_with_ip()

    print(generate_fsn_fsnip.__doc__)
    generate_fsn_fsnip(cnc_ips_path='../data/ipb-ips_raw.str/ipb-ips_raw.str',
                       maxmind_city_path='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv',
                       maxmind_IPv4_path='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv4.csv',
                       maxmind_IPv6_path='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv6.csv',
                       ignore_fsnip=False)

