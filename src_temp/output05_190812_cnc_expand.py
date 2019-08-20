#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
"""Output_05_190812_cnc_expand"""
import pandas as pd
from IPy import IP
from tqdm import tqdm


def get_cnc_base(path='../data/ipb-ips_raw.str/ipb-ips_raw'):
    """Load city_name in CNC."""
    df = pd.read_csv(path+'.str', encoding='utf-8', header=1, sep='\t')
    df.drop(['<OWNER>', '<ISP>', '<VIEW>', ], axis=1, inplace=True)
    df.drop_duplicates(keep='first', inplace=True)
    df['country_code'], df['state'], df['city'] = df['<CITY>'].str.split('_', 2).str
    df['state'] = df['state'].apply(lambda x: str(x).capitalize())
    df['city'] = df['city'].apply(lambda x: str(x).capitalize())
    # df.to_csv('../fileout/cnc01_base.csv')
    return df


def get_continent_country_from_maxmind(
        data='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv'):
    """Get continent-country from MaxMind."""
    df = pd.read_csv(data)
    cols = ['continent_code', 'country_iso_code', ]
    bad_cols = [col for col in df.columns if col not in cols]
    df.drop(bad_cols, axis=1, inplace=True)
    df.drop_duplicates(keep='first', inplace=True)
    df.dropna(subset=['country_iso_code'], inplace=True)
    df.fillna('NA', inplace=True)  # North America(NA) was misidentified as NAN
    # df.to_csv('../fileout/maxmind_continent-country.csv')
    return df


def expand_cnc_with_continent():
    """Merge continent-country to CNC."""
    cnc = get_cnc_base()
    continent_country = get_continent_country_from_maxmind()
    std = pd.merge(left=cnc, right=continent_country,
                   left_on='country_code', right_on='country_iso_code')
    std.drop('country_iso_code', axis=1, inplace=True)
    std['cnc_city'] = std['continent_code'] + '_' + std['country_code'] + std['state'] + std['city']
    bad_cols = [col for col in std.columns if col not in ['<IPHEAD>', '<IPTAIL>', 'cnc_city']]
    std.drop(bad_cols, axis=1, inplace=True)
    # std.to_csv('../fileout/cnc02_continent.csv', index=None)
    return std


def get_maxmind_cityDatabase():
    """Load city_name in MaxMind."""
    print('load maxmind dataframe ...')
    id_v4 = pd.read_csv(
        "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv4.csv", encoding='utf-8')
    id_v6 = pd.read_csv(
        "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv6.csv", encoding='utf-8')
    bad_cols = [col for col in id_v4.columns if col not in ['network', 'geoname_id']]
    for df in [id_v4, id_v6]:
        df.drop(bad_cols, axis=1, inplace=True)
        df.drop_duplicates(subset=['geoname_id', ], keep='first', inplace=True)
    city = pd.read_csv(
        "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv", encoding='utf-8')
    city.fillna('Qita', inplace=True)
    # Creat new city_name:continent_code + country_iso_code + subdivision_1_name + city_name
    city['maxmind_city'] = city['continent_code'] + '_' + city['country_iso_code'] + '_' \
        + city['subdivision_1_name'] + '_' + city['city_name']
    database_v4 = pd.merge(id_v4, city, on='geoname_id')
    database_v6 = pd.merge(id_v6, city, on='geoname_id')
    bad_cols = [col for col in database_v4.columns if col not in ['network', 'maxmind_city']]
    for df, save_name in zip([database_v4, database_v6], ['maxmind_for_cityName_v4.csv', 'maxmind_for_cityName_v6.csv']):
        df.drop(bad_cols, axis=1, inplace=True)
        df.to_csv('../fileout/cnc_expand' + '/' + save_name, index=None)
    return (database_v4, database_v6)


def maxmind_query(database, ip):
    """Query city_name in MaxMind."""
    def binary_query(database, ip):
        low, high = 0, database.shape[0]-1
        while low <= high:
            mid = (low + high) // 2
            ip_, ip_mid = IP(ip), IP(database['network'][mid])
            if ip_ in ip_mid:
                return database['maxmind_city'][mid]
            elif ip_mid >= ip_:
                high = mid - 1
            else:
                low = mid + 1
        return ''
    if IP(ip).version() == 4:
        return binary_query(database[0], ip)
    else:
        return binary_query(database[1], ip)


def expand_cnc_with_maxmind():
    """Expand CNC using MaxMind."""
    cnc = pd.read_csv('../fileout/cnc_expand/cnc02_continent.csv')
    mmv4 = pd.read_csv('../fileout/cnc_expand/maxmind_for_cityName_v4.csv')
    mmv6 = pd.read_csv('../fileout/cnc_expand/maxmind_for_cityName_v6.csv')
    database = (mmv4, mmv6,)
    print(cnc.shape, cnc.columns)
    print(mmv4.shape, mmv4.columns)
    print(mmv6.shape, mmv6.columns)
    print('matching...')
    tqdm.pandas()
    # cnc=cnc.loc[:10000].copy()
    print(cnc.shape, cnc.columns)
    cnc['maxmind_city'] = cnc['<IPHEAD>'].progress_apply(lambda x: maxmind_query(database, x))
    print(cnc.shape, cnc.columns)
    # cnc.to_csv('../fileout/cnc03_expand_with_maxmind.csv')


if __name__ == '__main__':
    expand_cnc_with_continent()
    # get_maxmind_cityDatabase()
    # expand_cnc_with_maxmind()
