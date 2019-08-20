#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
"""Output_02_190704_geo_inverse_coding
Get geographical location by IP address (Batch query).
"""
import re
import functools
import pandas as pd
from tqdm import tqdm
import geopy.geocoders  # https://geopy.readthedocs.io/en/stable/
# from geopy.geocoders import Nominatim
from geopy.geocoders import GoogleV3, Yandex, Photon
from geopy.extra.rate_limiter import RateLimiter  # for dataframe quering


def is_en(s):
    """Judge string status of MaxMind { Null, English, non-English }"""
    if s == 'nan':
        return 0
    else:
        ans = re.search(r"[a-zA-Z\']+$", s)
        return 1 if ans else 2


def city_location_ansys(data="../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv"):
    """Judge and get non-English part of city_name in MaxMind"""
    df = pd.read_csv(data, encoding='utf-8')
    df['isEnglish'] = df['city_name'].astype(str).apply(is_en)
    num_0 = list(df['isEnglish']).count(0)
    print("city_name == Null:", num_0)
    num_1 = list(df['isEnglish']).count(1)
    print("city_name == English:", num_1)
    num_2 = list(df['isEnglish']).count(2)
    print("city_name == Non-English:", num_2)
    non_english = df[df['isEnglish'] == 2]
    non_english.to_csv('../fileout/GeoIP2-City-Locations-non-English.csv')
    return non_english


UA = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0"
# default param
geopy.geocoders.options.default_user_agent = UA
# Time to wait for the response of the geocoding service before throwing an exception (s)
geopy.geocoders.options.default_timeout = 7
# set Geo-coder
# geolocator=GoogleV3(user_agent=UA)#Since 2018/07, Google has required API keys for every request.
# geolocator=Nominatim(user_agent=UA)
# geolocator = Yandex(user_agent=UA, lang='en')
geolocator = Photon()  # Do not need API key.


def geo_reverse_coding(latitude, longitude, UA=UA):
    """Given longitude and latitude, return to a geographic location."""
    address = str(latitude) + ',' + str(longitude)
    location = geolocator.reverse(address, language='en')
    print('Lat&Lon:', address)
    print('location:', location.address)
    print('location.raw', location.raw)
    return location


def get_geoID():
    """Get a dataframe from MaxMind which key=geoname_id and value=lat&lon"""
    id_v4 = pd.read_csv(
        "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv4.csv", encoding='utf-8')
    id_v6 = pd.read_csv(
        "../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Blocks-IPv6.csv", encoding='utf-8')
    print("IPv4_shape=", id_v4.shape, "IPv6_shape=", id_v6.shape)
    bad_cols = ['network', 'registered_country_geoname_id', 'represented_country_geoname_id', 'is_anonymous_proxy',
                'is_satellite_provider', 'postal_code', 'accuracy_radius']
    for df in [id_v4, id_v6]:
        df.drop(bad_cols, axis=1, inplace=True)
    print('remaining_columns:', id_v4.columns)
    geoID = pd.concat([id_v4, id_v6], axis=0, ignore_index=True)
    del id_v4
    del id_v6
    print("get geoID!")
    print("geoID shape:", geoID.shape)
    geoID.drop_duplicates(subset=['geoname_id', ], keep='first', inplace=True)
    print("geoID shape after duplicates:", geoID.shape)
    return geoID


def merge_geoID_data(geoID, data="../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv"):
    """Merge lat&lon to data."""
    data = pd.read_csv(data, encoding='utf-8')
    print('merging...')
    res = pd.merge(data, geoID, on='geoname_id')
    res.to_csv('../fileout/GeoIP2-City-Locations-abnormal-vsLatLot.csv')
    return res


def batch_geo_reverse_coding(data):
    """Batch geo inverse coding."""
    # Modify default params using functools.partial
    georeverse = functools.partial(geolocator.reverse, language='en')
    location = RateLimiter(georeverse, min_delay_seconds=1, )
    data['latitude'] = data['latitude'].astype(str)
    data['longitude'] = data['longitude'].astype(str)
    data['LatLon'] = data['latitude'].str.cat(data['longitude'], sep=',')
    tqdm.pandas()  # Use tqdm to add a progress bar for pandas(progress_apply).
    data['location_raw'] = data['LatLon'].progress_apply(location)
    # data['city']=data['location_raw'].apply(lambda x:re.findall(r"[(](.*?)[(]",str(x)))
    data.to_csv('../fileout/GeoIP2-City-Locations-abnormal-resRaw.csv')
    return data


def get_finall_result(data='../fileout/GeoIP2-City-Locations-abnormal-resRaw.csv'):
    """Drop temp columns to get finall result."""
    data = pd.read_csv(data, encoding='utf-8')
    good_cols = ['geoname_id', 'locale_code', 'continent_code', 'continent_name', 'country_iso_code', 'country_name',
                 'subdivision_1_iso_code', 'subdivision_1_name', 'subdivision_2_iso_code', 'subdivision_2_name', 'city_name',
                 'metro_code', 'time_zone', 'is_in_european_union', ]+['location_raw', ]
    for col in data.columns:
        if col not in good_cols:
            data.drop(col, axis=1, inplace=True)
    data['location_raw'] = data['location_raw'].astype(str)
    data['city'] = data['location_raw'].str.split(',').apply(lambda x: x[0][10:])
    # data['city'] = data['location_raw'].str.split(',')# [10:]
    # data['city']=data['location_raw'].str.split(',')[0][10:]
    data.drop('location_raw', axis=1, inplace=True)
    data.to_csv('../fileout/GeoIP2-City-Locations-abnormal-res.csv')


if __name__ == '__main__':
    # MaxMind analysis：
    city_location_ansys()

    # Reverse:
    geo_reverse_coding(latitude='25.9922', longitude='119.4460')

    # Batch reverse:
    # geoID=get_geoID()
    # data=merge_geoID_data(geoID,)
    # batch_geo_reverse_coding(data)
    # get_finall_result()
