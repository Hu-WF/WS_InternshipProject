#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
"""Output_03_190722_geoip_evaluation"""
import pandas as pd
import xml.etree.ElementTree as ET


def xml_to_csv(data="../data/QQ-LocList/LocList.xml"):
    """Load LocList.xml and parse into LocLost.csv."""
    tree = ET.parse(data)
    root = tree.getroot()
    result = []
    for country in root.iter('CountryRegion'):  # country
        country_name, country_code = country.attrib["Name"], country.attrib["Code"]
        for state in country:  # state
            try:
                state_name, state_code = state.attrib['Name'], state.attrib['Code']
            except:
                state_name = state_code = None
            for city in state:  # city
                city_name, city_code = city.attrib['Name'], city.attrib['Code']
                result.append({'country_name': country_name, 'country_code': country_code,
                               'state_name': state_name, 'state_code': state_code,
                               'city_name': city_name, 'city_code': city_code})
    result = pd.DataFrame(result, columns=[
                          'country_name', 'country_code', 'state_name', 'state_code', 'city_name', 'city_code'])
    print('Convert XML to CSV sucess.')
    result.to_csv('../fileout/FSN_fromQQ.csv')
    print("qqLoc.shape =:", result.shape[0])
    country = result.drop_duplicates(subset=['country_name', ], keep='first', inplace=False)
    print("qqLoc.country_name.shape :", country.shape[0])
    state = result.drop_duplicates(subset=['state_name', ], keep='first', inplace=False)
    print("qqLoc.state_name.shape :", state.shape[0])
    city = result.drop_duplicates(subset=['city_name', ], keep='first', inplace=False)
    print("qqLoc.city_name.shape :", city.shape[0])
    return 0


def maxmind_eval(data='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv'):
    """MaxMind evaluation."""
    data = pd.read_csv(data)
    print(data.columns)
    print('MaxMind.shape :', data.shape)
    cols = ['continent_code', 'continent_name','country_iso_code', 'country_name', 'city_name']
    bad_cols = [col for col in data.columns if col not in cols]
    data.drop(bad_cols, axis=1, inplace=True)
    continent = data.drop_duplicates(subset=['continent_name', ], keep='first', inplace=False)
    print('MaxMind.continent_name.shape :', continent.shape)
    country = data.drop_duplicates(
        subset=['country_name', ], keep='first', inplace=False)
    print('MaxMind.country_name.shape :', country.shape)
    city = data.drop_duplicates(subset=['city_name', ], keep='first', inplace=False)
    print('MaxMind.city_name.shape :', city.shape)


def cnc_eval(path='../data/ipb-ips_raw.str/ipb-ips_raw'):
    """CNC evaluation."""
    data = pd.read_csv(path+'.str', encoding='utf-8', header=1, sep='\t')  # header=1
    print('CNC.shape :', data.shape)
    data.drop(['<IPHEAD>', '<IPTAIL>', '<OWNER>',
               '<ISP>', '<VIEW>', ], axis=1, inplace=True)
    data['country_code'], data['state'], data['city'] = data['<CITY>'].str.split('_', 2).str
    country = data.drop_duplicates(subset=['country_code'], keep='first', inplace=False)
    print("CNC.country.shape :", country.shape)
    state = data.drop_duplicates(subset=['state'], keep='first', inplace=False)
    print("CNC.state.shape :", state.shape)
    city = data.drop_duplicates(subset=['city'], keep='first', inplace=False)
    print("CNC.city.shape :", city.shape)


if __name__ == '__main__':
    xml_to_csv()
    # maxmind_eval()
    # cnc_eval()
