#!/usr/bin/env python 3.7
# -*- coding:utf-8 -*-
import pandas as pd


class GenerateFSN():
    '''通过cnc和maxmind建立city_name的FSN标准库。包含以下操作：
    1.cnc仅有5514个city_name，maxmind有145718个city_name,且cnc中的city_name大部分与maxmind重复（仅564个未出现），
    因此，先进行以下操作：
        ①.删除cnc中city_name在maxmind中出现过的数据、值为qita的数据；
    2.标准FSN含有以下字段：<continent_code><country_code><subdivision_1_iso_code><city_name>。
    maxmind具备以上所有字段，而cnc只有<country_code><state><city_name>三个字段，因此进行以下操作以补全cnc空缺字段：
        ②.通过maxmind导出<continent_code>-<country_code>键值对表，然后将其merge到cnc中；
        ③.通过maxmind导出<subdivision_1_iso_code>-<subdivision_1_name>键值对表，然后merge到cnc中，替换掉cnc['state']；
    3.经过以上cnc前处理操作后，将cnc和maxmind分别生成<FSN>字段，然后合并成一个文件即可。
    '''
    def __init__(self,cnc_path='../data/ipb-ips_raw.str/ipb-ips_raw.str',
                 maxmind_path='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv'):
        # in data
        self.__cnc=pd.read_csv(cnc_path,encoding='utf-8',header=1,sep='\t')
        self.__maxmind=pd.read_csv(maxmind_path)
        #out data
        self.__fsn_path='../fileout/fsn.csv'

    def __load_cnc(self,):
        '''载入cnc的city_name数据库，并拆分出contry_code、state、city字段'''
        pd.set_option('mode.chained_assignment', None)#close SettingwithCopyWarning
        df = self.__cnc
        df.drop(['<IPHEAD>', '<IPTAIL>', '<OWNER>', '<ISP>', '<VIEW>', ], axis=1, inplace=True)
        df.drop_duplicates(keep='first', inplace=True)
        df['country_code'], df['state'], df['city'] = df['<CITY>'].str.split('_', 2).str
        df['state'] = df['state'].apply(lambda x: str(x).capitalize())
        df['city'] = df['city'].apply(lambda x: str(x).capitalize())
        # df.to_csv('../fileout/cnc/cnc_base.csv')
        return df

    def __filter_cnc_base_on_maxmind(self,cnc):
        '''过滤掉cnc['city']中在maxmind['city_name']出现过的数据行，以及city值为Qita的数据行'''
        maxmind=self.__maxmind
        cnc = cnc[cnc['city'] != 'Qita']#删除city值为Qita的数据行
        # 将cnc中用'-'连接的字符串转换为用空格连接，并将各单词的首字母转换为大写，便于与maxmind匹配
        def strProcessing(s):
            ss = s.split('-')
            slist = map(lambda x: x.capitalize(), ss)
            return ' '.join(slist)
        cnc['city'] = cnc['city'].apply(lambda x: strProcessing(x))
        # 对cnc中的部分异常值做转换处理，便于与maxmind匹配
        cnc['state'] = cnc['state'].replace('Rio-de-janeiro', 'Rio de Janeiro')
        cnc['state'] = cnc['state'].replace('Newfoundland-and-labrador', 'Newfoundland and Labrador')
        cnc['state'] = cnc['state'].replace('Fukushima', 'Fukushima-ken')
        cnc['state'] = cnc['state'].replace('Bucureti', 'Bucuresti')
        cnc['state'] = cnc['state'].replace('Tatarstan', 'Tatarstan Republic')
        cnc['state'] = cnc['state'].replace('Districtofcolumbia', 'District of Columbia')
        cnc['state'] = cnc['state'].replace('Newyork', 'New York')
        cnc['state'] = cnc['state'].replace('Neimenggu', 'Inner Mongolia Autonomous Region')
        cnc['state'] = cnc['state'].replace('Xizang', 'Tibet')
        # 判断cnc中的数据是否在maxmind中出现过，若出现过，则过滤掉
        def judge_in_maxmind(ele, sheet):
            if ele in sheet['city_name'].values:return True
            else: return False
        cnc['judge_res'] = cnc['city'].apply(lambda x: judge_in_maxmind(x, maxmind))
        cnc = cnc[cnc['judge_res'] == False]
        # print('cnc中city值为maxmind的补集，且!=Qita的有：', cnc.shape)
        return cnc

    def __get_continent_country_from_maxmind(self,):
        '''通过maxmind获取continent-country键值对表'''
        df = self.__maxmind[['continent_code', 'country_iso_code', ]]
        df.drop_duplicates(keep='first',inplace=True)
        df.dropna(subset=['country_iso_code'], inplace=True)
        df.fillna('NA', inplace=True)  # pandas读入北美洲NA时，会识别成空缺值，因此填充回NA
        # df.to_csv('../fileout/maxmind/maxmind_continent-country.csv')
        return df

    def __get_sub1code_sub1name_from_maxmind(self,):
        '''通过maxmind获取subdivision_1_code和subdivision_1_name的键值对'''
        df=self.__maxmind[['subdivision_1_iso_code', 'subdivision_1_name',]]
        df.drop_duplicates(keep='first', inplace=True)
        # df.to_csv('../fileout/maxmind/maxmind_sub1code-sub1name.csv')
        return df

    def __expand_cnc_with_maxmind(self,cnc,continent_country,sub1code_sub1name):
        '''通过maxmind提供的continent_code-country_code、subdivision_1_iso_code-subdivision_1_name键值对来完善cnc'''
        #expand cnc with continent_couontry：
        std = pd.merge(left=cnc, right=continent_country, left_on='country_code', right_on='country_iso_code')
        #expand cnc with subdivision_1_iso_code-subdivision_1_name：
        std = pd.merge(left=std, right=sub1code_sub1name, left_on='state', right_on='subdivision_1_name', how='left')
        # 若subdivision_1_iso_code仍为Nan，则用state填充
        std.loc[std['subdivision_1_iso_code'].isnull(), 'subdivision_1_iso_code'] = std.loc[
            std['subdivision_1_iso_code'].isnull(), 'state']
        std = std[['continent_code', 'country_code', 'state', 'subdivision_1_iso_code', 'city']]
        # std.to_csv('../fileout/fsn/cnc_expand_with_maxmind.csv', index=None)
        return std

    def __generate_and_merge_fsn(self,cnc):
        '''依次生成cnc FSN和maxmind FSN，然后合并为一个FSN'''
        # generate cnc FSN
        cnc['FSN'] = cnc['continent_code'] + '.' + cnc['country_code'] + '.' + cnc['subdivision_1_iso_code'] + '.' + cnc[
            'city']
        cnc = cnc[['FSN', ]]
        print('cnc_FSN_shape:',cnc.shape)
        # cnc.to_csv('../fileout/fsn/cnc_fsn.csv', index=None)
        # generate maxmind FSN
        maxmind = self.__maxmind[['continent_code', 'country_iso_code', 'subdivision_1_iso_code', 'city_name']]
        # maxmind.drop_duplicates(subset=['city_name',],keep='first',inplace=True)#former drop_duplicates
        maxmind['continent_code'].fillna('NA', inplace=True)
        maxmind.fillna('Qita', inplace=True)
        maxmind['FSN'] = maxmind['continent_code'] + '.' + maxmind['country_iso_code'] + '.' + maxmind[
            'subdivision_1_iso_code'] + '.' + maxmind['city_name']
        maxmind = maxmind[['FSN', ]]
        maxmind.drop_duplicates(subset=['FSN'], keep='first', inplace=True)
        print('maxmind_FSN_shape', maxmind.shape)
        # maxmind.to_csv('../fileout/fsn/maxmind_fsn.csv', index=None)
        # concat cnc FSN and maxmind FSN
        fsn = pd.concat([cnc, maxmind], axis=0, ignore_index=True)
        fsn.drop_duplicates(keep='first', inplace=True)
        print('FSN_shape', fsn.shape)
        fsn.sort_values(axis=0,by="FSN",kind='quicksort',inplace=True)#排序
        fsn.to_csv(self.__fsn_path, index=None)
        return fsn

    def get_fsn_from_cnc_maxmind(self):
        '''供外部调用的FSN生成函数,包括以下操作：
        ①.cnc载入；②.过滤cnc中与maxmind重复的数据；③.获取continent_country键值对；④.获取sub1code_sub1name键值对；
        ⑤.根据键值对扩充cnc；⑥.生成cnc和maxmind的FSN，合并输出最终FSN。
        '''
        print('Generating...')
        cnc=self.__load_cnc()#载入cnc
        cnc_filtered=self.__filter_cnc_base_on_maxmind(cnc)#过滤cnc中与maxmind重复的数据
        continent_country=self.__get_continent_country_from_maxmind()#获取键值对
        sub1code_sub1name=self.__get_sub1code_sub1name_from_maxmind()#获取键值对
        cnc_std=self.__expand_cnc_with_maxmind(cnc_filtered,continent_country,sub1code_sub1name)#根据键值对扩充cnc
        self.__generate_and_merge_fsn(cnc_std,)#生成cnc和maxmind的FSN，并合并输出最终文件
        print('The generated FSN is in the following file path:',self.__fsn_path)


if __name__ == '__main__':
    gf=GenerateFSN(cnc_path='../data/ipb-ips_raw.str/ipb-ips_raw.str',
                   maxmind_path='../data/GeoIP2-City-CSV/GeoIP2-City-CSV_20190625/GeoIP2-City-Locations-en.csv')
    help(gf.get_fsn_from_cnc_maxmind)
    gf.get_fsn_from_cnc_maxmind()
