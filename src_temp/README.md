**1.Code in src_temp**  
    `output02_190704_geo_inverse_coding.py` :通过地理逆编码完成MaxMind的300+个异常city_name的批量爬取与替换。  
    `output03_190722_geoip_evaluation.py` :对CNC、MaxMind和qqLocList三个库进行初步分析。  
    `output04_190807_city_isp_query.py` :输入IP地址，在CNC和MaxMind中查询city_name和ISP。  
    `output05_190812_cnc_expand.py` :通过MaxMind为CNC扩充多个key-value。  
    `output06_190819_generate_fsn.py` :通过cnc和maxmind生成city_name的FSN标准库，并导出IP-FSN查询库。  
