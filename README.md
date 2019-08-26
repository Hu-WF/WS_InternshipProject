### 2019/06/25-09/01 WS internship
All outputs during internship  

**1.Requirements**  
- python >= 3.X  
- IPy >= 1.0  
- geopy >= 0.81   
- unihandecode >= 1.20.0   
- pandas  
- numpy  
- scipy  

**2.Code in src_temp**  
    `output02_190704_geo_inverse_coding.py` :通过地理逆编码完成MaxMind的300+个异常city_name的批量爬取与替换。  
    `output03_190722_geoip_evaluation.py` :对CNC、MaxMind和qqLocList三个库进行初步分析。  
    `output04_190807_city_isp_query.py` :输入IP地址，在CNC和MaxMind中查询city_name和ISP。  
    `output05_190812_cnc_expand.py` :通过MaxMind为CNC扩充多个key-value。  
    `output06_190819_generate_fsn.py` :通过CNC和MaxMind生成city_name的FSN标准库，并导出IP-FSN查询库。  

**3.Code in src_final**  
    `generate_fsn.py` :通过CNC和MaxMind生成city_name的FSN标准库，并导出IP-FSN查询库。 

**4.How-to-run**  
Make sure that the file is shown below, and then run codes in src_ipynb/_py directly:  
　　|-Project  
　　　　|-fileout  
　　　　　　|-cnc  
　　　　　　|-maxmind  
　　　　　　|-qqloc  
　　　　|-src_final  
　　　　　　|-generate_fsn.py  
　　　　|-src_temp  
　　　　　　|-output02_190704_geo_inverse_coding.py  
　　　　　　|-output03_190722_geoip_evaluation.py  
　　　　　　|-output04_190807_city_isp_query.py  
　　　　　　|-output05_190812_cnc_expand.py  
　　　　　　|-output06_190819_generate_fsn.py  
　　　　|-data  
　　　　　　|-GeoIP2-City-CSV  
　　　　　　　　|-GeoIP2-City-CSV_20190625  
　　　　　　　　　　|-GeoIP2-City-Blocks-IPv4.csv  
　　　　　　　　　　|-GeoIP2-City-Blocks-IPv6.csv  
　　　　　　　　　　|-GeoIP2-City-Locations-en.csv  
　　　　　　|-GeoIP2-ISP-CSV  
　　　　　　　　|-GeoIP2-ISP-CSV_20190618  
　　　　　　　　　　|-GeoIP2-ISP-Blocks-IPv4.csv  
　　　　　　　　　　|-GeoIP2-ISP-Blocks-IPv6.csv  
　　　　　　|-ipb-ips_raw.str  
　　　　　　　　|-ipb-ips_raw.str  
　　　　　　　　|-ipb-ips_raw.str.md5  
　　　　　　|-QQ-LocList  
　　　　　　　　|-LocList.xml   
　　　　|-README.md  
