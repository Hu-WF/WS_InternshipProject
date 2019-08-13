### 2019/06/25-09/01 WS internship
All outputs during internship  

**1.URL**  
https://www.wangsu.com/

**2.Requirements**  
- python >= 3.X  
- IPy >= 1.0  
- geopy >= 1.20.0   
- pandas  
- numpy  
- scipy  

**3.Code in src_ipynb & src_py**  
    `output02_190704_batch_get_cityname.ipynb` :通过geoip库完成maxmind的300+个异常city_name的批量爬取与替换。  
    `output03_190722_cnc_maxmind_qq_evaluation.ipynb` :对cnc、MaxMind和qqLocList三个库进行初步分析。  
    `output04_190725_cityname_query.ipynb` :完成city_name查询模块，输入单个ip地址，在cnc和MaxMind中查得对应city_name并输出。  
    `output04_190807_city_isp_query.ipynb` :输入IP地址，在CNC和MaxMind中查询city_name和ISP。  
    `output05_190813_generate_fsn.ipynb` :通过cnc和maxmind生成city_name的FSN标准库。  

完成city_name查询模块，输入单个ip地址，在cnc和MaxMind中查得对应city_name并输出。  

**4.How-to-run**  
Make sure that the file is shown below, and then run codes in src_ipynb/_py directly:  
　　|-Project  
　　　　|-fileout  
　　　　　　|-cnc  
　　　　　　|-maxmind  
　　　　　　|-qqloc  
　　　　|-src_ipynb  
　　　　　　|-Outputs.ipynb#所有提交的.ipynb类型的output  
　　　　|-src_py  
　　　　　　|-Outputs.py#所有提交的.py类型的output  
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
