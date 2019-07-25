#### 2019/06/25-09/01 网宿科技股份有限公司实习
工作职责：IP库数据分析、数据挖掘
实习期间所有代码提交Outputs（无涉密内容）

**1.URL**  
https://www.wangsu.com/

**2.Requirements**  
- python >= 3.X  
- IPy >= 1.0  
- geopy >= 1.20.0   
- pandas  
- numpy  
- scipy  

**3.Outputs**  
    `output02_190704_batch_get_cityname.ipynb` :通过geoip库完成maxmind的300+个异常city_name的批量爬取与替换。  
    `output03_190722_cnc_maxmind_qq_evaluation.ipynb` :对cnc、MaxMind和qqLocList三个库进行初步分析。  
    `output04_190725_cityname_query.ipynb` :完成city_name查询模块，输入单个ip地址，在cnc和MaxMind中查得对应city_name并输出。  
    `output04_190725_cityname_query.py` :output04的.py版本。  

**4.How-to-run**  
Make sure that the file is shown below, and then run jinnanMain.py directly:  
　　|-Project  
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
　　　　|-fileout  
　　　　　　|-cnc  
　　　　　　|-maxmind  
　　　　　　|-qqloc  
　　　　|-src  
　　　　　　|-Outputs.py\.ipynb  
　　　　|-README.md  
