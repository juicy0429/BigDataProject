#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# 접속설정
req =  "https://kr.investing.com/currencies/live-currency-cross-rates"

# investiong.com 특성상 ban 당하므로 헤더 정보 추가
header={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36'}
result = requests.get(req, headers=header)
html = result.text

# BeautifulSoup 설정
bs = BeautifulSoup(html, "html.parser")

# 배열 선언
column_list = ['CURRENT_NAME','BID', 'ASK', 'LOW', 'HIGH', 'PC', 'PCP','RECORD_TIME']
currency_list=[]
bid_list = []
ask_list = []
low_list = []
high_list = []
pc_list = []
pcp_list = []
record_time = []
now = datetime.now()


# 데이터 가져오기
for a in range(20):
    columns = bs.findAll("div", "topBox") # div 중에 class이름이 topbox인것을 모두 찾아라.
    title = columns[a].findAll('a')# columns 중에 a태그를 찾아라.
    currency_name = title[0].text

    data = bs.findAll("div", "contentBox")
    content = data[a].findAll('div')
    bid = content[2].text # 홀수번호는 글자포함, 짝수번호는 데이터만
    ask = content[5].text

    content1 = data[a].findAll('span')
    low = content1[0].find('i').text
    high = content1[1].find('i').text
    pc = content1[2].text
    pcp = content1[3].text

    currency_list.append(currency_name)
    bid_list.append(bid)
    ask_list.append(ask)
    low_list.append(low)
    high_list.append(high)
    pc_list.append(pc)
    pcp_list.append(pcp)
    record_time.append(now)

df_1 = pd.DataFrame(currency_list)
df_2 = pd.DataFrame(bid_list)
df_3 = pd.DataFrame(ask_list)
df_4 = pd.DataFrame(low_list)
df_5 = pd.DataFrame(high_list)
df_6 = pd.DataFrame(pc_list)
df_7 = pd.DataFrame(pcp_list)
df_8 = pd.DataFrame(record_time)

result = pd.concat([df_1,df_2, df_3, df_4, df_5, df_6, df_7, df_8], axis=1)
result.columns = [column_list]

# csv로 저장
result.to_csv("real_time_exchange_rate.csv", mode = 'a', header=False)

subprocess.call('hadoop fs -copyFromLocal * big_data', shell = True)
subprocess.call('hadoop fs -put -f * big_data', shell = True)




