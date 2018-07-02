# -*- coding: utf-8 -*-
import os
import requests
from lxml import html
import pymysql
import time


db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                     password="woxnskzhcs!", db="i3city_test", charset="utf8")
cursor = db.cursor()


start_time = time.time()
id = 0
for i in range(0, 236):
    current_time = time.time()
    headers = {
        'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        'Accept-Encoding': "gzip, deflate",
        'Accept-Language': "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        'Connection': "keep-alive",
        'Host': "data.qingdao.gov.cn",
        'Upgrade-Insecure-Requests': "1",
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0",
        }
    params = {
        'method': 'GetCatalog',
        '_order': 'cc.update_time+desc',
        'length': 6,
        'pageLength': 6,
        'start': 0
    }
    url = "http://data.qingdao.gov.cn/odweb/catalog/catalog.do?method=GetCatalog&_order=cc.update_time+desc&length=6&pageLength=6&start=" + str(i * 6)
    resp = requests.get(url, headers= headers)
    dataChunk = resp.json()
    for data in dataChunk['data']:
        catalogStatistic = data['catalogStatistic']
        update_time = data['update_time']
        cata_items = data['cata_items']
        create_time = data['create_time']
        cata_title = data['cata_title']
        open_type = data['open_type']
        org_name = data['org_name']
        cata_tags = data['cata_tags']
        cataLogGroups = data['cataLogGroups'][0]['group_name']
        cataLogIndustrys = data['cataLogIndustrys'][0]['group_name']
        released_time = data['released_time']
        description = data['description']
        data_count = data['catalogStatistic']['data_count']
        use_scores = data['catalogStatistic']['use_scores']
        use_points  = data['catalogStatistic']['use_points']
        use_grade = data['catalogStatistic']['use_grade']
        use_comments = data['catalogStatistic']['use_comments']
        use_favs = data['catalogStatistic']['use_favs']
        use_visit = data['catalogStatistic']['use_visit']
        use_data_count = data['catalogStatistic']['use_data_count']
        conf_update_cycle = data['conf_update_cycle']
        cata_id = data['cata_id']

        cata_items = cata_items.replace("\"", "'")
        if cata_tags is None:
            cata_tags = ""
        sql = """insert into qd_gov_data(id, cata_title, open_type, cata_tags, cataLogGroups, create_time,
                    update_time, released_time, cata_items, org_name, cataLogIndustrys,
                    description, data_count, use_scores, use_points, use_grade, use_comments,
                    use_favs, use_visit, use_data_count, url, downloadURL, conf_update_cycle)"""

        url1 = "http://data.qingdao.gov.cn/odweb/catalog/catalogDetail.do?method=GetDownLoadInfo&cata_id=" + cata_id + "&conf_type = 2"
        headers = {
            'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            'Accept-Encoding': "gzip, deflate",
            'Accept-Language': "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            'Connection': "keep-alive",
            'Host': "data.qingdao.gov.cn",
            'Upgrade-Insecure-Requests': "1",
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0",
        }
        resp = requests.get(url1, headers= headers)
        download_links = resp.json()
        fileId = ""
        tag = False
        for download_link in download_links['data']:
            if 'csv' in download_link['fileName']:
                tag = True
                fileId = download_link['fileId']
                break

        if not tag:
            for download_link in download_links['data']:
                if 'xls' in download_link['fileName']:
                    tag = True
                    fileId = download_link['fileId']
                    break
        if not tag:
            for download_link in download_links['data']:
                if 'json' in download_link['fileName']:
                    tag = True
                    fileId = download_link['fileId']
                    break
        if not tag:
            for download_link in download_links['data']:
                if 'xml' in download_link['fileName']:
                    tag = True
                    fileId = download_link['fileId']
                    break

        if not tag:
            continue

        URL = "http://data.qingdao.gov.cn/odweb/catalog/catalogDetail.htm?cata_id=" + cata_id
        downloadURL = "http://data.qingdao.gov.cn/odweb/catalog/CatalogDetailDownload.do?method=getFileDownloadAddr&fileId=" + fileId

        sql = sql + "values" + '(' + str(id) + ',"' + cata_title + '","' + open_type\
              + '","' + cata_tags + '","' + cataLogGroups + '","' + create_time \
              + '","' + update_time + '","' + released_time + '","' + cata_items \
              + '","' + org_name + '","' + cataLogIndustrys + '","' + description \
              + '",' + str(data_count) + ',' + str(use_scores) + ',' + str(use_points) \
              + ',' + str(use_grade) + ',' + str(use_comments) + ','+ str(use_favs) + ',' \
              + str(use_visit) + ',' + str(use_data_count) + ',"' + URL + '","' \
              + downloadURL + '","' + conf_update_cycle + '")'
        # print(sql)
        cursor.execute(sql)
        db.commit()
        id += 1
    end_time = time.time()
    time_a_epoch = end_time - current_time
    used_time = end_time - start_time
    total_time = time_a_epoch * 236
    remain_time = total_time - used_time

    print("epoch:" + str(i) + "/236" +"\t time_a_epoch:" + str(time_a_epoch) + " used_time: " + str(used_time) + "\t total_time: " + str(total_time) + "\t remain_time:" + str(remain_time))


db.close()

