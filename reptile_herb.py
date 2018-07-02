# -*- coding: utf-8 -*-
import os
import requests
from lxml import html
import pymysql
import time


db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                     password="woxnskzhcs!", db="i3city_test", charset="utf8")
cursor = db.cursor()
sql = "select * from bj_gov_data WHERE  id = 1"
cursor.execute(sql)
result = cursor.fetchall()
start_time = time.time()
for i in range(0, 143):
    current_time = time.time()
    url = "http://data.harbin.gov.cn/odweb/catalog/catalog.do?method=GetCatalog"
    params = {
        'method': 'GetCatalog',
        'cate_type': 'default',
        'length': '6',
        'pageLength': '6',
        'start': 6 * i + 6
    }
    resp = requests.post(url, params)
    dataChunk = resp.json()
    for data in dataChunk['data']:
        cata_id = data['cata_id']
        cata_title = data['cata_title']
        cata_title = cata_title.replace("\"", "|" )
        cata_tags = data['cata_tags']
        cata_items = data['cata_items']
        description = data['description']
        description = description.replace("\"", "|")
        update_time = data['update_time']  # 最后更新时间
        release_time = data['released_time']

        org_name = data['org_name']  # 来源部门

        catalog_format = data['catalog_format']  # 有几种格式的文件
        data_count = data['data_count']  # 几个数据
        # file_count = data['file_count'] #几个文件
        api_count = data['api_count']
        task_count = data['task_count']
        use_scores = data['use_scores']
        use_points = data['use_points']
        use_grade = data['use_grade']
        use_comments = data['use_comments']
        use_favs = data['use_favs']
        use_visit = data['use_visit']  # 访问次数
        use_task_count = data['use_task_count']
        use_data_count = data['use_data_count']
        use_file_count = data['use_file_count']  # 下载次数
        use_api_count = data['use_api_count']
        data_group = data['group_name']
        update_cycle = data['update_cycle']
        sql = """insert into harbin_gov_data(cata_id,cata_title,cata_tags, cata_items,
                description,update_time ,release_time ,
                org_name,catalog_format,data_count ,
                api_count ,task_count,use_scores ,
                use_points,use_grade ,use_comments,use_favs,use_visit,
                use_task_count,use_data_count,use_file_count ,use_api_count, url, downloadURL, scene, sector, update_cycle)"""
        if cata_id == "":
            continue
        url = "http://data.harbin.gov.cn/odweb/catalog/catalogDetail.htm?cata_id=" + cata_id + "&target_tab=data-download"
        response = requests.get(url)
        page = response.text
        tree = html.fromstring(page)
        elements = tree.xpath("/html/body/div[4]/div[1]/div/div/div[2]/div[2]/ul/li[6]/div/div[2]/ul/li")
        tag = True
        id = ""
        if range(len(elements)) == 0:
            continue
        for index in range(len(elements) - 1, 0, -1):
            element = elements[index]
            str2 = element[0].text
            if "csv" in str2:
                dict = element[1][0].attrib
                id = dict['id']
                break
            elif "xls" in str2:
                dict = element[1][0].attrib
                id = dict['id']
                break
            elif "json" in str2:
                dict = element[1][0].attrib
                id = dict['id']
                break
            elif "xml" in str2:
                dict = element[1][0].attrib
                id = dict['id']
                break

        URL = "http://data.harbin.gov.cn/odweb/catalog/catalogDetail.htm?cata_id=" + cata_id
        if id == "":
            print(URL)
            continue
        downloadURL = "http://data.harbin.gov.cn/odweb/catalog/catalogDetail.do?method=getFileDownloadAddr&fileId=" + id

        strs = data_group.split(',')
        scene = strs[0].split(':')[2]
        sector = strs[1].split(':')[2]

        # print(URL +"\n" +downloadURL+ "\n" + scene + "\n" +sector )
        sql = sql + "values" + '("' + cata_id + '", "' + cata_title + '", "' + cata_tags + '", "' + cata_items + '", "' + description + '", "' + update_time + '", "' + release_time + '", "' + org_name + '", "' + catalog_format + '", ' + str(
            data_count) + ',' + str(api_count) + ',' + str(task_count) + ',' + str(use_scores) + ',' + str(
            use_points) + ',' + str(use_grade) + ',' + str(use_comments) + ',' + str(use_favs) + ',' + str(
            use_visit) + ',' + str(use_task_count) + ',' + str(use_data_count) + ',' + str(use_file_count) + ',' + str(
            use_api_count) + ',"' + URL + '","' + downloadURL + '","' + scene + '","' + sector + '","' + update_cycle +'")'
        # print(sql)
        cursor.execute(sql)
        db.commit()
    end_time = time.time()
    time_a_epoch = end_time - current_time
    used_time = end_time - start_time
    total_time = time_a_epoch * 143
    remain_time = total_time - used_time

    print("epoch:" + str(i) + "/143" +"\t time_a_epoch:" + str(time_a_epoch) + " used_time: " + str(used_time) + "\t total_time: " + str(total_time) + "\t remain_time:" + str(remain_time))


db.close()
