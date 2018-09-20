
import pymysql
import urllib.request
import urllib.parse
import requests
import random
import matplotlib.pyplot as plt # plt 用于显示图片
import matplotlib.image as mpimg # mpimg 用于读取图片
import zipfile
import os,sys
from lxml import html


def download_bj():
    db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                         password="woxnskzhcs!", db="i3city_test", charset="utf8")
    cursor = db.cursor()

    sql = 'SELECT name, file_url FROM bj_gov_data'
    cursor.execute(sql)
    results = cursor.fetchall()
    for result in results:
        url = result[1]
        local_path = 'E:/ontoData/bjData/' + result[0] + '.csv'
        urllib.request.urlretrieve(url, local_path)
        print(result[0])


def login(session):
    data = {}
    data['uname'] = "lsq123"
    data['upass'] = "123456789z"


    image = session.get('http://www.gyopendata.gov.cn/city/images.do?' + str(random.uniform(0, 1)))
    url = 'http://www.gyopendata.gov.cn/city/login.do'

    file = open('download/aa.jpg', 'wb')
    file.write(image.content)
    file.close()

    image = mpimg.imread('download/aa.jpg')
    plt.imshow(image)
    plt.axis('off')
    plt.show()

    code = input()
    data['code'] = code
    resp = session.post(url, data=data)
    print(resp.text)

def download_guiyang(session):
    db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                         password="woxnskzhcs!", db="i3city_test", charset="utf8")
    cursor = db.cursor()
    sql = "select name, res_id from guiyang_gov_data"
    cursor.execute(sql)
    results = cursor.fetchall()
    for index in range(273, len(results)):
        print(index)
        result = results[index]
    # for result in results:
        name = result[0]
        res_Id = result[1]
        url = 'http://www.gyopendata.gov.cn/city/filelist.htm'
        para = {
                'doId': '2',
                'resId': res_Id,
                'resName': name,
                'text1': 'all'
        }
        flag = True
        while flag:
            try:
                resp = session.post(url, data=para)
                print("%s  %s  " % (res_Id, name))
                print(resp.text)
                files = resp.json()
                flag = False
            except:
                ValueError
                print("oops")

        # resp = session.post(url, data=para)
        # print("%s  %s  "%(res_Id, name))
        # print(resp.text)
        # files = resp.json()
        # print(data)

        download_url = 'http://www.gyopendata.gov.cn/city/downloadFileZip.htm'
        data = {}
        data['resId'] = res_Id
        data['zipName'] = name
        uploadDirs = ''
        uploadNames = ''
        for file in files:
            uploadDirs += ',' + file['uploadDir']
            uploadNames += ',' + file['uploadName']
        uploadNames = uploadNames[:0] + uploadNames[1:]
        uploadDirs = uploadDirs[:0] + uploadDirs[1:]
        data['uploadNames'] = uploadNames
        data['uploadDirs'] = uploadDirs
        print(data)

        response = session.post(download_url, data=data)
        print(response.content)
        # print(response)
        local_path = 'E:/ontoData/guiyangData/' + name + '.zip'
        # urllib.request.urlretrieve(url, local_path)
        # print(result[0])
        local_file = open(local_path, 'wb')
        local_file.write(response.content)
        local_file.close()

def download_hb():
    db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                         password="woxnskzhcs!", db="i3city_test", charset="utf8")
    cursor = db.cursor()
    sql = 'SELECT cata_title, downloadURL FROM harbin_gov_data'
    cursor.execute(sql)
    results = cursor.fetchall()
    for index in range(459, len(results)):
        result = results[index]
        url = result[1]
        name = result[0].strip()
        name = name.replace('|', '、')
        local_path = 'E:/ontoData/hbData/' + name + '.zip'
        file = open(local_path, 'wb')
        resp = requests.get(url)
        file.write(resp.content)
        file.close()
        # urllib.request.urlretrieve(url, local_path)
        print(index)
        print(name)

def compress():
    local_path = 'E:/ontoData/guiyangData/'

    root, dirs, files = os.walk(local_path)
    for file in files:
        file_path = os.path.join(root, file)
        os.mkdirs(file_path)
        if zipfile.is_zipfile(file_path):
            zip_file = zipfile.ZipFile(file_path)
            zip_file.extractall(path= local_path)

def set_bj_topic2():
    db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                         password="woxnskzhcs!", db="i3city_test", charset="utf8")
    cursor = db.cursor()
    sql = 'select  source, name from bj_gov_data'
    cursor.execute(sql)
    results = cursor.fetchall()
    for result in results:
        source = result[0]
        name = result[1]
        describ_url = 'http://www.bjdata.gov.cn/cms/web/dataDetail/sjxx/'
        target = source + '/' + name + '.txt'
        print(target)
        describ_url = describ_url + urllib.parse.quote(target)
        print(describ_url)
        response = requests.get(describ_url)
        response.encoding = 'utf-8'
        words = response.text.split('\n')
        tt = ''
        for word in words:
            if '资源分类' in word:
                tt = word
                break

        topic = tt.replace("资源分类", "").strip()
        print(topic)
        # print(type(channel_id))
        sql_update = "update bj_gov_data set topic = %s where name = %s"
        cursor.execute(sql_update, (topic, name))
        db.commit()


def set_bj_topic():
    db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                         password="woxnskzhcs!", db="i3city_test", charset="utf8")
    cursor = db.cursor()
    sql = 'select distinct channel_id from bj_gov_data'
    cursor.execute(sql)
    results = cursor.fetchall()
    for result in results:
        channel_id = result[0]
        print(channel_id)
        chanel_html = 'http://www.bjdata.gov.cn/cms/web/templateIndexList/indexList.jsp?currPage=1&channelID=' + str(channel_id) + '&sortType=null&orderBy=null'
        resp = requests.get(chanel_html)
        tree = html.fromstring(resp.text)
        source_xpath = '/html/body/div/div[2]/div[1]/div[2]/span[1]/strong'
        name_xpath = '/html/body/div/div[2]/div[1]/div[2]/a/h2'
        source_node = tree.xpath(source_xpath)
        source = source_node[0].text
        name_node = tree.xpath(name_xpath)
        name = name_node[0].text.strip()
        describ_url = 'http://www.bjdata.gov.cn/cms/web/dataDetail/sjxx/'
        target = source + '/' + name + '.txt'
        print(target)
        describ_url = describ_url + urllib.parse.quote(target)
        print(describ_url)
        response = requests.get(describ_url)
        response.encoding = 'utf-8'
        words = response.text.split('\n')
        tt = ''
        for word in words:
            if '资源分类' in word:
                tt = word
                break

        topic = tt.replace("资源分类", "").strip()
        print(topic)
        # print(type(channel_id))
        sql_update = "update bj_gov_data set topic = %s where channel_id = %s"
        cursor.execute(sql_update, (topic, channel_id))
        db.commit()

if __name__ == '__main__':
    # session = requests.session()
    # # login(session)
    # download_guiyang(session)
    # download_hb()
    # compress()
    set_bj_topic()