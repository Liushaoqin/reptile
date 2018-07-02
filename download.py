
import pymysql
import urllib.request
import requests
import random
import matplotlib.pyplot as plt # plt 用于显示图片
import matplotlib.image as mpimg # mpimg 用于读取图片


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
    for index in range(270, len(results)):
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




if __name__ == '__main__':
    session = requests.session()
    # login(session)
    download_guiyang(session)
