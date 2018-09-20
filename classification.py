import re

import pymysql
import numpy
import jieba
import numpy as np
import sklearn.svm
import sklearn.tree
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer


def convert_name(table_name):
    chars = [chr(i) for i in range(48, 123)]

    new_table_name = table_name.replace('北京', '')
    new_table_name = new_table_name.replace('表', '')

    new_table_name = new_table_name.replace('哈尔滨', '')
    new_table_name = new_table_name.replace('市', '')
    if "年" in new_table_name:
        year_index = new_table_name.index("年")
        start_index = year_index - 1
        if not new_table_name[start_index].isdigit():
            pass
        else:
            while new_table_name[year_index + 1] == '版' or new_table_name[year_index + 1] == '）':
                year_index += 1
                if year_index == len(new_table_name) - 1:
                    break

            while new_table_name[start_index].isdigit() or new_table_name[start_index] == '-':
                start_index -= 1

            if start_index >= 0:
                new_table_name = new_table_name[:start_index] + new_table_name[year_index + 1:]
            else:
                new_table_name = new_table_name[year_index + 1:]

    if '（' in new_table_name:
        st = new_table_name.index('（')
        ed = new_table_name.index('）')
        for index in range(st, ed):
            if new_table_name[index] in chars:
                if st >= 0:
                    new_table_name = new_table_name[:st] + new_table_name[ed:]
                else:
                    new_table_name = new_table_name[ed:]
                break

    if '(' in new_table_name:
        st = new_table_name.index('(')
        ed = new_table_name.index(')')
        for index in range(st, ed):
            if new_table_name[index] in chars:
                new_table_name = new_table_name[:st] + new_table_name[ed:]
                break

    new_table_name = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）“”《》]+", "", new_table_name)
    print(new_table_name)
    return new_table_name


def get_bj_data():
    db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                         password="woxnskzhcs!", db="i3city_test", charset="utf8")
    cursor = db.cursor()
    sql = "select * from bj_gov_data"
    cursor.execute(sql)
    results = cursor.fetchall()

    x = []
    y = []
    static_count = 0
    for result in results:
        table_name = result[1]
        source = result[2]
        description = result[4]
        topic = result[10]

        if "统计" in description or "中关村" in table_name or "情况" in table_name or "总值表" in table_name or "月度" in table_name or "年度" in table_name or "属性" not in description:
            static_count += 1
            continue

        new_table_name = convert_name(table_name)
        x.append(new_table_name)
        y.append(topic.split('/')[0])
    db.close()
    return x, y


def get_hb_data():
    db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                         password="woxnskzhcs!", db="i3city_test", charset="utf8")
    cursor = db.cursor()
    sql = "select * from harbin_gov_data"
    cursor.execute(sql)
    results = cursor.fetchall()
    x = []
    x_origin = []
    y = []
    static_count = 0
    for result in results:
        table_name = result[1]
        description = result[4]
        source = result[7]
        scene = result[24]

        if "统计" in description or "情况" in table_name or "总值表" in table_name or "月度" in table_name or "年度" in table_name or "统计局" in source:
            static_count += 1
            continue
        new_table_name = convert_name(table_name)
        x.append(new_table_name)
        x_origin.append(table_name)
        y.append(scene)
    db.close()
    return x, x_origin, y


def get_embedding_dict(train_x, test_x):
    vocab_dict = {}
    vocab = []
    for name in train_x:
        words = jieba.lcut(name)
        for word in words:
            if word not in vocab:
                vocab.append(word)

    for name in test_x:
        words = jieba.lcut(name)
        for word in words:
            if word not in vocab:
                vocab.append(word)

    embedding_dict = {}
    # embedding_file = open("sgns.wiki.word", encoding='utf-8')
    embedding_file = open("sgns.renmin.word", encoding='utf-8')
    # embedding_file = open("sgns.sogou.word", encoding='utf-8')
    lines = embedding_file.readlines()

    for line in lines:
        allen = line.split(' ')
        embedding = np.zeros(300)
        if len(allen) < 301:
            continue
        for index in range(1, 301):
            embedding[index - 1] = float(allen[index])
        embedding_dict[allen[0]] = embedding

    my_embedding_dict = {}
    for token in vocab:
        if token not in embedding_dict:
            print(token)
        else:
            my_embedding_dict[token] = embedding_dict[token]
    return vocab, my_embedding_dict, embedding_dict
    # print("Full Mode: " + "/ ".join(words))


def get_embedding(embedding_dict, x_set, y_set):
    x_embedding = []
    y = []
    assert len(x_set) == len(y_set)
    for index in range(0, len(x_set)):
        x = x_set[index]
        words = jieba.lcut(x)
        embed = np.zeros(300)
        num = 0
        for word in words:
            if word in embedding_dict:
                embed += np.array(embedding_dict[word])
                num += 1
        if num != 0:
            embed /= num
            x_embedding.append(embed)
            y.append(y_set[index])
    return x_embedding, y


def update_hb_data(x, y):
    db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                         password="woxnskzhcs!", db="i3city_test", charset="utf8")
    cursor = db.cursor()
    for index in range(len(x)):
        sql = "update harbin_gov_data set my_topic = %s where cata_title = %s"
        cursor.execute(sql, (y[index], x[index]))
        db.commit()
    db.close()


def get_tf_idf(texts, splited= True):
    corpus = []
    for text in texts:
        for tokens in text:
            if not splited:
                corpus.append(tokens)
            else:
                text_split = jieba.lcut(tokens)
                text_str = ''
                for word in text_split:
                    text_str += word
                    text_str += ' '
                corpus.append(text_str)
    # return corpus, corpus
    vectorizer = CountVectorizer()
    transformer = TfidfTransformer()
    tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus))
    vocab = vectorizer.get_feature_names()
    weight = tfidf.toarray()
    return vocab, weight


def get_embedding(vocab):
    embedding_file = open("sgns.renmin.word", encoding='utf-8')
    lines = embedding_file.readlines()
    embedding_dict = {}
    for line in lines:
        allen = line.split(' ')
        embedding = np.zeros(300)
        if len(allen) < 301:
            continue
        for i in range(1, 301):
            embedding[i - 1] = float(allen[i])
        embedding_dict[allen[0]] = embedding

    word_embedding = {}
    for word in vocab:
        if word in embedding_dict:
            word_embedding[word] = embedding_dict[word]

    return word_embedding


def get_text_embedding(vocab_embed, weight, vocab):
    bj_text_embedding = []
    for i in range(0, 431):
        temp = np.zeros(300)
        embed_sum = 0.0
        for word_index in range(0, 1499):
            if weight[i][word_index] != 0 and vocab[word_index] in vocab_embed:
                temp += (vocab_embed[vocab[word_index]] * weight[i][word_index])
                embed_sum += weight[i][word_index]
        temp /= embed_sum
        bj_text_embedding.append(temp)

    hb_text_embedding = []
    for i in range(431, 1097):
        temp = np.zeros(300)
        embed_sum = 0.0
        for word_index in range(0, 1499):
            if weight[i][word_index] != 0 and vocab[word_index] in vocab_embed:
                temp += (vocab_embed[vocab[word_index]] * weight[i][word_index])
                embed_sum += weight[i][word_index]
        if embed_sum != 0:
            temp /= embed_sum
        hb_text_embedding.append(temp)

    return bj_text_embedding, hb_text_embedding



if __name__ == '__main__':
    train_x, train_y = get_bj_data()
    test_x, test_x_origin, test_y = get_hb_data()
    # vacub, my_embedding_dict, embedding_dict = get_embedding_dict(train_x, test_x)
    #
    # train_x_embedding, train_y = get_embedding(my_embedding_dict, train_x, train_y)
    # test_x_embedding, test_y = get_embedding(my_embedding_dict, test_x, test_y)
    vocabulary, weight = get_tf_idf([train_x, test_x])
    vocab_embedding = get_embedding(vocabulary)

    bj_text_embedding, hb_text_embedding = get_text_embedding(vocab_embedding, weight, vocabulary)

    bj_text_embedding.pop(68)
    bj_text_embedding.pop(183)
    train_y.pop(68)
    train_y.pop(183)

    train_x_embedding = bj_text_embedding
    test_x_embedding = hb_text_embedding
    svm = sklearn.svm.LinearSVC()
    svm = svm.fit(train_x_embedding, train_y)

    y_ = svm.predict(test_x_embedding)

    tree = sklearn.tree.DecisionTreeClassifier()
    tree.fit(train_x_embedding, train_y)
    y2 = tree.predict(test_x_embedding)

    for i in range(len(test_x)):
        print(test_x_origin[i], y_[i])
    # update_hb_data(test_x_origin, y_)
