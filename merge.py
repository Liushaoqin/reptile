import math
import re

import pandas
import pymysql
import numpy
import jieba
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer


def convert_name(table_name):
    chars = [chr(i) for i in range(48, 123)]

    new_table_name = table_name.replace('北京', '')
    new_table_name = new_table_name.replace('表', '')

    new_table_name = new_table_name.replace('哈尔滨', '')
    new_table_name = new_table_name.replace('广州', '')
    new_table_name = new_table_name.replace('市', '')

    if '区' in new_table_name and new_table_name[2] == '区':
        new_table_name = new_table_name[3:]

    if '年' in new_table_name:
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
    x_origin = []
    sources = []
    columns = []
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

        st = description.index("包括")
        if "等属性" in description:
            ed = description.index("等属性")
        else:
            ed = description.index("属性")
        properties = description[st + 2 : ed].replace('、', ' ')
        columns.append(properties)

        sources.append(source)

        new_table_name = convert_name(table_name)
        x_origin.append(table_name)
        x.append(new_table_name)
        y.append(topic)
    db.close()
    return x, x_origin, columns, sources


def get_hb_data():
    db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                         password="woxnskzhcs!", db="i3city_test", charset="utf8")
    cursor = db.cursor()
    sql = "select * from harbin_gov_data"
    cursor.execute(sql)
    results = cursor.fetchall()
    x = []
    x_origin = []
    columns = []
    sources = []
    y = []
    static_count = 0
    for result in results:
        table_name = result[1]
        description = result[4]
        props = result[3]
        source = result[7]
        scene = result[24]

        if "统计" in description or "情况" in table_name or "总值表" in table_name or "月度" in table_name or "年度" in table_name or "统计局" in source:
            static_count += 1
            continue

        words = props.split('|')
        prop = ''
        for word in words:
            prop += word
            prop += ' '
        columns.append(prop)
        sources.append(source)
        new_table_name = convert_name(table_name)
        x.append(new_table_name)
        x_origin.append(table_name)
        y.append(scene)
    db.close()
    return x, x_origin, columns, sources


def get_gz_data():
    db = pymysql.connect(host="sc.qk0.cc", port=13306, user="root",
                         password="woxnskzhcs!", db="i3city_test", charset="utf8")
    cursor = db.cursor()
    sql = "select * from guangzhou_gov_data"
    cursor.execute(sql)
    results = cursor.fetchall()

    x = []
    x_origin = []
    sources = []
    columns = []
    y = []

    static_count = 0
    for result in results:
        table_name = result[2]
        source = result[3]
        description = result[8]
        topic = result[4]

        if "统计" in description or "统计局" in source or "情况" in table_name or "总值表" in table_name or "月度" in table_name \
                or "年度" in table_name or "包括" not in description:
            static_count += 1
            continue

        st = description.index("包括")
        if "等内容" in description:
            ed = description.index("等内容")
        elif "内容" in description:
            ed = description.index("内容")
        elif "等属性" in description:
            ed = description.index("等属性")
        elif "属性" in description:
            ed = description.index("属性")
        else:
            ed = -1
        if ed == -1:
            static_count += 1
            continue

        properties = description[st + 2: ed].replace('、', ' ')
        columns.append(properties)

        sources.append(source)

        new_table_name = convert_name(table_name)
        x_origin.append(table_name)
        x.append(new_table_name)
        y.append(topic)
    db.close()
    return x, x_origin, columns, sources


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
        else:
            print(word)
    return word_embedding


def get_text_embedding(vocab_embed, weight, vocab, text_size_1, text_size_2):
    bj_text_embedding = []
    for i in range(0, text_size_1):
        temp = np.zeros(300)
        embed_sum = 0.0
        for word_index in range(0, len(vocab)):
            if weight[i][word_index] != 0 and vocab[word_index] in vocab_embed:
                temp += (vocab_embed[vocab[word_index]] * weight[i][word_index])
                embed_sum += weight[i][word_index]
        temp /= embed_sum
        bj_text_embedding.append(temp)

    hb_text_embedding = []
    for i in range(text_size_1, text_size_1 + text_size_2):
        temp = np.zeros(300)
        embed_sum = 0.0
        for word_index in range(0, len(vocab)):
            if weight[i][word_index] != 0 and vocab[word_index] in vocab_embed:
                temp += (vocab_embed[vocab[word_index]] * weight[i][word_index])
                embed_sum += weight[i][word_index]
        if embed_sum != 0:
            temp /= embed_sum
        hb_text_embedding.append(temp)

    return bj_text_embedding, hb_text_embedding


def calculate_table_name_likelihood(table_embedding_a, table_embedding_b):
    most_like_set = []
    most_index_set = []
    for index_a in range(len(table_embedding_a)):
        a_embed = table_embedding_a[index_a]
        most_like = 0
        most_index = []
        for index_b in range(len(table_embedding_b)):
            b_embed = table_embedding_b[index_b]
            likelihood = calculate_vector_like(a_embed, b_embed)
            if likelihood > most_like:
                most_like = likelihood
                most_index = [index_a, index_b]

        print(most_index)
        most_index_set.append(most_index)
        most_like_set.append(most_like)
    return most_like_set, most_index_set


def calculate_self_likehood(table_embedding):
    most_like_set = []
    most_index_set = []
    for index_a in range(len(table_embedding)):
        a_embed = table_embedding[index_a]
        most_like = 0
        most_index = []
        for index_b in range(index_a + 1, len(table_embedding)):
            b_embed = table_embedding[index_b]
            likelihood = calculate_vector_like(a_embed, b_embed)
            if likelihood > most_like:
                most_like = likelihood
                most_index = [index_a, index_b]

        most_index_set.append(most_index)
        most_like_set.append(most_like)
    return most_like_set, most_index_set


def calculate_vector_like(a, b):
    assert len(a) == len(b)
    sum = 0.0
    a_size = 0.0
    b_size = 0.0
    for index in range(0, len(a)):
        sum += a[index] * b[index]
        a_size += a[index] * a[index]
        b_size += b[index] * b[index]

    return sum/(math.sqrt(a_size) * math.sqrt(b_size))


def calculate_property_likelihood(columns_a, columns_b):
    most_like_set = []
    most_index_set = []
    for index_a in range(len(columns_a)):
        column_a = columns_a[index_a]
        most_like = 0
        most_index = []
        for index_b in range(len(columns_b)):
            column_b = columns_b[index_b]

            column_a_split = column_a.strip().split(' ')
            column_b_split = column_b.strip().split(' ')
            likelihood = calculate_prop_like(column_a_split, column_b_split)
            if likelihood > most_like:
                most_like = likelihood
                most_index = [index_a, index_b]
        most_like_set.append(most_like)
        most_index_set.append(most_index)
    return most_like_set, most_index_set


def calculate_prop_like(a, b):
    # a = filter_prop(a)
    # b = filter_prop(b)
    union = set(a) | set(b)
    intersection = set(a) & set(b)
    if len(union) == 0:
        return 0
    return len(intersection) / len(union)


def filter_prop(a):
    b = []
    c = ['', '地址', '序号', '名称', '电话', '联系电话', '企业名称', '联系人单位', '办结日期', '单位名称', '年份', '邮编', '备注', '邮政编码', '机构名称', '区县名称', '姓名', '联系人', '单位', '项目名称', '服务时间', '发证日期', '时间', '区县', '社区名称', '负责人', '许可证号', '法定代表人', '街道名称', '名称（全称）', '地址（详细到门牌号）', '服务项目', '区域', '单位地址', '类别名', '详细地址', '经营范围']
    for x in a:
        if x not in c:
            b.append(x)
    return b


def calculate_prop_like_weight(prop_weights, vocabulary):
    bj_word_set = []
    bj_weight = []
    for i in range(0, 431):
        temp_word_dict = []
        temp_weight_dict = []
        for index in range(len(vocabulary)):
            word = vocabulary[index]
            if prop_weights[i][index] != 0:
                temp_word_dict.append(word)
                temp_weight_dict.append(prop_weights[i][index])
        bj_word_set.append(temp_word_dict)
        bj_weight.append(temp_weight_dict)

    hb_word_set = []
    hb_weight_set = []
    for j in range(431, 1097):
        temp_word_set = []
        temp_weight_dict = []
        for index in range(len(vocabulary)):
            word = vocabulary[index]
            if prop_weights[j][index] != 0:
                temp_word_set.append(word)
                temp_weight_dict.append(prop_weights[j][index])
        hb_word_set.append(temp_word_set)
        hb_weight_set.append(temp_weight_dict)

    most_like_set = []
    most_index_set = []
    for bj_index in range(len(bj_word_set)):
        bj_word = bj_word_set[bj_index]
        most_like = 0
        most_index = []
        for hb_index in range(len(hb_word_set)):
            hb_word = hb_word_set[hb_index]
            union = set(bj_word) | set(hb_word)
            intersection = set(bj_word) & set(hb_word)
            w = 0.0
            num = 0
            for element in intersection:
                for i in range(len(bj_word)):
                    if element == bj_word[i]:
                        w += bj_weight[bj_index][i]
                        num += 1
            if num != 0:
                w /= num
            like = len(intersection) / len(union)
            like *= w
            if like > most_like:
                most_like = like
                most_index = [bj_index, hb_index]

        most_like_set.append(most_like)
        most_index_set.append(most_index)
    return most_like_set, most_index_set


def get_prop_frequent(x_list):
    a = {}
    for x in x_list:
        for y in x:
            for word in y.split(' '):
                if word in a:
                    a[word] += 1
                else:
                    a[word] = 1
    return a





if __name__ == '__main__':
    bj_x, bj_x_origin, bj_columns, bj_sources = get_bj_data()
    hb_x, hb_x_origin, hb_columns, hb_sources = get_hb_data()
    gz_x, gz_x_origin, gz_columns, gz_sources = get_gz_data()
    vocabulary, weight = get_tf_idf([bj_x, hb_x], True)
    # vocabulary, weight = get_tf_idf([hb_x, gz_x], True)
    # vocabulary, weight = get_tf_idf([bj_x, gz_x], True)
    vocab_embedding = get_embedding(vocabulary)

    bj_text_embed, hb_text_embed = get_text_embedding(vocab_embedding, weight, vocabulary, 431, 666)
    # bj_text_embed, gz_text_embed = get_text_embedding(vocab_embedding, weight, vocabulary, 431, 680)
    # hb_text_embed, gz_text_embed = get_text_embedding(vocab_embedding, weight, vocabulary, 666, 680)

    like_set, index_set = calculate_table_name_likelihood(bj_text_embed, hb_text_embed)

    mapping = []
    for i in range(len(like_set)):
        if like_set[i] > 0.9:
            mapping.append(index_set[i])

    tag = np.zeros(len(bj_x) + len(hb_x))
    bj_and_hb_x = []
    for mapping_index in mapping:
        bj_i = mapping_index[0]
        hb_i = mapping_index[1]
        temp = [bj_x[bj_i], hb_x[hb_i]][len(bj_x[bj_i]) > len(hb_x[hb_i])]
        bj_and_hb_x.append(temp)
        tag[bj_i] = 1
        tag[hb_i + len(bj_x)] = 1

    for i in range(0, len(bj_x) + len(hb_x)):
        if i < len(bj_x):
            if tag[i] == 0:
                bj_and_hb_x.append(bj_x[i])
            else:
                continue
        else:
            if tag[i] == 0:
                bj_and_hb_x.append(hb_x[i - len(bj_x)])
            else:
                continue

    vocabulary2, weight2 = get_tf_idf([bj_and_hb_x, gz_x], True)
    vocab_embedding_2 = get_embedding(vocabulary)
    bj_and_hb_textembed, gz_embed = get_text_embedding(vocab_embedding_2, weight2, vocabulary2, 1047, 680)
    like_set2, index_set2 = calculate_table_name_likelihood(bj_and_hb_textembed, gz_embed)

    # like_set, index_set = calculate_self_likehood(hb_text_embed)

    tra = []
    for a in mapping:
        tra.append(bj_x[a[0]] + '#' + hb_x[a[1]])

    # like_set_2, index_set_2 = calculate_property_likelihood(bj_columns, hb_columns)

    #tfidf 属性Jaccard系数计算
    # prop_vocab, prop_weight = get_tf_idf([bj_columns, hb_columns], False)
    # prop_like_set, prop_index_set = calculate_prop_like_weight(prop_weight, prop_vocab)

    # prop_map = get_prop_frequent([bj_columns, hb_columns])
    # bb = sorted(prop_map.items(), key= lambda x: x[1], reverse=True)

    # aa = []
    # for index in range(0, len(like_set)):
    #     pair = index_set[index]
    #     if len(pair) > 0:
    #         aa.append([hb_x_origin[pair[0]], hb_x_origin[pair[1]], like_set[index]])
    # bb = sorted(aa, key=lambda x: x[2], reverse=True)

    # aa = []
    # for index in range(0, len(like_set_2)):
    #     pair = index_set_2[index]
    #     if len(pair) > 0:
    #         aa.append([bj_x_origin[pair[0]], hb_x_origin[pair[1]], bj_columns[pair[0]], hb_columns[pair[1]], like_set_2[index]])
    #
    # bb = sorted(aa, key= lambda x: x[4], reverse= True)
    #

    #
    # aa = []
    # for index in range(0, len(prop_like_set)):
    #     pair = prop_index_set[index]
    #     if len(pair) > 0:
    #         aa.append([bj_x_origin[pair[0]], hb_x_origin[pair[1]], bj_columns[pair[0]], hb_columns[pair[1]], prop_like_set[index]])
    #
    # bb = sorted(aa, key= lambda x: x[4], reverse= True)


