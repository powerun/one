#!/usr/bin/python

import random
import math
import pandas
import project_ad_db
import datetime
import sqlalchemy
import logging
from logging import handlers
from multiprocessing import Process, Lock, Value

MAX_WRITE_SQL_NUM = 3000



#MAX_AD_PERCENT = 0.6
#TOTAL_SCORE = 20


def tag_calculate(per, ad):
    per_data = per.values
    ad_data = ad.values
    result = 0
    # 性别标签
    if ad_data[3] is None or len(ad_data[3])==3:  # 广告无要求
        result += 1
    elif per_data[3] is None:  # 人数据错误
        return 0
    elif per_data[3] in ad_data[3]:  # 得分
        result += 1
    else:  # 人数据错误
        return 0
    # 孩子标签
    age_child = {'1': '0-0',
                 '2': '0-3',
                 '3': '4-6',
                 '4': '7-12',
                 '5': '13-18',
                 }
    if ad_data[5] is None or ad_data[5] == '1':  # 广告无要求 孩子系数0.1
        if per_data[5] == '0':
            result += 1
        else:
            result = result + (len(per_data[5 + 1].split(',')) - 1) * 0.1 + 1
    elif per_data[5 + 1] is None:  # 无小孩
        return 0
    else:
        score = get_score(per_data[5 + 1], ad_data[5], age_child, 0.5)  # 得分
        if score:
            result += score
        else:
            return 0
    # 年龄标签
    age_adult = {'0': '0-19',
                 '1': '20-29',
                 '2': '30-49',
                 '3': '50-200',
                 }
    if ad_data[2] is None or len(ad_data[2])==7:  #广告无要求
        result += 1
    elif not (str(per_data[2]).isdigit()) or per_data[2] is None or per_data[2] == 0 :  #人数据错误
        return 0
    else:
        score = get_score(str(int(per_data[2])), ad_data[2], age_adult, 0.2)  #得分
        if score:
            result += score
        else:
            pass
    # 收入标签
    if ad_data[6] is None or len(ad_data[6])==5:  #广告无要求
        result += 1
    elif per_data[6 + 1] is None:  #陌生人处理，系数0.1
        result += 0.1
    elif per_data[6 + 1] in ad_data[6]:  #得分
        result += 1
    else:  #得分
        result += min([abs(int(i) - int(per_data[6 + 1])) for i in ad_data[6].split(',')])*0.1
    return result


def percent_random_ad(ad, ad_weight):
    tem = []
    ad_data = ad.values
    MAX_SQL_NUM = 20
    ################################################
    MAX_AD_NUM = 3    # 广告数量
    ################################################
    '''j = 0
    for i in ad_weight_sort:
        if i/TOTAL_SCORE >= MAX_AD_PERCENT:
            j += 1
        else:
            break
    MAX_AD_NUM = j'''
    ################################################
    j = len(ad_weight) - ad_weight.count(0)
    if MAX_AD_NUM > j:
        MAX_AD_NUM = j
    ################################################
    if MAX_AD_NUM <= 0:
        print(len(ad_weight), '  ',ad_weight.count(0), '  ',j, '  ',MAX_AD_NUM)
        return None
    else:
        tem_data = list(zip(ad_data, ad_weight))
        random.shuffle(tem_data)
        tem_data = sorted(tem_data, key=lambda x:x[1], reverse=True)
        tem_data = tem_data[0:MAX_AD_NUM]
        #print(tem_data)
        weight_sum = sum([x[1] for x in tem_data])
        if MAX_AD_NUM<MAX_SQL_NUM:
            for i in range(MAX_AD_NUM):
                weight = round(tem_data[i][1]/weight_sum*MAX_SQL_NUM)
                for j in range(weight):
                    tem.append(tem_data[i][0])
            tem_len = len(tem)
            if tem_len>MAX_SQL_NUM:
                tem = tem[0:MAX_SQL_NUM]
            elif tem_len<MAX_SQL_NUM:
                tem = tem + random.sample(tem, MAX_SQL_NUM-tem_len)
            else:
                pass
            random.shuffle(tem)
            #tem = random.sample(tem, len(tem))
            return tem
        else:
            #tem = dict(tem_data[0:MAX_SQL_NUM])
            #tem = list(tem.keys())
            tem = dict(tem_data)
            tem = list(tem.keys())
            tem = random.sample(tem, MAX_SQL_NUM)
            return tem


def get_score(person, ad, ad_dict, gamma_k):
    person_len = len(person)
    ad_len = len(ad)
    if ad_len<=0:
        result = 1
    elif person_len>0 and ad_len>0:
        result = [0]
        person_data = person.split(',')
        ad_data = ad.split(',')
        for j in person_data:
            j_person = int(j)
            for i in ad_data:
                ad_tem = ad_dict[i].split('-')
                ad_tem = [int(k) for k in ad_tem]
                delta_up = ad_tem[1] - j_person
                delta_down = j_person - ad_tem[0]
                if delta_up>=0 and delta_down>=0:
                    result += [1]
                    break
                else:
                    x = min(abs(delta_up), abs(delta_down))
                    result += [gamma_function(x, gamma_k)]
        result_count = result.count(1)
        if result_count==0:
            return max(result)
        elif result_count==1:
            return 1
        else:
            return 1 + (result_count-1)*0.1   #系数0.1
    else:
        result = 0
    return result


def gamma_function(x, k, a=0):
    gamma=lambda x:math.exp(-k*(x-a))
    if(x<=a):
        return 1
    else:
        return gamma(x)


class Logger():
    level_relations = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'critical':logging.CRITICAL
    }
    def __init__(self,filename,level='info',when='D',backCount=7,fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)
        self.logger.setLevel(self.level_relations.get(level))
        sh = logging.StreamHandler()
        sh.setFormatter(format_str)
        th = handlers.TimedRotatingFileHandler(filename=filename,when=when,backupCount=backCount,encoding='utf-8')
        th.setFormatter(format_str)
        self.logger.addHandler(sh)
        self.logger.addHandler(th)


def person_ad_process(log, per_data, ad_data, lock, num):
    if per_data is None or ad_data is None:
            return None
    per_num = len(per_data)
    ad_num = len(ad_data)
    tem = [[]] * MAX_WRITE_SQL_NUM
    max_write_k = 0
    x = project_ad_db.data_base()
    open1 = x.sql_open(log)
    for i in range(0, per_num):
        per_tem = []
        tem[max_write_k] = tem[max_write_k] + [per_data.iloc[i, 1]]
        for j in range(0, ad_num):
            per_tem.append(tag_calculate(per_data.iloc[i, :], ad_data.iloc[j, :]))
        '''lock.acquire()
        f = open('test.txt','a+')
        f.write(str([per_data.iloc[i, 1]]+per_tem))
        f.write('\n')
        f.close()
        lock.release()'''
        ad_tem2 = percent_random_ad(ad_data.iloc[:, 0], per_tem)
        ad_result = ''
        if ad_tem2 is None:
            pass
        else:
            for ad_random in ad_tem2[:-1]:
                ad_result = ad_result + ad_random + ','
            ad_result = ad_result + ad_tem2[-1]
        tem[max_write_k] = tem[max_write_k] + [ad_result]
        if (max_write_k == MAX_WRITE_SQL_NUM-1):
            lock.acquire()
            #write_data(tem[0: max_write_k + 1], log, num)
            project_ad_db.insert_sql_data(open1, tem[0: max_write_k + 1], log, num)
            lock.release()
            tem = [[]] * MAX_WRITE_SQL_NUM
            max_write_k = 0
        elif (i == per_num-1):
            lock.acquire()
            #write_data(tem[0: max_write_k + 1], log, num)
            project_ad_db.insert_sql_data(open1, tem[0: max_write_k + 1], log, num)
            lock.release()
        else:
            max_write_k += 1
    open1.close()


def write_data(data, log, num):
    d_type = {'ID': sqlalchemy.types.NVARCHAR(length=32),
              'RESI_HOUSE_ID': sqlalchemy.types.NVARCHAR(length=32),
              'MTRL_ID': sqlalchemy.types.NVARCHAR(length=700),
              'CREATE_DTTM': sqlalchemy.types.DateTime(),
              'PLAY_DT': sqlalchemy.types.Date(),
              }
    data_sql = pandas.DataFrame(data=data, columns=list(d_type.keys()))
    try:
        num.value += project_ad_db.write_sql_data(data_sql, 't_resi_adv_relaship', d_type, 'append', log)
    except Exception as e:
        log.logger.warning(e)


def read_data(log):
    try:
        #per_data = project_ad_db.get_sql_data('t_resi_attr', log)
        per_data = project_ad_db.per_sql_data(log)
        today_time = datetime.datetime.now().strftime('%Y-%m-%d')
        ad_data = project_ad_db.ad_sql_data(today_time, log)
    except Exception as e:
        log.logger.warning(e)
    else:
        return per_data,ad_data


def main_test(log):
    s = datetime.datetime.now()
    per_data, ad_data = read_data(log)
    third_len = int(len(per_data)/3)
    lock = Lock()
    #cores = multiprocessing.cpu_count()
    #pool = multiprocessing.Pool(processes=cores)
    result_num = Value('i', 0)
    p = Process(target=person_ad_process, args=(log, per_data[0:third_len], ad_data, lock, result_num))
    p2 = Process(target=person_ad_process, args=(log, per_data[third_len:third_len*2], ad_data, lock, result_num))
    p3 = Process(target=person_ad_process, args=(log, per_data[third_len*2:], ad_data, lock, result_num))
    p.start()
    p2.start()
    p3.start()
    p.join()
    p2.join()
    p3.join()
    e = datetime.datetime.now()
    log.logger.info(e - s)
    log.logger.info(result_num.value)


if __name__ == '__main__':
    log = Logger('record.log', level='debug')
    s = datetime.datetime.now().strftime('%Y-%m-%d')
    ad_data = project_ad_db.ad_sql_data(s, log)
    ad_data.to_csv('aaa.csv')
    print(ad_data)



