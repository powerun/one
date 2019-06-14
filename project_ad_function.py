#!/usr/bin/python

import random
import math
import project_ad_db
import datetime
import logging
from logging import handlers
from multiprocessing import Process, Value


#MAX_AD_PERCENT = 0.6
#TOTAL_SCORE = 20


def tag_calculate(per_data, ad_data):
    result = 0
    # 性别标签
    if ad_data[3] is None or len(ad_data[3])==3:  # 广告无要求
        result_sex = 0.99                         # 0.99为无限制系数
    elif per_data[3] is None:  # 人数据错误
        return 0
    elif per_data[3] in ad_data[3]:  # 得分
        result_sex = 1
    else:  # 人数据错误
        return 0
    # 孩子标签
    age_child = {'1': '0-0',
                 '2': '0-3',
                 '3': '4-6',
                 '4': '7-12',
                 '5': '13-18',
                 }
    if ad_data[5] is None or len(ad_data[5]) == 9:  # 广告无要求
        result_child = 0.99
    elif (per_data[5 + 1] is None) and (not('1'in ad_data[5])):  # 要求有，而实际无
        return 0
    elif (per_data[5 + 1] is None) and ('1'in ad_data[5]):  # 无小孩
        result_child = 1
    else:
        score = get_score(per_data[5 + 1], ad_data[5], age_child, 0.5)  # 得分
        if score:
            result_child = score
        else:
            return 0
    # 年龄标签
    age_adult = {'0': '0-19',
                 '1': '20-29',
                 '2': '30-49',
                 '3': '50-200',
                 }
    if ad_data[2] is None or len(ad_data[2])==7:  # 广告无要求
        result_age = 0.99
    elif not (str(per_data[2]).isdigit()) or per_data[2] is None:  # 人数据错误
        return 0
    else:
        score = get_score(str(int(per_data[2])), ad_data[2], age_adult, 0.2)  # 得分
        if score:
            result_age = score
        else:
            result_age = 0
    # 收入标签
    if ad_data[6] is None or len(ad_data[6])==5:  # 广告无要求
        result_income = 0.99
    elif per_data[6 + 1] is None:  # 陌生人处理，系数0.1
        result_income = 0.1
    elif per_data[6 + 1] in ad_data[6]:  # 得分
        result_income = 1
    else:  # 得分
        result_income = min([abs(int(i) - int(per_data[6 + 1])) for i in ad_data[6].split(',')])*0.1
    return (result+result_sex+result_child+result_age+result_income)


def percent_random_ad(ad_data, ad_weight):
    tem = []
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
        return []
    else:
        tem_data = list(zip(ad_data, ad_weight))
        random.shuffle(tem_data)
        tem_data = sorted(tem_data, key=lambda x:x[1], reverse=True)
        tem_data = tem_data[0:MAX_AD_NUM]
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
            #random.shuffle(tem)   #顺序打乱
            #tem = random.sample(tem, len(tem))   #或 顺序打乱
            return tem
        else:
            tem = dict(tem_data[0:MAX_SQL_NUM])   #顺序不打乱1
            tem = list(tem.keys())   #顺序不打乱2
            #tem = dict(tem_data)   #顺序打乱1
            #tem = list(tem.keys())   #顺序打乱2
            #tem = random.sample(tem, MAX_SQL_NUM)   #顺序打乱3
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
                    return 1   #满足要求，返回
                    #break   #满足继续计算，全部计算完成
                else:
                    x = min(abs(delta_up), abs(delta_down))
                    result += [gamma_function(x, gamma_k)]
        '''   #满足继续计算，全部计算完成
        result_count = result.count(1)
        if result_count==0:
            return max(result)
        else:
            return 1 + (result_count-1)*0.01   #系数0.01
        '''
        return max(result)
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


def read_data(log):
    today_time = datetime.datetime.now()
    yesterday_time = today_time + datetime.timedelta(days=-1)
    today_time = today_time.strftime('%Y-%m-%d')
    yesterday_time = yesterday_time.strftime('%Y-%m-%d')
    x = project_ad_db.data_base()
    pool = x.pool_open(log)
    per_num = 0
    ad_data = []
    if pool:
        try:
            open = pool.connection()
            # project_ad_db.delete_table(open, 't_resi_adv_relaship')
            per_num, ad_data = project_ad_db.get_ad_data(open, yesterday_time, today_time)
            per_num = per_num.iat[0, 0]
        except Exception as e:
            log.logger.warning(e)
        else:
            open.close()
        finally:
            pool.close()
    else:
        pass
    return per_num, ad_data


def write_data(log, per1, per2, ad_data, flag_num):
    sql_order = "replace into t_resi_adv_relaship(RESI_HOUSE_ID, MTRL_ID, PLAY_DT, CREATE_DTTM) VALUES (%s,%s,CURDATE(),NOW())"
    x = project_ad_db.data_base()
    pool = x.pool_open(log)
    if pool:
        try:
            open = pool.connection()
            per_data = project_ad_db.get_per_data(open, per1, per2)
            cur = open.cursor()
            s = datetime.datetime.now()
            for per in per_data:
                tag_coefficient = []
                ad_data_temp = ad_data[ad_data.AREA_ID == per[-1]]
                for j in range(ad_data_temp.shape[0]):
                    tag_coefficient.append(tag_calculate(per, ad_data_temp.iloc[j, :].values))
                tag_result = percent_random_ad(ad_data_temp.iloc[:, 0].values, tag_coefficient)
                cur.execute(sql_order, [per[1]] + [','.join(tag_result)])
            e = datetime.datetime.now()
        except Exception as e:
            flag_num.value -= 1
            log.logger.warning(e)
        else:
            open.commit()
            cur.close()
            open.close()
            print(e - s)
        finally:
            pool.close()
    else:
        flag_num.value -= 1


def main_ad(log):
    s = datetime.datetime.now()
    per_num, ad_data = read_data(log)
    flag_num = Value('i', 1)
    if per_num <= 0 or len(ad_data) <= 0:
        flag_num.value -= 1
    else:
        process_num = 3
        per_data_one = int(per_num / process_num) + 1
        process_num = range(1, process_num + 1)
        process_list = []
        for i, j in enumerate(process_num):
            p = Process(target=write_data, args=(log, per_data_one * i, per_data_one, ad_data, flag_num))
            p.start()
            process_list.append(p)
        for p in process_list:
            p.join()
    e = datetime.datetime.now()
    log.logger.info(flag_num.value)
    log.logger.info(e - s)
    log.logger.info('===============')
    return flag_num.value


if __name__ == '__main__':
    log = Logger('record.log', level='debug')
    per, ad = read_data(log)
    # ad.to_csv('aaa.csv')
    print(ad)
    print(per)





