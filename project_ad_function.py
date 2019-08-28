#!/usr/bin/python

import random
import math
import project_ad_db
import pandas as pd
import datetime
import psutil
from multiprocessing import Process, Value


class OpenDoorAd():

    def __init__(self, log):
        self.log = log

    def tag_calculate_ad(self, per_data, ad_data):
        # 性别标签
        if ad_data[3] is None or len(ad_data[3]) == 3:  # 广告无要求
            result_sex = 0.99  # 0.99为无限制系数
        elif per_data[3] is None:  # 人数据错误
            return 0
        elif per_data[3] in ad_data[3]:  # 得分
            result_sex = 1 - 0.01*(len(ad_data[3])-1)/3.0
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
        elif (per_data[5] == '0') and (not ('1' in ad_data[5])):  # 要求有，而实际无
            return 0
        elif (per_data[5] == '0') and ('1' in ad_data[5]):  # 无小孩
            result_child = 1
        else:
            score = self.score_calculate_ad(per_data[5 + 1], ad_data[5], age_child, 0.5, 9)  # 得分
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
        if ad_data[2] is None or len(ad_data[2]) == 7:  # 广告无要求
            result_age = 0.99
        elif not (str(per_data[2]).isdigit()) or per_data[2] is None:  # 人数据错误
            return 0
        else:
            score = self.score_calculate_ad(str(int(per_data[2])), ad_data[2], age_adult, 0.2, 7)  # 得分
            if score:
                result_age = score
            else:
                result_age = 0
        # 收入标签
        if ad_data[6] is None or len(ad_data[6]) == 5:  # 广告无要求
            result_income = 0.99
        elif per_data[6 + 1] is None:  # 陌生人处理，系数0.1
            result_income = 0.1
        elif per_data[6 + 1] in ad_data[6]:  # 得分
            result_income = 1 - 0.01*(len(ad_data[6])-1)/5.0
        else:  # 得分
            result_income = min([abs(int(i) - int(per_data[6 + 1])) for i in ad_data[6].split(',')]) * 0.1
        '''
        delta_day = (datetime.datetime.now() - ad_data[7]).days
        if delta_day>30:
            result_day = 0
        else:
            result_day = self.gamma_value(delta_day, 0.2)
        '''
        return (result_sex + result_child + result_age + result_income)

    def percent_random_ad(self, tag_data, MAX_SQL_NUM = 20, MAX_AD_NUM = 3):
        ################################################
        # MAX_AD_PERCENT = 0.6
        # TOTAL_SCORE = 20
        '''j = 0
        for i in ad_weight_sort:
            if i/TOTAL_SCORE >= MAX_AD_PERCENT:
                j += 1
            else:
                break
        MAX_AD_NUM = j'''
        ################################################
        if tag_data:
            tag_len = len(tag_data)
            if MAX_AD_NUM > tag_len:
                MAX_AD_NUM = tag_len
            random.shuffle(tag_data)
            sort_tem = sorted(tag_data, key=lambda x:x[1], reverse=True)
            sort_tem = sort_tem[0:MAX_AD_NUM]
            tem_data = list(zip(*sort_tem))
            if MAX_AD_NUM < MAX_SQL_NUM:
                tem = []
                weight_sum = sum(tem_data[1])
                weight2 = (round(x/weight_sum*MAX_SQL_NUM) for x in tem_data[1])
                tuple(tem.extend([i] * j) for i, j in zip(tem_data[0], weight2))
                tem_len = len(tem)
                if tem_len > MAX_SQL_NUM:
                    tem = tem[0:MAX_SQL_NUM]
                elif tem_len < MAX_SQL_NUM:
                    tem = tem + random.sample(tem, MAX_SQL_NUM-tem_len)
                else:
                    pass
                #random.shuffle(tem)
                #tem = random.sample(tem, len(tem))
            else:
                tem = tem_data[0][0:MAX_SQL_NUM]
                #tem = random.sample(tem, MAX_SQL_NUM)
            return (','.join(tem))
        else:
            return ''

    def score_calculate_ad(self, person, ad, ad_dict, gamma_k, tag_num):
        person_len = len(person)
        ad_len = len(ad)
        if ad_len <= 0:
            result = 1
        elif person_len > 0 and ad_len > 0:
            result = [0]
            person_data = map(int, person.split(','))
            ad_data = ad.split(',')
            for i in ad_data:
                ad_tem = ad_dict.get(i,'0-0').split('-')
                for j_person in person_data:
                    delta_up = int(ad_tem[1]) - j_person
                    delta_down = j_person - int(ad_tem[0])
                    if delta_up >= 0 and delta_down >= 0:
                        return 1 - 0.01*(ad_len-1)/tag_num
                        # result += [1]
                        # break
                    else:
                        x = min(abs(delta_up), abs(delta_down))
                        result.append(self.gamma_value(x, gamma_k))
            '''   
            result_count = result.count(1)
            if result_count==0:
                return max(result)
            else:
                return 1 + (result_count-1)*0.01  
            '''
            return max(result)
        else:
            result = 0
        return result

    def gamma_value(self, x, k, a=0):
        if (x <= a):
            return 1
        else:
            return math.exp(-k * (x - a))

    def pid_num(self):
        num = 0
        for i in psutil.pids():
            pid_tem = psutil.Process(i)
            if 'project_ad_main' in pid_tem.name():
                num += 1
        return num

    def read_data(self):
        today_time = datetime.datetime.now()
        today_time = today_time.strftime('%Y-%m-%d')
        #yesterday_time = today_time + datetime.timedelta(days=-1)
        #yesterday_time = yesterday_time.strftime('%Y-%m-%d')
        x = project_ad_db.data_base()
        open = x.sql_open(self.log)
        if open:
            try:
                per, ad_data = self.get_ad_mysql(open, today_time)
                per_num = per.iat[0, 0]
            except Exception as e:
                self.log.logger.warning(e)
                return 0, []
            else:
                return per_num, ad_data
            finally:
                open.close()

    def write_data(self, per1, per2, ad_data, flag_num):
        sql_order = "replace into t_resi_adv_relaship(RESI_HOUSE_ID, MTRL_ID, PLAY_DT, CREATE_DTTM) VALUES (%s,%s,CURDATE(),NOW())"
        x = project_ad_db.data_base()
        pool = x.pool_open(self.log)
        dict_max = 300
        dict_per = {}
        if pool:
            try:
                open = pool.connection()
            except Exception as e:
                flag_num.value -= 1
                self.log.logger.warning(e)
            else:
                try:
                    per_data = self.get_per_mysql(open, per1, per2)
                    cur = open.cursor()
                    ad_data_size = ad_data.shape[0]
                    for per in per_data:
                        per_dict_key = '-'.join(tuple(map(str, per[2:])))
                        if per_dict_key in dict_per:
                            cur.execute(sql_order, [per[1], dict_per.get(per_dict_key)])
                        else:
                            tag_coefficient = []
                            for j in range(ad_data_size):
                                if per[-1] == ad_data.iat[j, -1]:
                                    coefficient_one = self.tag_calculate_ad(per, ad_data.iloc[j, :].values)
                                    if coefficient_one:
                                        tag_coefficient.append((ad_data.iat[j, 0], coefficient_one))
                            if tag_coefficient:
                                tag_result = self.percent_random_ad(tag_coefficient)
                                cur.execute(sql_order, [per[1], tag_result])
                                if len(dict_per) > dict_max:
                                    dict_per.pop(list(dict_per.keys())[0])
                                dict_per.update({per_dict_key: tag_result})
                except Exception as e:
                    flag_num.value -= 1
                    self.log.logger.warning(e)
                else:
                    open.commit()
                    cur.close()
                finally:
                    open.close()
            finally:
                pool.close()
        else:
            flag_num.value -= 1

    def get_per_mysql(self, open, per1, per2):
        cur = open.cursor()
        sql_order = '''SELECT CONCAT(t2.ID,'_',t1.RESI_HOUSE_ID)as ID,CONCAT(t2.ID,'_',t1.RESI_HOUSE_ID)as RESI_HOUSE_ID,CONVERT(t1.AGE,CHAR(3))as AGE,t1.SEX,t1.RESI_TYPE,t1.IS_HAVE_CHILD,t1.CHILD_AGE,t1.INCOME_STATUS,t2.ID as AREA_ID
                    from (SELECT ID FROM t_area_info) t2 CROSS JOIN (SELECT * FROM t_resi_attr where RESI_HOUSE_ID in ('00','01','02','03','10','11','12','13')) t1
                    UNION ALL
                    SELECT t1.ID,t1.RESI_HOUSE_ID,t1.AGE,t1.SEX,t1.RESI_TYPE,t1.IS_HAVE_CHILD,t1.CHILD_AGE,t1.INCOME_STATUS,t2.AREA_ID
                    FROM t_resi_attr t1,t_resident_house_info t2
                    WHERE t1.RESI_HOUSE_ID=t2.ID
                    AND t1.IS_DELETE='0'
                    AND t1.RESI_HOUSE_ID <> '' LIMIT %s,%s''' % (per1, per2)
        cur.execute(sql_order)
        while True:
            result = cur.fetchone()
            # result = cur.fetchmany(11)
            # if result is ():
            if result is None:
                cur.close()
                break
            else:
                yield (result)

    def get_ad_mysql(self, open, today):
        sql_order_per = '''SELECT COUNT(t1.RESI_HOUSE_ID)+ (SELECT COUNT(ID)*8-8 FROM t_area_info)
                        FROM t_resi_attr t1
                        WHERE t1.IS_DELETE='0'; '''
        sql_order_ad = '''SELECT t3.ADV_MTRL_ID,t2.CAMP_ID,t2.AGE,t2.SEX,t2.RESI_TYPE,t2.CHILD_AGE,t2.INCOME_STATUS,t2.UPDATE_DTTM,t5.AREA_ID
                        FROM t_adv_camp t1 
                        LEFT JOIN (select t2.CAMP_ID, sum(t1.PLAY_NUM) playNum from t_adv_mtrl_play_count t1, t_adv_camp_mtrl t2 where t1.ADV_MTRL_ID = t2.ADV_MTRL_ID and t1.COUNT_DT <= '%s' group by t2.CAMP_ID) s1 on t1.id = s1.CAMP_ID,t_adv_cond_resi t2,t_adv_camp_mtrl t3, t_adv_mtrl t4, t_adv_cond_area t5
                        WHERE
                        t1.ID = t2.CAMP_ID
                        AND t1.ID = t3.CAMP_ID
                        AND t1.ID = t5.CAMP_ID
                        AND t3.ADV_MTRL_ID = t4.ID
                        AND '%s' >= t1.REAL_START_DT 
                        AND t1.IS_DELETE = '0' 
                        AND t2.IS_DELETE = '0'
                        AND IFNULL(s1.playNum,0) < (IFNULL(t1.EXPECT_ADV_NUM,0)  * 1000)
                        AND t4.WIDTH = '1024'
                        AND t4.HEIGHT = '1345'
                        AND t1.ADV_TYPE = '2'
                        AND t1.CAMP_STATUS in ('unplayed','playing')
                        AND t1.is_Legal_hday_play in ('0', (SELECT is_Legal_hday FROM t_sys_calendar WHERE f_date='%s'))''' % (
            today, today, today)
        data_ad = pd.read_sql(sql_order_ad, con=open)
        len_per = pd.read_sql(sql_order_per, con=open)
        return len_per, data_ad

    def main_ad(self):
        s = datetime.datetime.now()
        per_num, ad_data = self.read_data()
        flag_num = Value('i', 1)
        if per_num <= 0 or len(ad_data) <= 0:
            flag_num.value -= 1
        else:
            process_num = 2
            per_data_one = int(per_num / process_num) + 1
            process_list = []
            s2 = datetime.datetime.now()
            for i in range(process_num):
                p = Process(target=self.write_data, args=(per_data_one * i, per_data_one, ad_data, flag_num))
                p.start()
                process_list.append(p)
            for p in process_list:
                p.join()
                e2 = datetime.datetime.now()
                self.log.logger.info(e2 - s2)
                self.log.logger.info(p.pid)
        e = datetime.datetime.now()
        self.log.logger.info(flag_num.value)
        self.log.logger.info(e - s)
        return flag_num.value

