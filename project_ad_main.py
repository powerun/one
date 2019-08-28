#!/usr/bin/python


from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers import cron
import project_ad_function
import project_ad_db
from flask import Flask,jsonify
app = Flask(__name__)
import configparser


log = project_ad_db.Logger()
ads = project_ad_function.OpenDoorAd(log)


@app.route("/update/", methods=['GET','POST'])
def update_html():
    log.logger.info('start html')
    try:
        if ads.pid_num() > 2:
            flag_num = 0
        else:
            flag_num = ads.main_ad()
    except Exception as e:
        log.logger.warning(e)
        code = {"code": "1"}
    else:
        if flag_num > 0:
            code = {"code": "0"}
        else:
            code = {"code": "1"}
    finally:
        log.logger.info('finish html')
    return jsonify(code)


def get_port(db_config='root.conf'):
    config = configparser.ConfigParser()
    config.read(db_config)
    host = config['APPRUN']['host']
    post = int(config['APPRUN']['port'])
    ft = config['APPRUN']['time']
    return host,post,ft


if __name__ == "__main__":
    host_i, port_i,f_time = get_port()
    scheduler = BackgroundScheduler()
    triggers = cron.CronTrigger(day='*/1', hour=f_time, minute='00',second='00')
    try:
        scheduler.add_job(ads.main_ad, trigger=triggers, args=[])
        # scheduler.add_job(project_ad_function.person_ad_process, trigger='interval', seconds=5, args=[log])
    except Exception as e:
        log.logger.warning(e)
    scheduler.start()
    app.run(host=host_i, port=port_i, debug=False)

