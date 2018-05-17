import datetime
import os

from apscheduler.schedulers.background import BackgroundScheduler

from dqt_api import app

scheduler = BackgroundScheduler()
scheduler.start()


def remove_old_logs():
    today = datetime.date.today()
    days = app.config.get('LOG_RETENTION_DAYS', 365)
    base_dir = app.config['LOG_DIR']
    for fn in os.listdir(base_dir):
        if fn.startswith('log_file') and '.' in fn:
            _, date_str, *_ = fn.split('.')
            f_dt = datetime.datetime.strptime(date_str[:10], '%Y-%m-%d').date()
            if (today - f_dt).days > days:
                os.remove(os.path.join(base_dir, fn))
