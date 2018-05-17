Simple backend API for a data query tool.

# About #
Backend using flask with cherrypy server.

# Doco #
## Setup ##
Create a base directory where logging and configuration information can be stored, and put this in `config.py` file.


    BASE_DIR='path/to/local/storage'  # location for logs, etc.
    SECRET_KEY=b''  # generate this with os.urandom(24), any number is fine
    LOG_KEY=b''  # if you want to encrypt old log files, os.urandom(32)
    SQLALCHEMY_DATABASE_URI = 'sqlalchemy-connection-string'  # see # see: http://docs.sqlalchemy.org/en/latest/core/engines.html
    ORIGINS = ['*']  # origins to allow for cors
    # optional
    MASK = 5  # mask values smaller than this number
     AGE_STEP = 5  # increment for age graph
     AGE_MAX = 90  # max age to show (otherwise inferred from data)
     AGE_MIN = 60  # minimum age to show (otherwise inferred from data)
     COHORT_TITLE = 'ACT'  # short name for population under review

Install requirements with `pip install -r requirements.txt`

Run the application with `python dqt_api --config /path/to/config.py`.
