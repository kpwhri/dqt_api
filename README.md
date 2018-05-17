Simple backend API for a data query tool.

# About #
Backend using flask with cherrypy or tornado server.

This manages a data cohort, allowing users to make requests of the data. See the `dqt-client` for more info on frontend use cases.

# Doco #

## Setup ##
1. Create a base directory where logging and configuration information can be stored, and put this in `config.py` file.


        BASE_DIR='path/to/local/storage'  # location for logs, etc.
        SECRET_KEY=b''  # generate this with os.urandom(24), any number is fine
        SQLALCHEMY_DATABASE_URI = 'sqlalchemy-connection-string'  # see # see: http://docs.sqlalchemy.org/en/latest/core/engines.html
        ORIGINS = ['*']  # origins to allow for cors
        # optional
        LOG_KEY=b''  # if you want to encrypt old log files, os.urandom(32)
        MASK = 5  # mask values smaller than this number
        AGE_STEP = 5  # increment for age graph
        AGE_MAX = 90  # max age to show (otherwise inferred from data)
        AGE_MIN = 60  # minimum age to show (otherwise inferred from data)
        COHORT_TITLE = 'ACT'  # short name for population under review

2. Install requirements with `pip install -r requirements.txt`

3. Load data using `load_csv.py`
    * This isn't meant to be a general purpose load script, so it will require some modification on your part
    * You can also auto-fill 100 subjects by using:
        * `python manage.py --method load --count 100 --config /path/to/config.py`

4. Run the application with `python dqt_api --config /path/to/config.py`.
