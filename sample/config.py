# base directory for logs, etc. to be stored in
BASE_DIR = r'C:\data\config\dqt'
# os.urandom(n)
SECRET_KEY = b'>)r\x01\xc5\xeap q\xa1|\x89\xa8gq\tX\x95\xb3\x8d\xadgA\xf7'
# os.urandom(32)
LOG_KEY = b'6\x1d\xef\n\xd8\x8clT/\xf3h1v\xebfF\xa6\x9f\xc2\xc0-e"\xab\xd9\xcf\x93\xf6\x1f\xad\xe9c'
SQLALCHEMY_DATABASE_URI = r'mssql+pyodbc://SERVER/DATABASE?driver=SQL Server'
AGE_STEP = 5
MASK = 5  # don't show groups with values smaller than this
AGE_MAX = 100  # maximum age to show (everything above will be shown as 100+)
AGE_MIN = 30  # minimum age to show (everything below will be shown as <=30)
ORIGINS = ['*']  # best to supply a specific URL
COHORT_TITLE = 'My Data'
UPDATE_DATE = 'January 2020'
