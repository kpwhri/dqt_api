from flask import Flask
from flask_cors import CORS
from dqt_api.flask_whooshee import Whooshee
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
whooshee = Whooshee()
db = SQLAlchemy()
cors = CORS()
