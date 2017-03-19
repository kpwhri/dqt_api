from flask import Flask
from flask_cors import CORS
from flask_whooshee import Whooshee
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
whooshee = Whooshee(app)
db = SQLAlchemy(app)
cors = CORS()
