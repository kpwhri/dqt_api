from flask import Flask
from flask.ext.whooshee import Whooshee
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
whooshee = Whooshee(app)
db = SQLAlchemy(app)
