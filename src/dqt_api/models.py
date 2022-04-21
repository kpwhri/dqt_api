"""
SQLAlchemy-style models for building backend database.
"""

from datetime import datetime

from dqt_api import db, whooshee


class Variable(db.Model):
    """Any variable which can be used to filter the dataset.
    """
    id = db.Column(db.Integer, primary_key=True)
    case = db.Column(db.Integer)  # represents subject
    item = db.Column(db.Integer, db.ForeignKey('item.id'))
    value = db.Column(db.Integer, db.ForeignKey('value.id'))


@whooshee.register_model('name', 'description')
class Item(db.Model):
    """Items can take a variety of values and belong to a larger category.
    Examples: 'sex', 'race', 'any dementia'
    """
    __searchable__ = ['name', 'description']

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    description = db.Column(db.String(500))
    category = db.Column(db.Integer, db.ForeignKey('category.id'))
    is_numeric = db.Column(db.Boolean, default=False)  # True: associated are numeric, not categorical


@whooshee.register_model('name', 'description')
class Category(db.Model):
    """Container for a group of items and the target of searches.
    """
    __searchable__ = ['name', 'description']

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))
    description = db.Column(db.String(500))
    order = db.Column(db.SmallInteger)


# @whooshee.register_model('name', 'description')
class Value(db.Model):
    """A possible value for an item.
    At the moment, this will even include values when there is a decimal range/natural ordering.
    """
    __searchable__ = ['name', 'description']

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))  # this feels too long
    name_numeric = db.Column(db.Float)
    description = db.Column(db.String(500))
    order = db.Column(db.SmallInteger)

    def __init__(self, name, description=None, order=None):
        try:
            self.name_numeric = float(name)
        except ValueError:
            self.name_numeric = None
        self.name = name
        self.description = description
        self.order = order


class DataModel(db.Model):
    """Data table for graphing/other tables.
    This table must be modified to extract additional information for graphs/tables
    """
    case = db.Column(db.Integer, primary_key=True)
    age_bl = db.Column(db.Integer)
    age_fu = db.Column(db.Integer)
    sex = db.Column(db.String(10))
    enrollment = db.Column(db.String(15))
    followup_years = db.Column(db.Integer)
    intake_date = db.Column(db.Integer)


class UserData(db.Model):
    """Table for collecting information for the users.
    """
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))
    email_address = db.Column(db.String(100))
    affiliation = db.Column(db.String(50))
    reason_for_visiting = db.Column(db.String(200))
    ip_address = db.Column(db.String(20))
    cookie = db.Column(db.String(20))
    visit_date_utc = db.Column(db.DateTime, default=datetime.utcnow)


class TabData(db.Model):
    """Data for populating tabs.
    """
    id = db.Column(db.Integer, primary_key=True)
    header = db.Column(db.String(15))
    line = db.Column(db.SmallInteger)
    text_type = db.Column(db.String(10))  # header, bold, text, etc. (formatting)
    text = db.Column(db.Text)
    order = db.Column(db.SmallInteger)


class Comment(db.Model):
    """Comments to include on graphs and locations"""
    id = db.Column(db.Integer, primary_key=True)
    location = db.Column(db.String(10))  # e.g., "table"
    line = db.Column(db.SmallInteger)
    comment = db.Column(db.String(200))


class DataEntry(db.Model):
    """Optional: Formal data dictionary for loading data dictionary page"""
    id = db.Column(db.Integer, primary_key=True)
    label = db.Column(db.String(100))
    variable = db.Column(db.String(50 ))  # underlying variable name
    values = db.Column(db.Text)
    category = db.Column(db.String(50))
    description = db.Column(db.Text)


class DataFile(db.Model):
    """Data dictionary files"""
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(50))
    file = db.Column(db.LargeBinary)
    md5_checksum = db.Column(db.String(32))
