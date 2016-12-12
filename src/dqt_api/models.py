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


@whooshee.register_model('name', 'description')
class Value(db.Model):
    """A possible value for an item.
    At the moment, this will even include values when there is a decimal range/natural ordering.
    """
    __searchable__ = ['name', 'description']

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))
    description = db.Column(db.String(500))


class ItemValue(db.Model):
    """The set of values that are associated with each item.
    """
    id = db.Column(db.Integer, primary_key=True)
    item = db.Column(db.Integer, db.ForeignKey('item.id'))
    value = db.Column(db.Integer, db.ForeignKey('value.id'))
    range_max = db.Column(db.Boolean)  # true: max of range; false: min of range; null: categorical relationship
