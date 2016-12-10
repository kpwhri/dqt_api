from flask import request, jsonify

from dqt_api import db, app, models


@app.route('/', methods=['GET'])
def index():
    return 'Home page.'


@app.route('/api/search', methods=['GET'])
def search():
    """Search target should use these conventions:
        space: +
        grouping: 'multiple+words'
    """
    target = request.args.get('query')
    terms = []
    # search category
    # for c in models.Category.query.whoosh_search(target):
    for c in models.Category.query.filter(models.Category.name.like('%{}%'.format(target))):
        terms.append({
            'type': 'category',
            'id': c.id,
            'name': c.name,
            'description': c.description
        })
    # search item
    # for i in models.Item.query.whoosh_search(target):
    for i in models.Item.query.filter(models.Item.name.like('%{}%'.format(target))):
        terms.append({
            'type': 'category',
            'id': i.id,
            'name': i.name,
            'description': i.description
        })

    # search value
    # for v in models.Value.query.whoosh_search(target):
    for v in models.Value.query.filter(models.Value.name.like('%{}%'.format(target))):
        terms.append({
            'type': 'category',
            'id': v.id,
            'name': v.name,
            'description': v.description
        })

    return jsonify({'search': terms})
