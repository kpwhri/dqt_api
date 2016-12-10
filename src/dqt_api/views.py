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
    if len(target) < 3:
        return 'Invalid search: must contain at least 3 characters.'
    terms = []
    # search category
    for c in models.Category.query.whooshee_search(target, order_by_relevance=-1):
        terms.append({
            'type': 'category',
            'id': c.id,
            'name': c.name,
            'description': c.description
        })
    # search item
    for i in models.Item.query.whooshee_search(target, order_by_relevance=-1):
        terms.append({
            'type': 'category',
            'id': i.id,
            'name': i.name,
            'description': i.description
        })

    # search value
    for v in models.Value.query.whooshee_search(target, order_by_relevance=-1):
        terms.append({
            'type': 'category',
            'id': v.id,
            'name': v.name,
            'description': v.description
        })

    return jsonify({'search': terms})
