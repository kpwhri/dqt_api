from flask import request, jsonify
import pandas as pd

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
            'type': 'item',
            'id': i.id,
            'name': i.name,
            'description': i.description
        })

    # search value
    for v in models.Value.query.whooshee_search(target, order_by_relevance=-1):
        terms.append({
            'type': 'value',
            'id': v.id,
            'name': v.name,
            'description': v.description
        })

    return jsonify({'search': terms})


@app.route('/api/filter', methods=['GET'])
def api_filter():
    """Filter population based on parameters.

    TODO: parameterize items to return
    """
    # get set of cases
    cases = None
    for key, val in request.args.lists():
        cases_ = set(
            db.session.query(models.Variable.case).filter(
                models.Variable.item == key,
                models.Variable.value.in_(val)
            ).all()
        )
        if cases:
            cases &= cases_
        else:
            cases = cases_
    cases = [x[0] for x in list(cases)]

    # get data for graphs
    res = {}

    items = {
        models.Item.query.filter_by(name='Sex').first().id: 'sex',
        models.Item.query.filter_by(name='Current Status').first().id: 'current_status',
        models.Item.query.filter_by(name='Age').first().id: 'age'
    }

    data = []
    curr = {}
    curr_inst = None
    for inst in db.session.query(models.Variable).filter(
            models.Variable.case.in_(cases),
            models.Variable.item.in_(items)
    ).order_by(models.Variable.case, models.Variable.item):
        if inst.case != curr_inst:
            if curr:
                data.append(curr)
                curr = {}
            curr_inst = inst.case
        curr[items[inst.item]] = db.session.query(models.Value.name).filter(models.Value.id == inst.value).first()[0]
    data.append(curr)
    res['data'] = data
    res['count'] = len(data)

    # df = pd.DataFrame(data)
    return jsonify(res)


@app.route('/api/category/add/<int:category_id>', methods=['GET'])
def add_category(category_id):
    """Get information about a particular category.

    """
    res = {'items': []}
    for item in models.Item.query.filter_by(category=category_id):
        vals = [x[0] for x in db.session.query(models.Variable.value).filter(models.Variable.item == item.id)]
        res['items'].append({
            'name': item.name,
            'id': item.id,
            'description': item.description,
            'values': [
                {'id': v.id, 'name': v.name, 'description': v.description
                 } for v in db.session.query(models.Value).filter(models.Value.id.in_(vals)).order_by(models.Value.name)
                ]
        })
    category = models.Category.query.filter_by(id=category_id).first()
    res['name'] = category.name
    res['description'] = category.description
    return jsonify(res)


@app.route('/api/item/add/<int:item_id>', methods=['GET'])
def add_category_from_item(item_id):
    """Get category from item

    """
    return add_category(models.Item.query.filter_by(id=item_id).first().category)


@app.route('/api/value/add/<int:value_id>', methods=['GET'])
def add_categories_from_value(value_id):
    """Get category from item

    """
    val = models.Value.query.filter_by(id=value_id).first()
    return add_category_from_item(models.Variable.query.filter_by(value=val.id).first().item)
