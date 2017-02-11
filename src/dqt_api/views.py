from collections import defaultdict, Counter

import math
from itertools import zip_longest

import pandas as pd
from flask import request, jsonify
from flask.ext.cors import cross_origin
from sqlalchemy import inspect

from dqt_api import db, app, models


POPULATION_SIZE = 0


@app.before_first_request
def initialize(*args, **kwargs):
    """Initialize starting values."""
    global POPULATION_SIZE
    POPULATION_SIZE = db.session.query(models.DataModel).count()


@app.route('/', methods=['GET'])
def index():
    return 'Home page.'


@app.route('/api/search', methods=['GET'])
@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
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
            'description': c.description,
            'categoryId': c.id,
            'itemId': None,
        })
    # search item
    for i in models.Item.query.whooshee_search(target, order_by_relevance=-1):
        terms.append({
            'type': 'item',
            'id': i.id,
            'name': i.name,
            'description': i.description,
            'categoryId': i.category,
            'itemId': i.id,
        })

    # search value (need to get a category for this!)
    # for v in models.Value.query.whooshee_search(target, order_by_relevance=-1):
    #     terms.append({
    #         'type': 'value',
    #         'id': v.id,
    #         'name': v.name,
    #         'description': v.description
    #     })
    return jsonify({'search': terms})


def parse_arg_list(arg_list):
    cases = None
    no_results_flag = None
    for key, [val, *_] in arg_list:
        if '~' in val:
            val = val.split('~')
            cases_ = set(
                db.session.query(models.Variable.case).join(
                    models.Value
                ).filter(
                    models.Variable.item == key,
                    models.Value.name_numeric.between(int(val[0]), int(val[1]))
                ).all()
            )
        else:
            val = val.split('_')
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
    if not cases:
        if cases is None:
            no_results_flag = False  # there was no query/empty query
        else:
            no_results_flag = True  # this query has returned no results
        cases = db.session.query(models.Variable.case).all()
    cases = {x[0] for x in list(cases)}
    return cases, no_results_flag


def get_age_step(df):
    age_step = app.config.get('AGE_STEP')
    age_max = app.config.get('AGE_MAX')
    age_min = app.config.get('AGE_MIN')
    if age_step and age_max and age_min:
        return age_min, age_max, age_step  # return early

    # ages = df['age'].unique()
    ages = [x[0] for x in db.session.query(models.DataModel.age).all()]
    if not age_step:
        step = []
        prev_age = None
        for age in ages:
            if prev_age:
                step.append(age - prev_age)
            prev_age = age
        age_step = min(step)
    if not age_max:
        age_max = get_max_in_range(ages, age_step)
    if not age_min:
        age_min = get_min_in_range(ages, age_step)
    return age_min, age_max, age_step


def histogram(iterable, low, high, bins=None, step=None, group_extra_in_top_bin=False, mask=0):
    """Count elements from the iterable into evenly spaced bins

        >>> scores = [82, 85, 90, 91, 70, 87, 45]
        >>> histogram(scores, 0, 100, 10)
        [0, 0, 0, 0, 1, 0, 0, 1, 3, 2]

    """
    if not bins and not step:
        raise ValueError('Need to specify either bins or step.')
    if not step:
        step = (high - low + 0.0) / bins
    if not bins:
        bins = int(math.ceil((high - low + 0.0) / step))
    dist = Counter((float(x) - low) // step for x in iterable)
    res = [dist[b] for b in range(bins)]
    if group_extra_in_top_bin:
        res[-1] += sum(dist[x] for x in range(bins, int(max(dist)) + 1))
    return [r if r > mask else 0 for r in res]


@app.route('/api/filter/chart', methods=['GET'])
@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
def api_filter_chart():
    # get set of cases
    cases, no_results_flag = parse_arg_list(request.args.lists())

    # get data for graphs
    data = iterchain(
        (models.DataModel.query.filter(
            models.DataModel.case.in_(case_set)
        ).all() for case_set in chunker(cases, 2000)),
        depth=3
    )
    df = pd.DataFrame(query_to_dict(data))
    mask_value = app.config.get('MASK', 0)
    age_min, age_max, age_step = get_age_step(df)
    # get age counts for each sex
    sex_data = {'labels': list(range(age_min, age_max + age_step, age_step)),
                'datasets': []}
    for label, age_df in df[['sex', 'age']].groupby(['sex']):
        sex_data['datasets'].append({
            'label': label.capitalize(),
            'data': histogram(age_df['age'], age_min, age_max, step=age_step, group_extra_in_top_bin=True,
                              mask=mask_value)
        })

    enroll_data = {
        'labels': [],
        'datasets': [{'data': []}]
    }
    for label, cnt, *_ in df.groupby(['enrollment']).agg(['count']).itertuples():
        enroll_data['labels'].append(label)
        cnt = int(cnt)
        if cnt <= mask_value:
            cnt = 0
        enroll_data['datasets'][0]['data'].append(cnt)

    if len(df.index) > mask_value and not no_results_flag:
        selected_subjects = len(df.index)
        enrollment_before_baseline = round(df['enrollment_before_baseline'].mean(), 2)
        enrollment_to_followup = round(df['enrollment_to_followup'].mean(), 2)
        followup_years = round(df['followup_years'].mean(), 2)
    else:
        selected_subjects = 0
        enrollment_before_baseline = 0
        enrollment_to_followup = 0
        followup_years = 0

    # ensure that masking has been done on all following values
    return jsonify({
        'age': sex_data,
        'enrollment': enroll_data,
        'subject_counts': [
            {'header': 'Population', 'value': POPULATION_SIZE},
            {'header': 'Selected', 'value': selected_subjects},
            {'header': 'Enrollment before Baseline (mean years)', 'value': enrollment_before_baseline},
            {'header': 'Enrollment to Followup (mean years)', 'value': enrollment_to_followup},
            {'header': 'Follow-up (mean years)', 'value': followup_years},
        ]
    })


def query_to_dict(rset):
    """Stole this from: https://gist.github.com/garaud/bda10fa55df5723e27da
    """
    result = defaultdict(list)
    for obj in rset:
        instance = inspect(obj)
        for key, x in instance.attrs.items():
            result[key].append(x.value)
    return result


def iterchain(*args, depth=2):
    for element in args:
        for elements in element:
            for el in elements:
                yield el
    raise StopIteration


def iterchain2(*args, depth=2):
    for element in args:
        if depth > 1:
            iterchain(element, depth=depth - 1)
        else:
            yield element
    raise StopIteration


@app.route('/api/category/add/<int:category_id>', methods=['GET'])
@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
def add_category(category_id):
    """Get information about a particular category.

    """
    res = {'items': []}
    for item in models.Item.query.filter_by(category=category_id):
        variables = [x[0] for x in db.session.query(models.Variable.value).filter(models.Variable.item == item.id)]
        values = []
        ranges = []
        for v in db.session.query(models.Value).filter(models.Value.id.in_(variables)).order_by(models.Value.name):
            values.append(
                {'id': v.id,
                 'name': v.name,
                 'description': v.description
                 }
            )
            # determine if value could be part of range
            if ranges is not None:
                val = None
                try:
                    val = int(v.name)
                except ValueError:
                    pass
                try:
                    val = float(v.name)
                except ValueError:
                    pass
                if val is None:
                    ranges = None
                else:
                    ranges.append(val)

        # determine step
        if ranges:
            prev = None
            rsteps = []
            for el in sorted(ranges):
                if prev:
                    rsteps.append(el - prev)
                prev = el
            ranges = [min(ranges), max(ranges), min(rsteps)]

        # record data
        res['items'].append({
            'name': item.name,
            'id': item.id,
            'description': item.description,
            'values': None if ranges else values,
            'range': ranges
        })
    category = models.Category.query.filter_by(id=category_id).first()
    res['name'] = category.name
    res['description'] = category.description
    return jsonify(res)


def chunker(iterable, chunk_size, fillvalue=None):
    return zip_longest(*[iter(iterable)] * chunk_size, fillvalue=fillvalue)


@app.route('/api/category/all', methods=['GET'])
@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
def add_all_categories():
    """Get information about a particular category.

    """
    categories = []
    for category in models.Category.query.all():
        category_id = category.id
        res = {'items': []}
        for item in models.Item.query.filter_by(category=category_id):
            values = []
            ranges = []
            for v in db.session.query(models.Value).join(
                    models.Variable
            ).filter(
                        models.Variable.item == item.id
            ).order_by(
                models.Value.name
            ):
                values.append(
                    {'id': v.id,
                     'name': v.name,
                     'description': v.description
                     }
                )
                # determine if value could be part of range
                if ranges is not None:
                    val = None
                    try:
                        val = int(v.name)
                    except ValueError:
                        pass
                    try:
                        val = float(v.name)
                    except ValueError:
                        pass
                    if val is None:
                        ranges = None
                    else:
                        ranges.append(val)

            # determine step
            if ranges:
                prev = None
                rsteps = []
                for el in sorted(set(ranges)):
                    if prev:
                        rsteps.append(el - prev)
                    prev = el
                rstep = min(rsteps)
                try:
                    ranges = [get_min_in_range(ranges, rstep), get_max_in_range(ranges, rstep), rstep]
                except Exception as e:
                    print(e)
                    pass

            # record data
            res['items'].append({
                'name': item.name,
                'id': item.id,
                'description': item.description,
                'values': None if ranges else values,
                'range': ranges
            })
        category = models.Category.query.filter_by(id=category_id).first()
        res['id'] = category.id
        res['name'] = category.name
        res['description'] = category.description
        categories.append(res)
    return jsonify({'categories': categories})


def get_min_in_range(ranges, rstep):
    v = min(x for x in ranges if x is not None)
    rstep = rstep
    if v and v % rstep:
        v -= v % rstep
    return v


def get_max_in_range(ranges, rstep):
    v = max(x for x in ranges if x is not None)
    try:
        v % rstep
    except ZeroDivisionError:
        print(v)
        print(rstep)
    if v and v % rstep:
        v += rstep - (v % rstep)
    return v


@app.route('/api/item/add/<int:item_id>', methods=['GET'])
@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
def add_category_from_item(item_id):
    """Get category from item

    """
    return add_category(models.Item.query.filter_by(id=item_id).first().category)


@app.route('/api/value/add/<int:value_id>', methods=['GET'])
@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
def add_categories_from_value(value_id):
    """Get category from item

    """
    val = models.Value.query.filter_by(id=value_id).first()
    return add_category_from_item(models.Variable.query.filter_by(value=val.id).first().item)
