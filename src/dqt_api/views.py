import os
from collections import defaultdict, Counter

import math
from itertools import zip_longest

import pandas as pd
import sqlalchemy
from flask import request, jsonify
from flask_cors import cross_origin
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
    try:
        for c in models.Category.query.whooshee_search(target, order_by_relevance=-1):
            terms.append({
                'type': 'category',
                'id': c.id,
                'name': c.name,
                'description': c.description,
                'categoryId': c.id,
                'itemId': None,
            })
    except sqlalchemy.exc.ProgrammingError:
        print('No categories')
        pass
    # search item
    try:
        for i in models.Item.query.whooshee_search(target, order_by_relevance=-1):
            terms.append({
                'type': 'item',
                'id': i.id,
                'name': i.name,
                'description': i.description,
                'categoryId': i.category,
                'itemId': i.id,
            })
    except sqlalchemy.exc.ProgrammingError:
        print('No items')
        pass
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
    masked = [r if r > mask else 0 for r in res]
    return masked


@app.route('/api/filter/export', methods=['GET'])
def api_filter_export():
    filters = []
    for key, [val, *_] in request.args.lists():
        item = db.session.query(models.Item.name).filter_by(id=key).first()[0]
        if '~' in val:
            low_val, high_val = val.split('~')
            filters.append('({} >= {} AND {} <= {})'.format(item, low_val, item, high_val))
        else:
            subfilters = []
            for v in val.split('_'):
                subfilters.append(db.session.query(models.Value.name).filter_by(id=v).first()[0])
                print(v, subfilters)
            if len(subfilters) > 1:
                filters.append('({} IN ({}))'.format(item, ', '.join(subfilters)))
            else:
                filters.append('({} = {})'.format(item, subfilters[0]))
    return jsonify({'filterstring': ' AND '.join(filters)})


def get_update_date_text():
    update_date = app.config.get('UPDATE_DATE', None)
    if update_date:
        return 'as of {}'.format(update_date)
    return ''


@app.route('/api/filter/chart', methods=['GET'])
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
    age_buckets = ['{}-{}'.format(age, age + age_step - 1) for age in range(age_min, age_max - age_step, age_step)]
    age_buckets.append('{}+'.format(age_max - age_step))
    sex_data = {'labels': age_buckets,  # show age range
                'datasets': []}
    for label, age_df in df[['sex', 'age']].groupby(['sex']):
        sex_data['datasets'].append({
            'label': label.capitalize(),
            'data': histogram(age_df['age'], age_min, age_max, step=age_step, group_extra_in_top_bin=True,
                              mask=mask_value)
        })

    enroll_data = []
    selected_subjects = 0
    for label, cnt, *_ in df.groupby(['enrollment']).agg(['count']).itertuples():
        cnt = int(cnt) if int(cnt) > mask_value else 0
        enroll_data.append({
            'header': '{} {}'.format(label.capitalize(), get_update_date_text()),
            'value': cnt
        })
        selected_subjects += cnt

    if selected_subjects > mask_value and not no_results_flag:
        followup_years = round(df['followup_years'].mean(), 2)
    else:
        selected_subjects = 0
        followup_years = 0

    subject_counts = [
                         {'header': 'Total {} Population {}'.format(app.config.get('COHORT_TITLE', ''),
                                                                    get_update_date_text()),
                          'value': POPULATION_SIZE},
                         {'header': 'Current Selection',
                          'value': selected_subjects},
                         {'header': '{} Follow-up {} (mean years)'.format(app.config.get('COHORT_TITLE', ''),
                                                                          get_update_date_text()),
                          'value': followup_years},
                     ] + enroll_data
    return jsonify({
        'age': sex_data,
        'subject_counts': subject_counts
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
def add_all_categories():
    """Get information about a particular category.

    """
    categories = []
    for category in db.session.query(models.Category).order_by(models.Category.order).all():
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
                models.Value.order,
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
                try:
                    rstep = min(rsteps)  # skip if not items
                except ValueError as e:
                    print(e, category.name, item.name)
                    continue
                try:
                    ranges = [get_min_in_range(ranges, rstep), get_max_in_range(ranges, rstep), rstep]
                except Exception as e:
                    print(e, category.name, item.name, rsteps)
                    continue

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


@app.route('/api/user/check', methods=['GET'])
def check_user_ip():
    remote_addr = get_ip_address()
    if remote_addr and remote_addr != 'untrackable':
        d = models.UserData.query.filter_by(ip_address=remote_addr).first()
        if d:
            db.session.add(models.UserData(ip_address=remote_addr))
            db.session.commit()
            return jsonify({'returnVisitor': True})
    return jsonify({'returnVisitor': False})


@app.route('/api/user/submit', methods=['POST'])
def submit_user_form():
    """Collect user-submitted information about reason for visit.
    """
    d = models.UserData(name=request.json['name'],
                        email_address=request.json['emailAddress'],
                        affiliation=request.json['affiliation'],
                        reason_for_visiting=request.json['reasonForVisiting'],
                        ip_address=get_ip_address()
                        )
    db.session.add(d)
    db.session.commit()
    return jsonify({
        'id': str(os.urandom(16)),  # this should be a token in SSL
        'validUser': True
    })


def get_ip_address():
    """Code borrowed from flask_security"""
    if 'X-Forwarded-For' in request.headers:
        remote_addr = request.headers.getlist("X-Forwarded-For")[0].rpartition(' ')[-1]
    else:
        remote_addr = request.remote_addr or 'untrackable'
    return str(remote_addr)


@app.route('/api/tabs', methods=['GET'])
def get_tabs():
    """Get headers and content for each page"""
    res = []
    curr = None
    for tab in db.session.query(models.TabData).order_by(
            models.TabData.order, models.TabData.header, models.TabData.line
    ):
        if tab.line == 0:
            if curr:
                res.append(curr)
            curr = {
                'header': tab.header,
                'lines': [{'type': tab.text_type, 'text': tab.text}]
            }
        else:
            curr['lines'].append({'type': tab.text_type, 'text': tab.text})
    res.append(curr)  # fencepost
    return jsonify({'tabs': res})


@app.route('/api/comments/<string:component>', methods=['GET'])
def get_comments(component):
    """Get data concerning comments on main page"""
    comments = []
    for c in db.session.query(
        models.Comment
    ).filter(
        models.Comment.location == component
    ).order_by(
        models.Comment.location,
        models.Comment.line
    ):
        comments.append(c.comment)
    return jsonify({
        'comments': comments,
        'mask': app.config['MASK'],
        'cohortTitle': app.config.get('COHORT_TITLE', '')
    })
