import os
import random
import string
from collections import defaultdict, Counter
import logging

import math
from functools import lru_cache
from io import BytesIO
from itertools import zip_longest

import datetime

import copy

import pandas as pd
import sqlalchemy
from flask import request, jsonify, send_file
from loguru import logger
from sqlalchemy import inspect, text

from dqt_api import db, app, models


class LoguruHandler(logging.Handler):
    def emit(self, record):
        logger_opt = logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelno, record.getMessage())


def remove_values(f):
    new_subject_counts = copy.deepcopy(f[0])
    new_sex_data_bl = copy.deepcopy(f[1])
    new_sex_data_fu = copy.deepcopy(f[2])
    for i in range(len(new_subject_counts)):
        new_subject_counts[i]['value'] = 0
    for i in range(len(new_sex_data_bl['datasets'])):
        new_sex_data_bl['datasets'][i]['data'] = [0] * len(new_sex_data_bl['datasets'][i]['data'])
    for i in range(len(new_sex_data_fu['datasets'])):
        new_sex_data_fu['datasets'][i]['data'] = [0] * len(new_sex_data_fu['datasets'][i]['data'])
    new_sex_data_bl_g = get_google_chart(new_sex_data_bl)
    new_sex_data_fu_g = get_google_chart(new_sex_data_fu)
    return new_subject_counts, new_sex_data_bl, new_sex_data_fu, new_sex_data_bl_g, new_sex_data_fu_g


def jitter_value_by_date(value):
    return value + hash(datetime.date.today().strftime('%Y%m%dh')) % 6 - 2


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
    app.logger.info('Searching for: {}'.format(target))
    return jsonify(_search(target))


@lru_cache()
def _search(target):
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
    except sqlalchemy.exc.ProgrammingError as pe:
        app.logger.warning(f'Search {target} found no categories: {pe}')
        raise pe
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
    except sqlalchemy.exc.ProgrammingError as pe:
        app.logger.warning(f'Search {target} found no items: {pe}')
    return {'search': terms}


@lru_cache(maxsize=256)
def parse_arg_list(arg_list):
    cases = None
    no_results_flag = None
    for key, val in arg_list:
        if '~' in val:
            val = val.split('~')
            q = db.session.query(models.Variable.case).join(
                models.Value
            ).filter(
                models.Variable.item == key
            )
            if val[0] and val[1]:
                q = q.filter(models.Value.name_numeric.between(int(val[0]), int(val[1])))
            elif val[0]:
                q = q.filter(models.Value.name_numeric >= int(val[0]))
            elif val[1]:
                q = q.filter(models.Value.name_numeric <= int(val[1]))
            else:
                pass
            cases_ = set(q.all())
        else:
            val = val.split('_')
            cases_ = set(
                db.session.query(models.Variable.case).filter(
                    models.Variable.item == key,
                    models.Variable.value.in_(val)
                ).all()
            )
        if cases is None:
            cases = cases_
        else:
            cases &= cases_
    if not cases:
        if cases is None:
            no_results_flag = False  # there was no query/empty query
            cases = db.session.query(models.Variable.case).all()
        else:
            no_results_flag = True  # this query has returned no results
    cases = {x[0] for x in list(cases)} if cases else None
    return cases, no_results_flag


@lru_cache(maxsize=32)
def get_age_step():
    age_step = app.config.get('AGE_STEP')
    age_max = app.config.get('AGE_MAX')
    age_min = app.config.get('AGE_MIN')
    if age_step and age_max and age_min:
        return age_min, age_max, age_step  # return early
    else:
        ages = {x[0] for x in db.session.query(models.DataModel.age_bl).all()}
        return _get_age_step(age_step, age_min, age_max, ages)


def _get_age_step(age_step, age_min, age_max, ages):
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


def histogram(iterable, low, high, bins=None, step=None, group_extra_in_top_bin=False, mask=0,
              jitter_function=lambda x: x):
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
    masked = [jitter_function(r) if jitter_function(r) > mask else 0 for r in res]
    return masked


@app.route('/api/filter/export', methods=['GET'])
def api_filter_export():
    filters = []
    for key, [val, *_] in request.args.lists():
        item = db.session.query(models.Item.name).filter_by(id=key).first()[0]
        if '~' in val:
            low_val, high_val = val.split('~')
            if high_val and low_val:
                filters.append('({} >= {} AND {} <= {})'.format(item, low_val, item, high_val))
            elif high_val:
                filters.append('({} <= {})'.format(item, high_val))
            elif low_val:
                filters.append('({} >= {})'.format(item, low_val))
        else:
            subfilters = []
            for v in val.split('_'):
                subfilters.append(db.session.query(models.Value.name).filter_by(id=v).first()[0])
            if len(subfilters) > 1:
                filters.append('({} IN ({}))'.format(item, ', '.join(subfilters)))
            else:
                filters.append('({} = {})'.format(item, subfilters[0]))
    app.logger.info(f'Exporting parameters: {filters}')
    return jsonify({'filterstring': ' AND '.join(filters)})


def get_update_date_text():
    update_date = app.config.get('UPDATE_DATE', None)
    if update_date:
        return f'as of {update_date}'
    return ''


@app.route('/api/filter/chart', methods=['GET'])
def api_filter_chart(jitter=True):
    arg_list = tuple((key, val) for key, [val, *_] in request.args.lists())
    (subject_counts, _, _,
     sex_data_bl_g, sex_data_fu_g) = api_filter_chart_helper(jitter, arg_list)
    return jsonify({
        'subject_counts': subject_counts,
        'age_bl_g': sex_data_bl_g,
        'age_fu_g': sex_data_fu_g,
    })


@app.route('/api/dictionary/get', methods=['GET'])
def api_get_dictionary():
    lst = []
    prev_variable = None
    for de in models.DataEntry.query:
        if de.category != prev_variable:
            prev_variable = de.category
            lst.append({
                'id': de.id,
                'name': de.category,
                'data': []
            })
        lst[-1]['data'].append(
            {'label': de.label,
             'category': de.variable,
             'description': de.description,
             'values': de.values or ''
             }
        )
    return jsonify({
        'data_entries': lst
    })


@lru_cache(maxsize=256)
def api_filter_chart_helper(jitter=True, arg_list=None):
    def jitter_function(x):
        return jitter_value_by_date(x) if jitter else x

    # get set of cases
    cases, no_results_flag = parse_arg_list(arg_list or ())
    if no_results_flag and app.config.get('NULL_FILTER', None):
        return app.config['NULL_FILTER']
    if app.config.get('PRECOMPUTED_FILTER', None) and (no_results_flag is False or len(cases) >= app.config['POPULATION_SIZE']):
        return app.config['PRECOMPUTED_FILTER']

    # get data for graphs
    data = iterchain(
        (models.DataModel.query.filter(
            models.DataModel.case.in_(case_set)
        ).all() for case_set in chunker(cases, 2000)),
        depth=3
    )
    df = pd.DataFrame(query_to_dict(data))
    import random
    df.to_csv(f'data_{random.randint(0, 1000)}.csv', index=False)
    mask_value = app.config.get('MASK', 0)
    age_min, age_max, age_step = get_age_step()
    # get age counts for each sex
    age_buckets = ['{}-{}'.format(age, age + age_step - 1) for age in range(age_min, age_max - age_step, age_step)]
    age_buckets.append('{}+'.format(age_max - age_step))
    sex_counts_bl, sex_data_bl = get_sex_by_age('age_bl', age_buckets, age_max, age_min, age_step, df, jitter_function,
                                                mask_value)
    _, sex_data_fu = get_sex_by_age('age_fu', age_buckets, age_max, age_min, age_step, df, jitter_function,
                                    mask_value)

    enroll_data = []
    selected_subjects = 0
    for label, cnt, *_ in df.groupby(['enrollment']).agg(['count']).itertuples():
        cnt = jitter_function(int(cnt)) if jitter_function(int(cnt)) > mask_value else 0
        enroll_data.append({
            'header': '- {} {}'.format(label.capitalize(), get_update_date_text()),
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
                          'value': app.config['POPULATION_SIZE']},
                         {'header': 'Current Selection',
                          'value': min(selected_subjects, app.config['POPULATION_SIZE'])},
                     ] + enroll_data + sex_counts_bl + [
                         {'header': '{} Follow-up {} (mean years)'.format(app.config.get('COHORT_TITLE', ''),
                                                                          get_update_date_text()),
                          'value': followup_years}
                     ]

    # for google api
    sex_data_bl_g = get_google_chart(sex_data_bl)
    sex_data_fu_g = get_google_chart(sex_data_fu)
    return subject_counts, sex_data_bl, sex_data_fu, sex_data_bl_g, sex_data_fu_g


def get_google_chart(data):
    new_data = []
    new_labels = ['Age']
    for label in data['labels']:
        new_data.append([label])
    for dataset in data['datasets']:
        new_labels.append(dataset['label'])
        for i, data_point in enumerate(dataset['data']):
            new_data[i].append(data_point)
    new_data.insert(0, new_labels)
    return new_data


def get_sex_by_age(age_var, age_buckets, age_max, age_min, age_step, df, jitter_function, mask_value):
    sex_data = {'labels': age_buckets,  # show age range
                'datasets': []}
    sex_counts = []
    for label, age_df in df[['sex', age_var]].groupby(['sex']):
        label = label[0].capitalize() if isinstance(label, tuple) else label.capitalize()
        sex_data['datasets'].append({
            'label': label,
            'data': histogram(age_df[age_var], age_min, age_max, step=age_step, group_extra_in_top_bin=True,
                              mask=mask_value, jitter_function=jitter_function)
        })
        sex_counts.append({
            'header': '- {}'.format(label),
            'value': jitter_function(len(age_df)) if jitter_function(len(age_df)) > mask_value else 0
        })
    return sex_counts, sex_data


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


def iterchain2(*args, depth=2):
    for element in args:
        if depth > 1:
            iterchain(element, depth=depth - 1)
        else:
            yield element


@app.route('/api/category/add/<int:category_id>', methods=['GET'])
def add_category(category_id):
    """Get information about a particular category.

    """
    category = models.Category.query.filter_by(id=category_id).first()
    res = get_range_from_category(category)
    return jsonify(res)


def rounding(val, rounder, decimals=1, direction=1):
    """
    
    :param rounder: 
    :param decimals: 
    :param val: 
    :param direction: 1=round up; 0=round down 
    :return: 
    """
    res = round(val - (val % rounder), decimals)
    if direction == 1 and res != val:
        return res + rounder
    return res


def chunker(iterable, chunk_size, fillvalue=None):
    return zip_longest(*[iter(iterable)] * chunk_size, fillvalue=fillvalue)


def get_range_from_category(category: models.Category):
    return _get_range_from_category(category.id, category.name, category.description)


@lru_cache(maxsize=256)
def _get_range_from_category(category_id, category_name, category_description):
    res = {
        'items': [],
        'id': category_id,
        'name': category_name,
        'description': category_description
    }
    for item in models.Item.query.filter_by(category=category_id):
        variables = set([x[0] for x in db.session.query(models.Variable.value).filter(models.Variable.item == item.id)])
        values = []
        ranges = set()
        for vals in (db.session.query(models.Value).filter(models.Value.id.in_(var_set)).order_by(models.Value.order,
                                                                                                  models.Value.name)
                     for var_set in chunker(variables, 2000)):  # chunking for sql server max 2000 parameters
            for v in vals:
                values.append(
                    {'id': v.id,
                     'name': v.name,
                     'description': v.description,
                     'order': v.order if v.order is not None else 100
                     }
                )
                # determine if value could be part of range
                if ranges is not None:
                    val = None
                    try:
                        val = int(v.name)
                    except ValueError:
                        pass
                    if val is None:
                        try:
                            val = rounding(float(v.name), 0.1, 1, 0)
                        except ValueError:
                            pass
                    if val is None:
                        ranges = None
                    else:
                        ranges.add(val)
        values = sorted(values, key=lambda k: k['order'])
        # determine step
        if ranges:
            prev = None
            rsteps = []
            for el in sorted(ranges):
                if prev:
                    rsteps.append(el - prev)
                prev = el
            max_range = max(ranges)
            if int(max_range) != max_range:
                max_range += 0.1
            if len(ranges) == 1 or len(rsteps) == 0:  # all items have same value
                continue
            ranges = [min(ranges), max_range, min(rsteps)]
            # increase step count if larger range
            if ranges[2] == 1 and ranges[1] - ranges[0] > 20:
                ranges = [rounding(ranges[0], 5, 0, 0), rounding(ranges[1], 5, 0, 1), 5]
            elif math.isclose(ranges[2], 0.1):
                if ranges[1] - ranges[0] > 10:
                    ranges = [int(rounding(ranges[0], 5, 1, 0)), int(rounding(ranges[1], 5, 1, 1)), 1]
                else:
                    ranges = [int(rounding(ranges[0], 5, 1, 0)), int(rounding(ranges[1], 5, 1, 1)), 0.1]

            # sometimes the above has trouble
            # TODO: write an appropriate rounding library
            ideal_rate = app.config.get('IDEAL_BUCKET_COUNT', 20)
            current_rounding = ranges[2] * 2
            segments = (ranges[1] - ranges[0]) / ranges[2]
            if segments > ideal_rate:
                if '.' in str(ranges[2]):
                    if current_rounding > 1:
                        current_rounding = int(current_rounding)
                        rounded = int(
                            math.ceil((ranges[1] - ranges[0]) / ideal_rate / current_rounding) * current_rounding)
                    else:
                        zeroes = (str(ranges[2]).split('.')[1].count('0') + 1) * 10
                        rounded = int(math.ceil((ranges[1] * zeroes - ranges[0] * zeroes) / ideal_rate / (
                                current_rounding * zeroes)) * current_rounding * zeroes) / zeroes

                else:
                    rounded = int(math.ceil((ranges[1] - ranges[0]) / ideal_rate / current_rounding) * current_rounding)

                new_min = int(math.ceil((ranges[0]) / rounded)) * rounded
                if new_min > ranges[0]:
                    new_min = int(math.ceil((ranges[0] - rounded) / rounded)) * rounded
                new_max = int(math.ceil((ranges[1]) / rounded)) * rounded
                ranges = [new_min, new_max, rounded]

        # record data
        res['items'].append({
            'name': item.name,
            'id': item.id,
            'description': item.description,
            'values': None if ranges else values,
            'range': [transform_decimal(x) for x in ranges] if ranges else None
        })
    return res


def transform_decimal(num):
    res = str(num)
    if '.' in res:
        return res[:res.index('.') + 2]
    return res


@app.route('/api/category/all', methods=['GET'])
def add_all_categories():
    """Get information about a particular category.

    """
    return jsonify({'categories': get_all_categories()})


def get_all_categories():
    if app.config.get('PRECOMPUTED_COLUMN', None):
        return app.config['PRECOMPUTED_COLUMN']
    categories = []
    for category in db.session.query(models.Category).order_by(models.Category.order).all():
        cat = models.Category.query.filter_by(id=category.id).first()
        res = get_range_from_category(cat)
        categories.append(res)
    return categories


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
        pass
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
            if d.cookie:
                cookie = d.cookie
            else:
                cookie = create_cookie()
            db.session.add(models.UserData(ip_address=remote_addr, cookie=cookie))
            db.session.commit()
            return jsonify({'returnVisitor': True, 'cookie': cookie})
    return jsonify({'returnVisitor': False})


def get_random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


def create_cookie():
    return '{}{}'.format(
        get_random_string(10),
        datetime.datetime.today().strftime('%Y%m%d')
    )


@app.route('/api/user/submit', methods=['POST'])
def submit_user_form():
    """Collect user-submitted information about reason for visit.
    """
    affiliation = request.json.get('affiliation', '') or ''
    cookie = create_cookie()
    d = models.UserData(name=request.json['name'][:50],
                        email_address=request.json['emailAddress'][:100],
                        affiliation=affiliation[:50],
                        reason_for_visiting=request.json['reasonForVisiting'][:200],
                        ip_address=get_ip_address(),
                        cookie=cookie
                        )
    db.session.add(d)
    db.session.commit()
    return jsonify({
        'id': str(os.urandom(16)),  # this should be a token in SSL
        'validUser': True,
        'cookie': cookie
    })


@app.route('/api/user/cookie', methods=['POST'])
def submit_user_cookie():
    cookie = request.json.get('cookie', None)
    if not cookie:  # no cookie
        app.logger.error('Failed to login cookie: "{}"'.format(cookie))
        return jsonify({
            'messages': {
                'error': ['Auto-login Failed: Unknown']
            },
            'status': False
        })
    # check if cookie not yet used (invalid cookie)
    if not models.UserData.query.filter_by(cookie=cookie).first():
        app.logger.error('Invalid cookie: "{}"'.format(cookie))
        return jsonify({
            'messages': {
                'error': ['Auto-login Failed: Unrecognized']
            },
            'status': False
        })
    db.session.add(models.UserData(cookie=cookie))
    db.session.commit()
    return jsonify({
        'messages': {
            'info': ['Login Successful']
        },
        'status': True
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
    c_header = None
    for tab in db.session.query(models.TabData).order_by(
            models.TabData.order, models.TabData.header, models.TabData.line
    ):
        if tab.header != c_header:
            c_header = tab.header
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


@app.route('/api/data/dictionary/get', methods=['GET'])
def get_data_dictionary():
    """Get excel file"""
    df = models.DataFile.query.order_by(text('-id')).first()
    return send_file(BytesIO(df.file),
                     mimetype='application/vnd.ms-excel',
                     attachment_filename=df.filename,
                     as_attachment=True)


@app.route('/api/data/dictionary/meta', methods=['GET'])
def get_data_dictionary_meta():
    """Get checksums"""
    df = models.DataFile.query.order_by(text('-id')).first()
    return jsonify({
        'checksums': [{
            'type': 'md5',
            'value': 'Unavailable' if df is None else df.md5_checksum,
        }]
    })
