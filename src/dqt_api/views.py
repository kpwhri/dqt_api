import hashlib
import os
import random
import string
from collections import defaultdict
import logging

from functools import lru_cache
from io import BytesIO
from itertools import zip_longest

import datetime

import copy

import sqlalchemy
from flask import request, jsonify, send_file
from loguru import logger
from sqlalchemy import inspect, text

from dqt_api import db, app, models
from dqt_api.pl_utils import load_cases_to_polars, censored_histogram_by_age_pl2


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


def jitter_and_mask_value_by_date(value, mask=0, label=''):
    """Add/subtract small increment from value. If resulting `new_value` <= mask, set the result to 0."""
    salt = app.config.get('JITTER', 'DEFAULT')
    noise_min = app.config.get('JITTER_MIN', -2)
    noise_max = app.config.get('JITTER_MAX', 2)
    year, week, _ = datetime.date.today().isocalendar()
    seed_str = f'{year}-W{week}_{label}_{salt}'
    incr = hash(seed_str) % (noise_max - noise_min + 1) + noise_min
    # incr = (int(hashlib.sha256(seed_str.encode()).hexdigest(), 16) % (noise_max - noise_min + 1)) + noise_min
    new_value = incr + value
    return masker(new_value, mask)


def masker(value, mask=0):
    return value if value > mask else 0


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
    app.logger.info(f'Searching for: {target}')
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
                q = q.filter(models.Value.name_numeric.between(float(val[0]), float(val[1])))
            elif val[0]:
                q = q.filter(models.Value.name_numeric >= float(val[0]))
            elif val[1]:
                q = q.filter(models.Value.name_numeric <= float(val[1]))
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


@lru_cache(maxsize=32)
def keep_enrollment(label):
    """
    Limit enrollment to only those labels present in `ENROLLMENT_RETAIN` config value.
    """
    if retain_labels := app.config.get('ENROLLMENT_RETAIN'):
        for retain_label in retain_labels:
            if label.lower() == retain_label.lower():
                return True
        return False
    else:
        return True


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


@app.route('/api/filter/export', methods=['GET'])
def api_filter_export():
    filters = []
    for key, [val, *_] in request.args.lists():
        item = db.session.query(models.Item.varname).filter_by(id=key).first()[0]
        if '~' in val:
            low_val, high_val = val.split('~')
            if high_val and low_val:
                filters.append(f'({item} >= {low_val} AND {item} <= {high_val})')
            elif high_val:
                filters.append(f'({item} <= {high_val})')
            elif low_val:
                filters.append(f'({item} >= {low_val})')
        else:
            subfilters = []
            for v in val.split('_'):
                subfilters.append(db.session.query(models.Value.name).filter_by(id=v).first()[0])
            if len(subfilters) > 1:
                filters.append(f'({item} IN ({", ".join(subfilters)}))')
            else:
                filters.append(f'({item} = {subfilters[0]})')
    app.logger.info(f'Exporting parameters: {filters}')
    return jsonify({'filterstring': ' AND '.join(filters)})


def get_update_date_text():
    update_date = app.config.get('UPDATE_DATE', None)
    if update_date:
        return f'through {update_date}'
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
    """
    param: jitter: this is only set to False during pre-computing of default/starting filter
    """

    def jitter_and_mask_function(x, mask=0, label=''):
        """
        Apply jitter function unless:
        * function is called with jitter=False (when precomputing)
        * or, if config contains JITTER=None
        """
        return (masker(x, mask) if jitter is False or not app.config.get('JITTER', True)
                else jitter_and_mask_value_by_date(x, mask, label))

    # get set of cases
    cases, no_results_flag = parse_arg_list(arg_list or ())
    if no_results_flag and app.config.get('NULL_FILTER', None):
        return app.config['NULL_FILTER']
    if app.config.get('PRECOMPUTED_FILTER', None) and (
            no_results_flag is False or len(cases) >= app.config['POPULATION_SIZE']):
        return app.config['PRECOMPUTED_FILTER']

    # get data for graphs
    df = load_cases_to_polars(cases)
    mask_value = app.config.get('MASK', 0)
    age_min, age_max, age_step = get_age_step()
    # get age counts for each sex
    age_buckets = [f'{age}-{age + age_step - 1}' for age in range(age_min, age_max - age_step, age_step)]
    age_buckets.append(f'{age_max - age_step}+')
    sex_counts_bl, sex_data_bl, excl_case_bl = get_sex_by_age('age_bl', age_buckets, age_max, age_min, age_step, df,
                                                jitter_and_mask_function, mask_value)
    sex_counts_fu, sex_data_fu, excl_case_fu = get_sex_by_age('age_fu', age_buckets, age_max, age_min, age_step, df,
                                    jitter_and_mask_function, mask_value)

    enroll_data = []
    # censor same cases for enrollment based on whichever age has fewer excluded cases
    # select subject count based on baseline ages, and ensure same values are censored
    selected_subjects_bl = sum(sum(x['data']) for x in sex_data_bl['datasets'])
    selected_subjects_fu = sum(sum(x['data']) for x in sex_data_fu['datasets'])
    if selected_subjects_bl > selected_subjects_fu:  # more baseline cases (i.e., more fu cases excluded)
        age_var = 'age_bl'
        df = df.filter(~df['case'].is_in(excl_case_bl))
        selected_subjects = selected_subjects_bl
        sex_counts = sex_counts_bl
    else:   # more fu cases (i.e., more bl cases excluded)
        age_var = 'age_fu'
        df = df.filter(~df['case'].is_in(excl_case_fu))
        selected_subjects = selected_subjects_fu
        sex_counts = sex_counts_fu

    for label, censored_hist_data, _ in censored_histogram_by_age_pl2(
            'enrollment', age_var, age_max, age_min, age_step, df,
    ):
        if not keep_enrollment(label):
            continue
        value = sum(censored_hist_data)
        value = jitter_and_mask_function(value, mask_value, label)
        # prepare the table row for display
        enroll_data.append({
            'id': f'enroll-{label}-count'.lower(),
            'header': f'- {label} {get_update_date_text()}',
            'value': value,
        })

    if selected_subjects > mask_value and not no_results_flag:
        followup_years = round(df['followup_years'].mean(), 2)
    else:
        selected_subjects = 0
        followup_years = 0

    # table rows ('cohort subjects' table)
    subject_counts = [
                         {'id': f'total-count',
                          'header': f'Total {app.config.get("COHORT_TITLE", "")} Population {get_update_date_text()}'.strip(),
                          'value': app.config['POPULATION_SIZE']},
                         {'id': f'selected-count',
                          'header': 'Current Selection',
                          'value': min(selected_subjects, app.config['POPULATION_SIZE'])},
                     ] + enroll_data + sex_counts + [
                         {'id': f'followup-years',
                          'header': f'{app.config.get("COHORT_TITLE", "")} Follow-up {get_update_date_text()} (mean years)'.strip(),
                          'value': followup_years}
                     ]

    # for google api: these are the charts
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
    excluded_cases = []
    for label, censored_hist_data, new_excluded_cases in censored_histogram_by_age_pl2(
            'sex', age_var, age_max, age_min, age_step, df, jitter_function, mask_value,
    ):
        excluded_cases += new_excluded_cases
        sex_data['datasets'].append({
            'label': label,
            'data': censored_hist_data,
        })
        sex_counts.append({
            'id': f'sex-{label}-count'.lower(),
            'header': f'- {label}',
            'value': sum(censored_hist_data),  # already jittered and masked
        })
    return sex_counts, sex_data, excluded_cases


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
        if item.is_loaded:  # new way: precalculated values
            values = None
            item_range = None
            if item.values:
                value_ints = {int(x) for x in item.values.split('||')}
                value_models = db.session.query(models.Value).filter(
                    models.Value.id.in_(value_ints)
                ).order_by(models.Value.order, models.Value.name)
                values = sorted([
                    {'id': v.id,
                     'name': v.name,
                     'description': v.description,
                     'order': v.order if v.order is not None else 100
                     } for v in value_models
                ], key=lambda k: k['order'])
            elif item.is_float:
                item_range = [str(item.float_range_start), str(item.float_range_end), str(item.float_range_step)]
            else:
                item_range = [str(item.int_range_start), str(item.int_range_end), str(item.int_range_step)]
            res['items'].append({
                'name': item.name,
                'id': item.id,
                'description': item.description,
                'values': values,
                'range': item_range,
            })
        else:  # old way -- may be needed if not calculated
            variables = set(
                [x[0] for x in db.session.query(models.Variable.value).filter(models.Variable.item == item.id)])
            values = []
            ranges = set()
            for vals in (
                    db.session.query(models.Value).filter(models.Value.id.in_(var_set)).order_by(models.Value.order,
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
            values = sorted(values, key=lambda k: k['order'])

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
    return f'{get_random_string(10)}{datetime.datetime.today().strftime("%Y%m%d")}'


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
        app.logger.error(f'Failed to login cookie: "{cookie}"')
        return jsonify({
            'messages': {
                'error': ['Auto-login Failed: Unknown']
            },
            'status': False
        })
    # check if cookie not yet used (invalid cookie)
    if not models.UserData.query.filter_by(cookie=cookie).first():
        app.logger.error(f'Invalid cookie: "{cookie}"')
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
    """Get excel file as a download"""
    df = models.DataFile.query.order_by(text('-id')).first()
    return send_file(BytesIO(df.file),
                     mimetype='application/vnd.ms-excel',
                     download_name=df.filename,
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
