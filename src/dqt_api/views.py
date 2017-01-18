from collections import defaultdict

from flask import request, jsonify
from flask.ext.cors import cross_origin

from dqt_api import db, app, models


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
@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
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
    if not cases:
        cases = db.session.query(models.Variable.case).all()
    cases = [x[0] for x in list(cases)]

    # get data for graphs
    res = {}

    items = {
        models.Item.query.filter_by(name='Sex').first().id: 'sex',
        models.Item.query.filter_by(name='Current Status').first().id: 'current_status',
        # models.Item.query.filter_by(name='Age').first().id: 'age',
    }

    age_var = models.Item.query.filter_by(name='Age').first().id

    data = []
    curr = {}
    curr_inst = None
    ages = defaultdict(lambda: defaultdict(int))
    enrollment = defaultdict(int)
    for inst in db.session.query(models.Variable).filter(
            models.Variable.case.in_(cases),
            models.Variable.item.in_(items)
    ).order_by(models.Variable.case, models.Variable.item):
        val = db.session.query(models.Value.name).filter(models.Value.id == inst.value).first()[0]
        # build json
        if inst.case != curr_inst:
            if curr:
                data.append(curr)
                curr = {}
            curr_inst = inst.case
        curr[items[inst.item]] = val
        # build male/female - age json
        if val == 'male' or val == 'female':
            age_id = db.session.query(models.Variable).filter(
                models.Variable.case == inst.case,
                models.Variable.item == age_var
            ).first()
            age = db.session.query(models.Value.name).filter(models.Value.id == age_id.value).first()[0]
            ages[age][val] += 1
        # enrollment info
        if val in ['enrolled', 'disenrolled', 'unknown', 'died']:
            enrollment[val] += 1
    data.append(curr)  # fencepost

    data = []
    for age in ages:
        data.append({
            'age': age,
            'male': ages[age]['male'],
            'female': ages[age]['female'],
            'total': ages[age]['male'] + ages[age]['female']
        })

    res['enroll'] = []
    for enr in enrollment:
        res['enroll'].append(
            {
                'label': enr,
                'count': enrollment[enr]
            }
        )
    res['data'] = data
    res['count'] = len(data)
    res['columns'] = ['male', 'female']
    res['enroll-columns'] = ['enrolled', 'disenrolled', 'unknown', 'died']

    return jsonify(res)


@app.route('/api/category/add/<int:category_id>', methods=['GET'])
@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
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
        categories.append(res)
    return jsonify({'categories': categories})


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


@app.route('/api/test/age', methods=['GET'])
@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
def test_sex():
    return jsonify({
        'columns': ['male', 'female'],
        'data': [
            {
                'male': 20,
                'age': '50',
                'female': 25,
                'total': 45
            },
            {
                'male': 30,
                'age': '55',
                'female': 25,
                'total': 55
            }
        ]
    })


@app.route('/api/test/json', methods=['GET'])
@cross_origin(origin='localhost', headers=['Content-Type', 'Authorization'])
def test_json():
    return jsonify({
        'columns': ['State', 'Under 5 Years', '5 to 13 Years', '14 to 17 Years', '18 to 24 Years', '25 to 44 Years',
                    '45 to 64 Years', '65 Years and Over'],
        'data': [{'18 to 24 Years': 450818,
                  '45 to 64 Years': 1215966,
                  '14 to 17 Years': 259034,
                  'State': 'AL',
                  '5 to 13 Years': 552339,
                  '25 to 44 Years': 1231572,
                  'Under 5 Years': 310504,
                  'Total': 4661900,
                  '65 Years and Over': 641667},
                 {'18 to 24 Years': 74257,
                  '45 to 64 Years': 183159,
                  '14 to 17 Years': 42153,
                  'State': 'AK',
                  '5 to 13 Years': 85640,
                  '25 to 44 Years': 198724,
                  'Under 5 Years': 52083,
                  'Total': 686293,
                  '65 Years and Over': 50277},
                 {'18 to 24 Years': 601943,
                  '45 to 64 Years': 1523681,
                  '14 to 17 Years': 362642,
                  'State': 'AZ',
                  '5 to 13 Years': 828669,
                  '25 to 44 Years': 1804762,
                  'Under 5 Years': 515910,
                  'Total': 6500180,
                  '65 Years and Over': 862573},
                 {'18 to 24 Years': 264160,
                  '45 to 64 Years': 727124,
                  '14 to 17 Years': 157204,
                  'State': 'AR',
                  '5 to 13 Years': 343207,
                  '25 to 44 Years': 754420,
                  'Under 5 Years': 202070,
                  'Total': 2855390,
                  '65 Years and Over': 407205},
                 {'18 to 24 Years': 3853788,
                  '45 to 64 Years': 8819342,
                  '14 to 17 Years': 2159981,
                  'State': 'CA',
                  '5 to 13 Years': 4499890,
                  '25 to 44 Years': 10604510,
                  'Under 5 Years': 2704659,
                  'Total': 36756666,
                  '65 Years and Over': 4114496},
                 {'18 to 24 Years': 466194,
                  '45 to 64 Years': 1290094,
                  '14 to 17 Years': 261701,
                  'State': 'CO',
                  '5 to 13 Years': 587154,
                  '25 to 44 Years': 1464939,
                  'Under 5 Years': 358280,
                  'Total': 4939456,
                  '65 Years and Over': 511094},
                 {'18 to 24 Years': 325110,
                  '45 to 64 Years': 968967,
                  '14 to 17 Years': 196918,
                  'State': 'CT',
                  '5 to 13 Years': 403658,
                  '25 to 44 Years': 916955,
                  'Under 5 Years': 211637,
                  'Total': 3501252,
                  '65 Years and Over': 478007},
                 {'18 to 24 Years': 84464,
                  '45 to 64 Years': 230528,
                  '14 to 17 Years': 47414,
                  'State': 'DE',
                  '5 to 13 Years': 99496,
                  '25 to 44 Years': 230183,
                  'Under 5 Years': 59319,
                  'Total': 873092,
                  '65 Years and Over': 121688},
                 {'18 to 24 Years': 75569,
                  '45 to 64 Years': 140043,
                  '14 to 17 Years': 25225,
                  'State': 'DC',
                  '5 to 13 Years': 50439,
                  '25 to 44 Years': 193557,
                  'Under 5 Years': 36352,
                  'Total': 591833,
                  '65 Years and Over': 70648},
                 {'18 to 24 Years': 1607297,
                  '45 to 64 Years': 4746856,
                  '14 to 17 Years': 925060,
                  'State': 'FL',
                  '5 to 13 Years': 1938695,
                  '25 to 44 Years': 4782119,
                  'Under 5 Years': 1140516,
                  'Total': 18328340,
                  '65 Years and Over': 3187797},
                 {'18 to 24 Years': 919876,
                  '45 to 64 Years': 2389018,
                  '14 to 17 Years': 557860,
                  'State': 'GA',
                  '5 to 13 Years': 1250460,
                  '25 to 44 Years': 2846985,
                  'Under 5 Years': 740521,
                  'Total': 9685744,
                  '65 Years and Over': 981024},
                 {'18 to 24 Years': 124834,
                  '45 to 64 Years': 331817,
                  '14 to 17 Years': 64011,
                  'State': 'HI',
                  '5 to 13 Years': 134025,
                  '25 to 44 Years': 356237,
                  'Under 5 Years': 87207,
                  'Total': 1288198,
                  '65 Years and Over': 190067},
                 {'18 to 24 Years': 147606,
                  '45 to 64 Years': 375173,
                  '14 to 17 Years': 89702,
                  'State': 'ID',
                  '5 to 13 Years': 201192,
                  '25 to 44 Years': 406247,
                  'Under 5 Years': 121746,
                  'Total': 1523816,
                  '65 Years and Over': 182150},
                 {'18 to 24 Years': 1311479,
                  '45 to 64 Years': 3239173,
                  '14 to 17 Years': 725973,
                  'State': 'IL',
                  '5 to 13 Years': 1558919,
                  '25 to 44 Years': 3596343,
                  'Under 5 Years': 894368,
                  'Total': 12901563,
                  '65 Years and Over': 1575308},
                 {'18 to 24 Years': 605863,
                  '45 to 64 Years': 1647881,
                  '14 to 17 Years': 361393,
                  'State': 'IN',
                  '5 to 13 Years': 780199,
                  '25 to 44 Years': 1724528,
                  'Under 5 Years': 443089,
                  'Total': 6376792,
                  '65 Years and Over': 813839},
                 {'18 to 24 Years': 306398,
                  '45 to 64 Years': 788485,
                  '14 to 17 Years': 165883,
                  'State': 'IA',
                  '5 to 13 Years': 345409,
                  '25 to 44 Years': 750505,
                  'Under 5 Years': 201321,
                  'Total': 3002555,
                  '65 Years and Over': 444554},
                 {'18 to 24 Years': 293114,
                  '45 to 64 Years': 713663,
                  '14 to 17 Years': 155822,
                  'State': 'KS',
                  '5 to 13 Years': 342134,
                  '25 to 44 Years': 728166,
                  'Under 5 Years': 202529,
                  'Total': 2802134,
                  '65 Years and Over': 366706},
                 {'18 to 24 Years': 381394,
                  '45 to 64 Years': 1134283,
                  '14 to 17 Years': 229927,
                  'State': 'KY',
                  '5 to 13 Years': 493536,
                  '25 to 44 Years': 1179637,
                  'Under 5 Years': 284601,
                  'Total': 4269245,
                  '65 Years and Over': 565867},
                 {'18 to 24 Years': 471275,
                  '45 to 64 Years': 1128771,
                  '14 to 17 Years': 254916,
                  'State': 'LA',
                  '5 to 13 Years': 542341,
                  '25 to 44 Years': 1162463,
                  'Under 5 Years': 310716,
                  'Total': 4410796,
                  '65 Years and Over': 540314},
                 {'18 to 24 Years': 112682,
                  '45 to 64 Years': 397911,
                  '14 to 17 Years': 69752,
                  'State': 'ME',
                  '5 to 13 Years': 133656,
                  '25 to 44 Years': 331809,
                  'Under 5 Years': 71459,
                  'Total': 1316456,
                  '65 Years and Over': 199187},
                 {'18 to 24 Years': 543470,
                  '45 to 64 Years': 1513754,
                  '14 to 17 Years': 316873,
                  'State': 'MD',
                  '5 to 13 Years': 651923,
                  '25 to 44 Years': 1556225,
                  'Under 5 Years': 371787,
                  'Total': 5633597,
                  '65 Years and Over': 679565},
                 {'18 to 24 Years': 665879,
                  '45 to 64 Years': 1751508,
                  '14 to 17 Years': 341713,
                  'State': 'MA',
                  '5 to 13 Years': 701752,
                  '25 to 44 Years': 1782449,
                  'Under 5 Years': 383568,
                  'Total': 6497967,
                  '65 Years and Over': 871098},
                 {'18 to 24 Years': 974480,
                  '45 to 64 Years': 2706100,
                  '14 to 17 Years': 585169,
                  'State': 'MI',
                  '5 to 13 Years': 1179503,
                  '25 to 44 Years': 2628322,
                  'Under 5 Years': 625526,
                  'Total': 10003422,
                  '65 Years and Over': 1304322},
                 {'18 to 24 Years': 507289,
                  '45 to 64 Years': 1391878,
                  '14 to 17 Years': 289371,
                  'State': 'MN',
                  '5 to 13 Years': 606802,
                  '25 to 44 Years': 1416063,
                  'Under 5 Years': 358471,
                  'Total': 5220393,
                  '65 Years and Over': 650519},
                 {'18 to 24 Years': 305964,
                  '45 to 64 Years': 730133,
                  '14 to 17 Years': 174405,
                  'State': 'MS',
                  '5 to 13 Years': 371502,
                  '25 to 44 Years': 764203,
                  'Under 5 Years': 220813,
                  'Total': 2938618,
                  '65 Years and Over': 371598},
                 {'18 to 24 Years': 560463,
                  '45 to 64 Years': 1554812,
                  '14 to 17 Years': 331543,
                  'State': 'MO',
                  '5 to 13 Years': 690476,
                  '25 to 44 Years': 1569626,
                  'Under 5 Years': 399450,
                  'Total': 5911605,
                  '65 Years and Over': 805235},
                 {'18 to 24 Years': 95232,
                  '45 to 64 Years': 278241,
                  '14 to 17 Years': 53156,
                  'State': 'MT',
                  '5 to 13 Years': 106088,
                  '25 to 44 Years': 236297,
                  'Under 5 Years': 61114,
                  'Total': 967440,
                  '65 Years and Over': 137312},
                 {'18 to 24 Years': 186657,
                  '45 to 64 Years': 451756,
                  '14 to 17 Years': 99638,
                  'State': 'NE',
                  '5 to 13 Years': 215265,
                  '25 to 44 Years': 457177,
                  'Under 5 Years': 132092,
                  'Total': 1783432,
                  '65 Years and Over': 240847},
                 {'18 to 24 Years': 212379,
                  '45 to 64 Years': 653357,
                  '14 to 17 Years': 142976,
                  'State': 'NV',
                  '5 to 13 Years': 325650,
                  '25 to 44 Years': 769913,
                  'Under 5 Years': 199175,
                  'Total': 2600167,
                  '65 Years and Over': 296717},
                 {'18 to 24 Years': 119114,
                  '45 to 64 Years': 388250,
                  '14 to 17 Years': 73826,
                  'State': 'NH',
                  '5 to 13 Years': 144235,
                  '25 to 44 Years': 345109,
                  'Under 5 Years': 75297,
                  'Total': 1315809,
                  '65 Years and Over': 169978},
                 {'18 to 24 Years': 769321,
                  '45 to 64 Years': 2335168,
                  '14 to 17 Years': 478505,
                  'State': 'NJ',
                  '5 to 13 Years': 1011656,
                  '25 to 44 Years': 2379649,
                  'Under 5 Years': 557421,
                  'Total': 8682661,
                  '65 Years and Over': 1150941},
                 {'18 to 24 Years': 203097,
                  '45 to 64 Years': 501604,
                  '14 to 17 Years': 112801,
                  'State': 'NM',
                  '5 to 13 Years': 241326,
                  '25 to 44 Years': 517154,
                  'Under 5 Years': 148323,
                  'Total': 1984356,
                  '65 Years and Over': 260051},
                 {'18 to 24 Years': 1999120,
                  '45 to 64 Years': 5120254,
                  '14 to 17 Years': 1058031,
                  'State': 'NY',
                  '5 to 13 Years': 2141490,
                  '25 to 44 Years': 5355235,
                  'Under 5 Years': 1208495,
                  'Total': 19490297,
                  '65 Years and Over': 2607672},
                 {'18 to 24 Years': 883397,
                  '45 to 64 Years': 2380685,
                  '14 to 17 Years': 492964,
                  'State': 'NC',
                  '5 to 13 Years': 1097890,
                  '25 to 44 Years': 2575603,
                  'Under 5 Years': 652823,
                  'Total': 9222414,
                  '65 Years and Over': 1139052},
                 {'18 to 24 Years': 82629,
                  '45 to 64 Years': 166615,
                  '14 to 17 Years': 33794,
                  'State': 'ND',
                  '5 to 13 Years': 67358,
                  '25 to 44 Years': 154913,
                  'Under 5 Years': 41896,
                  'Total': 641481,
                  '65 Years and Over': 94276},
                 {'18 to 24 Years': 1081734,
                  '45 to 64 Years': 3083815,
                  '14 to 17 Years': 646135,
                  'State': 'OH',
                  '5 to 13 Years': 1340492,
                  '25 to 44 Years': 3019147,
                  'Under 5 Years': 743750,
                  'Total': 11485910,
                  '65 Years and Over': 1570837},
                 {'18 to 24 Years': 369916,
                  '45 to 64 Years': 918688,
                  '14 to 17 Years': 200562,
                  'State': 'OK',
                  '5 to 13 Years': 438926,
                  '25 to 44 Years': 957085,
                  'Under 5 Years': 266547,
                  'Total': 3642361,
                  '65 Years and Over': 490637},
                 {'18 to 24 Years': 338162,
                  '45 to 64 Years': 1036269,
                  '14 to 17 Years': 199925,
                  'State': 'OR',
                  '5 to 13 Years': 424167,
                  '25 to 44 Years': 1044056,
                  'Under 5 Years': 243483,
                  'Total': 3790060,
                  '65 Years and Over': 503998},
                 {'18 to 24 Years': 1203944,
                  '45 to 64 Years': 3414001,
                  '14 to 17 Years': 679201,
                  'State': 'PA',
                  '5 to 13 Years': 1345341,
                  '25 to 44 Years': 3157759,
                  'Under 5 Years': 737462,
                  'Total': 12448279,
                  '65 Years and Over': 1910571},
                 {'18 to 24 Years': 114502,
                  '45 to 64 Years': 282321,
                  '14 to 17 Years': 56198,
                  'State': 'RI',
                  '5 to 13 Years': 111408,
                  '25 to 44 Years': 277779,
                  'Under 5 Years': 60934,
                  'Total': 1050788,
                  '65 Years and Over': 147646},
                 {'18 to 24 Years': 438147,
                  '45 to 64 Years': 1186019,
                  '14 to 17 Years': 245400,
                  'State': 'SC',
                  '5 to 13 Years': 517803,
                  '25 to 44 Years': 1193112,
                  'Under 5 Years': 303024,
                  'Total': 4479800,
                  '65 Years and Over': 596295},
                 {'18 to 24 Years': 82869,
                  '45 to 64 Years': 210178,
                  '14 to 17 Years': 45305,
                  'State': 'SD',
                  '5 to 13 Years': 94438,
                  '25 to 44 Years': 196738,
                  'Under 5 Years': 58566,
                  'Total': 804194,
                  '65 Years and Over': 116100},
                 {'18 to 24 Years': 550612,
                  '45 to 64 Years': 1646623,
                  '14 to 17 Years': 336312,
                  'State': 'TN',
                  '5 to 13 Years': 725948,
                  '25 to 44 Years': 1719433,
                  'Under 5 Years': 416334,
                  'Total': 6214888,
                  '65 Years and Over': 819626},
                 {'18 to 24 Years': 2454721,
                  '45 to 64 Years': 5656528,
                  '14 to 17 Years': 1420518,
                  'State': 'TX',
                  '5 to 13 Years': 3277946,
                  '25 to 44 Years': 7017731,
                  'Under 5 Years': 2027307,
                  'Total': 24326974,
                  '65 Years and Over': 2472223},
                 {'18 to 24 Years': 329585,
                  '45 to 64 Years': 538978,
                  '14 to 17 Years': 167685,
                  'State': 'UT',
                  '5 to 13 Years': 413034,
                  '25 to 44 Years': 772024,
                  'Under 5 Years': 268916,
                  'Total': 2736424,
                  '65 Years and Over': 246202},
                 {'18 to 24 Years': 61679,
                  '45 to 64 Years': 188593,
                  '14 to 17 Years': 33757,
                  'State': 'VT',
                  '5 to 13 Years': 62538,
                  '25 to 44 Years': 155419,
                  'Under 5 Years': 32635,
                  'Total': 621270,
                  '65 Years and Over': 86649},
                 {'18 to 24 Years': 768475,
                  '45 to 64 Years': 2033550,
                  '14 to 17 Years': 413004,
                  'State': 'VA',
                  '5 to 13 Years': 887525,
                  '25 to 44 Years': 2203286,
                  'Under 5 Years': 522672,
                  'Total': 7769089,
                  '65 Years and Over': 940577},
                 {'18 to 24 Years': 610378,
                  '45 to 64 Years': 1762811,
                  '14 to 17 Years': 357782,
                  'State': 'WA',
                  '5 to 13 Years': 750274,
                  '25 to 44 Years': 1850983,
                  'Under 5 Years': 433119,
                  'Total': 6549224,
                  '65 Years and Over': 783877},
                 {'18 to 24 Years': 157989,
                  '45 to 64 Years': 514505,
                  '14 to 17 Years': 91074,
                  'State': 'WV',
                  '5 to 13 Years': 189649,
                  '25 to 44 Years': 470749,
                  'Under 5 Years': 105435,
                  'Total': 1814468,
                  '65 Years and Over': 285067},
                 {'18 to 24 Years': 553914,
                  '45 to 64 Years': 1522038,
                  '14 to 17 Years': 311849,
                  'State': 'WI',
                  '5 to 13 Years': 640286,
                  '25 to 44 Years': 1487457,
                  'Under 5 Years': 362277,
                  'Total': 5627967,
                  '65 Years and Over': 750146},
                 {'18 to 24 Years': 53980,
                  '45 to 64 Years': 147279,
                  '14 to 17 Years': 29314,
                  'State': 'WY',
                  '5 to 13 Years': 60890,
                  '25 to 44 Years': 137338,
                  'Under 5 Years': 38253,
                  'Total': 532668,
                  '65 Years and Over': 65614}]})
