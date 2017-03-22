import argparse
import random

import sys
from collections import defaultdict

from flask_alembic import Alembic
from flask_script import Manager
from flask_migrate import Migrate
from flask_alembic.cli.script import manager as alembic_manager
from dqt_api import db, app, whooshee
from dqt_api import models
from dqt_api.__main__ import prepare_config


def main():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--config', required=True,
                        help='File containing configuration information. '
                             'BASE_DIR, SECRET_KEY.')
    parser.add_argument('--method', choices=('manage', 'create', 'load', 'delete', 'overload', 'reindex'),
                        default='manage',
                        help='Operation to perform.')
    parser.add_argument('--count', nargs='*', type=int,
                        help='When loading, specifying number of samples to be generated.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Run in debug mode.')
    args, unk = parser.parse_known_args()

    app.config.from_pyfile(args.config)
    prepare_config(args.debug)
    sys.argv = sys.argv[:1] + unk  # only use commands that have not yet been used

    if args.method == 'manage':
        manage()
    elif args.method == 'create':
        create()
    elif args.method == 'load':
        load(args.count[0])
    elif args.method == 'delete':
        delete()
    elif args.method == 'overload':
        overload(*args.count)
    elif args.method == 'reindex':
        reindex()


def reindex():
    """Reindex whooshee data"""
    whooshee.reindex()


def manage():
    """
    Run 'migrate' followed by 'upgrade'
    :return:
    """
    migrate = Migrate(app, db)
    manager = Manager(app)
    alembic = Alembic(app)

    manager.add_command('db', alembic_manager)
    manager.run()


def create():
    db.create_all()
    alembic = Alembic()
    alembic.init_app(app)


def _random_name(mn=2, mx=8, add_possibles=''):
    return ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ' + add_possibles) for _ in
                    range(random.randrange(mn, mx))]).capitalize()


def overload(category_count, item_count, subject_count=None):
    """Overload categories or items to help better optimize."""
    items = {}
    for _ in range(category_count):
        c = models.Category(name=_random_name())
        db.session.add(c)
        db.session.commit()
        for _ in range(item_count):
            i = models.Item(name=_random_name(),
                            description=_random_name(10, 30, add_possibles=' '),
                            category=c.id)
            db.session.add(i)
            db.session.commit()
            values = []
            value_type = random.choice(['float', 'int', 'categorical'])
            if value_type == 'float':
                for _ in range(20):
                    v = models.Value(name=str(random.random()))
                    db.session.add(v)
                    values.append(v.id)
            elif value_type == 'int':
                interval = random.randint(1, 11)
                max_value = random.randint(5, 11) * interval
                for _ in range(20):
                    v = models.Value(name=str(random.randrange(0, max_value, interval)))
                    db.session.add(v)
                    values.append(v.id)
            else:
                for _ in range(random.randint(2, 8)):
                    v = models.Value(name=_random_name(mn=4, mx=10))
                    db.session.add(v)
                    values.append(v.id)
            db.session.commit()
            items[i.id] = values
    db.session.commit()

    if subject_count:  # add subjects if not loaded yet
        load(subject_count)

    # update subjects
    for subject_id in db.session.query(models.DataModel.case).all():
        subject_id = subject_id[0]
        for item in items:
            v = models.Variable(case=subject_id, item=item, value=random.choice(items[item]))
            db.session.add(v)
        db.session.commit()
        print('Completed subject: {}'.format(subject_id))


def add_tabs():
    db.session.add(models.TabData(header='FAQ', line=0, text_type='header',
                                  text='What is the data query tool?'))
    db.session.add(models.TabData(header='FAQ', line=1, text_type='text',
                                  text='A really awesome tool'))
    db.session.add(models.TabData(header='Home', line=0, text_type='bold',
                                  text='This is the home page.'))
    db.session.add(models.TabData(header='Contact', line=0, text_type='text',
                                  text='Email me if you have questions.'))
    db.session.commit()


def add_comments():
    db.session.add(models.Comment(location='table', line=0,
                                  comment='(bl) implies baseline values, from the baseline ACT '
                                          'study visit.'))
    db.session.add(models.Comment(location='table', line=1,
                                  comment='(fu) implies baseline values, from a participant\'s most '
                                          'most recent ACT study visit.'))
    db.session.commit()


def load(count):
    """Load database with sample data."""

    def load_all(*lst, commit=False):
        for el in lst:
            db.session.add(el)
        if commit:
            db.session.commit()

    add_tabs()
    add_comments()

    c1 = models.Category(name='Demographics',
                         description='Statistical data relating to a population and its subgroups.')
    c2 = models.Category(name='CASI',
                         description='The cognitive abilities screening instrument (CASI) for testing cognitive '
                                     'function.')
    c3 = models.Category(name='Dementia/AD',
                         description='Dementia and Alzheimer\'s-related variables.')
    c4 = models.Category(name='ACT Enrollment',
                         description='Variables relating to enrollment in ACT data.')
    load_all(c1, c2, c3, c4, commit=True)

    i11 = models.Item(name='Age',
                      description='Filter by age range.',
                      category=c1.id,
                      is_numeric=True)
    i12 = models.Item(name='Sex',
                      description='Filter by sex.',
                      category=c1.id)
    i13 = models.Item(name='Race',
                      description='Filter by race.',
                      category=c1.id)
    i14 = models.Item(name='Hispanic',
                      description='Filter by ethnicity. 1=Hispanic',
                      category=c1.id)
    i21 = models.Item(name='CASI IRT Score (Baseline)',
                      description='Baseline cognitive abilities screening instrument (CASI) score.',
                      category=c2.id,
                      is_numeric=True)
    i22 = models.Item(name='CASI IRT Score (Follow-up)',
                      description='Most recent follow-up cognitive abilities screening instrument (CASI) score.',
                      category=c2.id,
                      is_numeric=True)
    i31 = models.Item(name='Any AD',
                      description='Any alzheimer\'s disease.',
                      category=c3.id)
    i32 = models.Item(name='Any Dementia',
                      description='Any dementia',
                      category=c3.id)
    i41 = models.Item(name='Current Status',
                      description='Current enrollment status in ACT.',
                      category=c4.id)
    i42 = models.Item(name='Enrollment before Baseline',
                      description='Filter by enrollment years.',
                      category=c4.id,
                      is_numeric=True)
    i43 = models.Item(name='Enrollment to Last Followup',
                      description='Filter by enrollment years.',
                      category=c4.id,
                      is_numeric=True)
    i44 = models.Item(name='Total followup',
                      description='Filter by years in cohort.',
                      category=c4.id,
                      is_numeric=True)
    i45 = models.Item(name='Intake Date',
                      description='Date when added to cohort.',
                      category=c4.id,
                      is_numeric=True)
    cis = [i11, i12, i13, i14, i21, i22, i31, i32, i41, i42, i43, i44, i45]

    mf = [models.Value(name='male'), models.Value(name='female')]
    v3 = models.Value(name='white')
    v4 = models.Value(name='black')
    v5a = models.Value(name='asian')
    v5b = models.Value(name='hp/pi')
    v6 = models.Value(name='other')
    race = [v3, v4, v5a, v5b, v6]
    ages = [models.Value(name=str(x)) for x in range(65, 102)]
    casi = [models.Value(name=str(x)) for x in range(0, 101)]
    yn = [models.Value(name='yes'), models.Value(name='no')]
    status = [models.Value(name='alive'), models.Value(name='dead')]
    intake_dates = [models.Value(name=str(x)) for x in range(1980, 2016)]

    load_all(*cis + mf + race + ages + yn + casi + status, commit=True)
    # load subjects with random data
    graph_data = defaultdict(defaultdict)  # separate summary data table
    for i in range(count):
        for item, vals, label in [(i11, ages, 'age'), (i12, mf, 'sex'), (i13, race, None), (i14, yn, None),
                                  (i21, casi, None), (i22, casi, None), (i31, yn, None), (i32, yn, None),
                                  (i41, status, 'enrollment'), (None, None, 'enrollment-years'),
                                  (i45, intake_dates, 'intake_date')]:
            if vals:
                sel = random.choice(vals)
                db.session.add(models.Variable(case=i, item=item.id, value=sel.id))
                if label:
                    graph_data[i][label] = sel.name
            else:  # special case for related enrollment years data
                enroll_before_baseline = models.Value(name=str(random.choice(range(0, 30))))
                enroll_to_followup = models.Value(name=str(random.choice(range(0, 30))))
                followup = models.Value(name=str(random.choice(range(2, int(enroll_to_followup.name) + 3))))
                db.session.add(enroll_before_baseline)
                db.session.add(enroll_to_followup)
                db.session.add(followup)
                db.session.add(models.Variable(case=i, item=i42.id, value=enroll_before_baseline.id))
                db.session.add(models.Variable(case=i, item=i43.id, value=enroll_to_followup.id))
                db.session.add(models.Variable(case=i, item=i44.id, value=followup.id))
                graph_data[i]['followup_years'] = followup.name

    db.session.commit()
    for case in graph_data:
        db.session.add(models.DataModel(case=case, **graph_data[case]))
    db.session.commit()


def delete():
    for m in [models.Variable, models.DataModel, models.Item, models.Category, models.Value,
              models.UserData, models.TabData, models.Comment]:
        db.session.query(m).delete()
    db.session.commit()


if __name__ == '__main__':
    main()
