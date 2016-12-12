import argparse
import random

import sys
from flask_alembic import Alembic
from flask_script import Manager
from flask_migrate import Migrate
from flask_alembic.cli.script import manager as alembic_manager
from dqt_api import db, app
from dqt_api import models
from dqt_api.__main__ import prepare_config


def main():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--config', required=True,
                        help='File containing configuration information. '
                             'BASE_DIR, SECRET_KEY.')
    parser.add_argument('--method', choices=('manage', 'create', 'load', 'delete'), default='manage',
                        help='Operation to perform.')
    parser.add_argument('--count', default=None, type=int,
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
        load(args.count)
    elif args.method == 'delete':
        delete()


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


def load(count):
    """Load database with sample data."""

    def load_all(*lst, commit=False):
        for el in lst:
            db.session.add(el)
        if commit:
            db.session.commit()

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
    cis = [i11, i12, i13, i14, i21, i22, i31, i32, i41]

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
    status = [models.Value(name='enrolled'), models.Value(name='disenrolled'),
              models.Value(name='unknown'), models.Value(name='died')]

    load_all(*cis + mf + race + ages + yn + casi + status, commit=True)
    # load subjects with random data
    for i in range(count):
        for item, vals in [(i11, ages), (i12, mf), (i13, race), (i14, yn),
                           (i21, casi), (i22, casi), (i31, yn), (i32, yn),
                           (i41, status)]:
            db.session.add(models.Variable(case=i, item=item.id, value=random.choice(vals).id))
    db.session.commit()


def delete():
    for m in [models.Variable, models.ItemValue, models.Item, models.Category, models.Value]:
        db.session.query(m).delete()
    db.session.commit()


if __name__ == '__main__':
    main()
