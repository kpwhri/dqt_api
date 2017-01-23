"""Not really meant to general purpose, but at least provides an example
of how to load data from csv into the dqt format.

"""
import argparse
import csv

import re
from collections import defaultdict

from dqt_api import db, app
from dqt_api import models
from dqt_api.__main__ import prepare_config

COLUMN_TO_CATEGORY = {
    'casi_irt': 'CASI',
    'anyad': 'Dementia/AD',
    'onsetdate': 'Dementia/AD',
    'diabetes': 'Comorbidities',
    'insulin': 'Comorbidities',
    'stroke': 'Comorbidities',
    'cesd_flag': 'Comorbidities',
    'cesd_score': 'Comorbidities',
    'hrt': 'Comorbidities',
    'adl_flag': 'Daily Living',
    'adl_sum': 'Daily Living',
    'iadl_flag': 'Daily Living',
    'iadl_sum': 'Daily Living',
    'intakedt': 'Cohort',
    'autopsy': 'Cohort',
    'current_status': 'Cohort',
    'gender': 'Demographics',
    'race_target': 'Demographics',
    'hispanic': 'Demographics',
    'years_enrolled': 'Cohort',
    'cohort_exit_date': 'Cohort',
    'baseline_age': 'Demographics',
}

CATEGORIES = {}  # name -> models.Category

VALUES = {}  # name -> models.Value

ITEMS = {}  # name -> models.Item


def int_round(x, base=5):
    return int(base * round(float(x)/base))


def add_categories():
    for cat in set(COLUMN_TO_CATEGORY.values()):
        c = models.Category(name=cat)
        CATEGORIES[cat] = c
        db.session.add(c)
    db.session.commit()


def add_items(items, labels):
    for item, label in zip(items, labels):
        if item in COLUMN_TO_CATEGORY:
            i = models.Item(name=item,
                            description=label,
                            category=CATEGORIES[COLUMN_TO_CATEGORY[item]].id)
            ITEMS[item] = i
            db.session.add(i)
    db.session.commit()


def parse_csv(fp):
    items = []
    graph_data = defaultdict(defaultdict)  # separate summary data table
    add_categories()
    with open(fp, newline='') as fh:
        reader = csv.reader(fh)
        for i, line in enumerate(reader):
            if i == 0:
                items = [x.lower() for x in line]
            elif i == 1:  # labels
                add_items(items, line)
            else:
                for j, value in enumerate(line):
                    if not value.strip():  # empty/missing value: exclude
                        continue
                    if items[j] not in COLUMN_TO_CATEGORY:
                        continue
                    # convert date to year
                    if re.match('\d{2}\w{3}\d{4}', value):
                        value = str(int_round(value[-4:]))
                    elif re.match('\d{2}\w{3}\d{2}', value):
                        value = int_round(value[-2:])
                        if value < 20:
                            value = '20{}'.format(value)
                        else:
                            value = '19{}'.format(value)
                    elif 'age' in items[j] or 'year' in items[j]:
                        value = str(int_round(value))

                    # add value if it doesn't exist
                    if value not in VALUES:
                        val = models.Value(name=value)
                        VALUES[value] = val
                        db.session.add(val)
                        db.session.commit()
                    new_value = VALUES[value]

                    # add variable with item and value
                    var = models.Variable(case=i,
                                          item=ITEMS[items[j]].id,
                                          value=new_value.id)
                    db.session.add(var)

                    if items[j] in ['gender', 'baseline_age', 'current_status']:
                        graph_data[i][items[j]] = value
                db.session.commit()
    for case in graph_data:
        db.session.add(models.DataModel(case=case, **graph_data[case]))
    db.session.commit()


def main():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--config', required=True,
                        help='File containing configuration information. '
                             'BASE_DIR, SECRET_KEY.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Run in debug mode.')
    parser.add_argument('--csv-file',
                        help='Input csv file containing separate record per line.')
    args, unk = parser.parse_known_args()

    app.config.from_pyfile(args.config)
    prepare_config(args.debug)

    args = parser.parse_args()
    parse_csv(args.csv_file)


if __name__ == '__main__':
    main()
