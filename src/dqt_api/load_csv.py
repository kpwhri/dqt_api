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

# mapping of csv columns to category names
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
    """Round a number to the nearest 'base' """
    return int(base * round(float(x)/base))


def add_categories():
    """
    Add categories to database based on the specification in
    'column_to_category' global variable.
    :return:
    """
    for cat in set(COLUMN_TO_CATEGORY.values()):
        c = models.Category(name=cat)
        CATEGORIES[cat] = c
        db.session.add(c)
    db.session.commit()


def add_items(items, descriptions):
    """
    Add items to database along with their descriptions, and commit.
    :param items: label to display for each item
    :param descriptions: description of items (second column of csv)
    :return:
    """
    for item, label in zip(items, descriptions):
        if item in COLUMN_TO_CATEGORY:
            i = models.Item(name=item,
                            description=label,
                            category=CATEGORIES[COLUMN_TO_CATEGORY[item]].id)
            ITEMS[item] = i
            db.session.add(i)
    db.session.commit()


def parse_csv(fp, age, gender, enrollment):
    """
    Load csv file into database, committing after each case.
    :param fp:
    :param age: column name for age variable (for graphing)
    :param gender: column name for gender variable (for graphing)
    :param enrollment: column name for enrollment variable (for graphing)
    :return:
    """
    items = []
    add_categories()
    with open(fp, newline='') as fh:
        reader = csv.reader(fh)
        for i, line in enumerate(reader):
            if i == 0:
                items = [x.lower() for x in line]
            elif i == 1:  # labels
                add_items(items, line)
            else:
                graph_data = defaultdict(lambda: None)  # separate summary data table
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
                    new_value = VALUES[value]

                    # add variable with item and value
                    var = models.Variable(case=i,
                                          item=ITEMS[items[j]].id,
                                          value=new_value.id)
                    db.session.add(var)

                    if items[j] in [age, gender, enrollment]:
                        graph_data[items[j]] = value

                db.session.add(models.DataModel(case=i,
                                                age=graph_data[age],
                                                sex=graph_data[gender],
                                                enrollment=graph_data[enrollment]))
                db.session.commit()  # commit each case separately
                print('Committed case #{} (stored with name {}).'.format(i+1, i))


def parse_csv_for_graph_data(fp, age, gender, enrollment):
    """
    Add only data models (these used to be the last to load, and so error prone).

    :param fp:
    :param age:
    :param gender:
    :param enrollment:
    :return:
    """
    mapping = {}
    with open(fp, newline='') as fh:
        reader = csv.reader(fh)
        for i, line in enumerate(reader):
            graph_data = defaultdict(lambda: None)  # separate summary data table
            if i == 0:
                for j, value in enumerate(line):
                    if value.lower() in [age, gender, enrollment]:
                        mapping[j] = value.lower()
            elif i == 1:
                continue
            else:
                for j, value in enumerate(line):
                    if j not in mapping:
                        continue
                    if not value.strip():  # empty/missing value: exclude
                        continue
                    # convert date to year
                    if mapping[j] == age:
                        value = str(int_round(value))

                    graph_data[mapping[j]] = value
                db.session.add(models.DataModel(case=i,
                                                age=graph_data[age],
                                                sex=graph_data[gender],
                                                enrollment=graph_data[enrollment]))
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
    parser.add_argument('--only-graph-data', action='store_true', default=False,
                        help='This part did not complete.')
    parser.add_argument('--age', required=True,
                        help='Variable for age (for graphing).')
    parser.add_argument('--gender', required=True,
                        help='Variable for gender (for graphing).')
    parser.add_argument('--enrollment', required=True,
                        help='Variable for enrollment (for graphing).')
    args, unk = parser.parse_known_args()

    app.config.from_pyfile(args.config)
    prepare_config(args.debug)

    args = parser.parse_args()
    if args.only_graph_data:
        parse_csv_for_graph_data(args.csv_file, args.age, args.gender, args.enrollment)
    else:
        parse_csv(args.csv_file, args.age, args.gender, args.enrollment)


if __name__ == '__main__':
    main()
