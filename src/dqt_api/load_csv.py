"""Not really meant to be general purpose, but at least provides an example
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
COLUMN_TO_CATEGORY = {}
COLUMN_TO_DESCRIPTION = {}

CATEGORIES = {}  # name -> models.Category

VALUES = {}  # name -> models.Value

ITEMS = {}  # name -> models.Item


def int_round(x, base=5):
    """Round a number to the nearest 'base' """
    return int(base * round(float(x) / base))


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


def add_items(items):
    """
    Add items to database along with their descriptions, and commit.
    :param items: label to display for each item
    :return:
    """
    for item in items:
        if item in COLUMN_TO_CATEGORY:
            i = models.Item(name=item,
                            description=COLUMN_TO_DESCRIPTION[item],
                            category=CATEGORIES[COLUMN_TO_CATEGORY[item]].id)
            ITEMS[item] = i
            db.session.add(i)
    db.session.commit()


def parse_csv(fp, age, gender, enrollment, intake_date, followup_years, enrollment_to_followup,
              enrollment_before_baseline):
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
                add_items(items)
            else:
                graph_data = defaultdict(lambda: None)  # separate summary data table
                for j, value in enumerate(line):
                    if not value.strip():  # empty/missing value: exclude
                        continue
                    if items[j] not in COLUMN_TO_CATEGORY:
                        print('Missing: {}'.format(value.strip()))
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

                    if items[j] in [age, gender, enrollment, enrollment_before_baseline,
                                    enrollment_to_followup, followup_years, intake_date]:
                        graph_data[items[j]] = value

                db.session.add(models.DataModel(case=i,
                                                age=graph_data[age],
                                                sex=graph_data[gender],
                                                enrollment=graph_data[enrollment],
                                                enrollment_before_baseline=int(float(graph_data[enrollment_before_baseline])),
                                                enrollment_to_followup=int(float(graph_data[enrollment_to_followup])),
                                                followup_years=int(float(graph_data[followup_years])),
                                                intake_date=graph_data[intake_date]))
                db.session.commit()  # commit each case separately
                print('Committed case #{} (stored with name {}).'.format(i + 1, i))


def parse_csv_for_graph_data(fp, age, gender, enrollment, intake_date, followup_years,
                             enrollment_to_followup, enrollment_before_baseline):
    """
    Add only data models (these used to be the last to load, and so error prone).

    :param intake_date:
    :param followup_years:
    :param enrollment_to_followup:
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
                                                enrollment=graph_data[enrollment],
                                                enrollment_before_baseline=graph_data[enrollment_before_baseline],
                                                enrollment_to_followup=graph_data[enrollment_to_followup],
                                                followup_years=graph_data[followup_years],
                                                intake_date=graph_data[intake_date]),
                               )
    db.session.commit()


def unpack_categories(categorization_csv):
    global COLUMN_TO_CATEGORY, COLUMN_TO_DESCRIPTION
    with open(categorization_csv, newline='') as fh:
        reader = csv.reader(fh)
        for i, lst in enumerate(reader):
            if i == 0:  # skip header
                continue
            variable, category, description = lst
            COLUMN_TO_CATEGORY[variable.lower()] = category
            COLUMN_TO_DESCRIPTION[variable.lower()] = description


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
    parser.add_argument('--age', required=True, type=str.lower,
                        help='Variable for age (for graphing).')
    parser.add_argument('--gender', required=True, type=str.lower,
                        help='Variable for gender (for graphing).')
    parser.add_argument('--enrollment', required=True, type=str.lower,
                        help='Variable for enrollment status (for graphing).')
    parser.add_argument('--enrollment-before-baseline', required=True, type=str.lower,
                        help='Variable for enrollment years before baseline date (for graphing).')
    parser.add_argument('--enrollment-to-followup', required=True, type=str.lower,
                        help='Variable for enrollment years until last followup date (for graphing).')
    parser.add_argument('--followup-years', required=True, type=str.lower,
                        help='Variable for years of presence in cohort (for graphing).')
    parser.add_argument('--intake-date', required=True, type=str.lower,
                        help='Variable for date when subject was added to cohort (for graphing).')
    parser.add_argument('--categorization-csv', required=True,
                        help='CSV/TSV containing columns Variable/Column-Category-ColumnDescription')
    args, unk = parser.parse_known_args()

    app.config.from_pyfile(args.config)
    prepare_config(args.debug)

    args = parser.parse_args()

    unpack_categories(args.categorization_csv)
    if args.only_graph_data:
        parse_csv_for_graph_data(args.csv_file, args.age, args.gender, args.enrollment,
                                 args.intake_date, args.followup_years, args.enrollment_to_followup,
                                 args.enrollment_before_baseline
                                 )
    else:
        parse_csv(args.csv_file, args.age, args.gender, args.enrollment,
                                 args.intake_date, args.followup_years, args.enrollment_to_followup,
                                 args.enrollment_before_baseline)


if __name__ == '__main__':
    main()
