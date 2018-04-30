"""Not really meant to be general purpose, but at least provides an example
of how to load data from csv into the dqt format.

"""
import argparse
import csv
import hashlib
import re
from collections import defaultdict
from datetime import datetime

from cronkd.util.xlsx import xlsx_to_list

from dqt_api import db, app, cors, whooshee
from dqt_api import models
from dqt_api.__main__ import prepare_config
from dqt_api.manage import add_tabs, add_comments

COLUMN_TO_CATEGORY = {}
COLUMN_TO_DESCRIPTION = {}
COLUMN_TO_LABEL = {}  # column name to "label" (the visible piece)

CATEGORIES = {}  # name -> models.Category

VALUES = {}  # name -> models.Value
VALUES_BY_ITEM = defaultdict(dict)

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


def add_items(items, datamodel_vars, use_desc=False):
    """
    Add items to database along with their descriptions, and commit.
    :param items: label to display for each item
    :return:
    """
    res = []
    desc = [i[:-5] for i in items if i.endswith('_desc')]
    for item in items:
        has_desc = False  # HACK: certain labels I only want when they end in "_desc"
        if item.endswith('_desc'):
            if use_desc:
                item = item[:-5]
                has_desc = True
            else:  # skip if not using descending
                res.append(None)
                continue
        if item in COLUMN_TO_LABEL and (item not in desc or has_desc or not use_desc):
            # item = COLUMN_TO_LABEL[item]
            res.append(item)
            if item not in ITEMS:
                i = models.Item(name=COLUMN_TO_LABEL[item],
                                description=COLUMN_TO_DESCRIPTION[item],
                                category=CATEGORIES[COLUMN_TO_CATEGORY[item]].id)
                ITEMS[item] = i
                db.session.add(i)
        elif item in datamodel_vars:
            res.append(datamodel_vars[item])
        elif item in desc:
            res.append(None)
            pass
        else:
            print('Missing column: {}.'.format(item))
            res.append(None)

    db.session.commit()
    return res


def line_not_empty(lst):
    return bool(lst and lst[0])


def parse_csv(fp, datamodel_vars,
              items_from_data_dictionary_only):
    """
    Load csv file into database, committing after each case.
    :param datamodel_vars:
    :param items_from_data_dictionary_only:
    :param fp:
    :return:
    """
    items = []
    curr_year = int(str(datetime.now().year)[-2:])
    if not CATEGORIES:
        add_categories()
    with open(fp, newline='') as fh:
        reader = csv.reader(fh)
        for i, line in enumerate(reader):
            if i == 0:
                items = add_items([x.lower() for x in line], datamodel_vars)
            elif line_not_empty(line):
                graph_data = defaultdict(lambda: None)  # separate summary data table
                for j, value in enumerate(line):
                    if not value.strip():  # empty/missing value: exclude
                        continue
                    if not items[j]:
                        print('Missing column #{}: {} ({})'.format(j, items[j], value.strip()))
                        continue

                    # pre-processing values
                    # convert date to year
                    if re.match('(\d{2}\w{3}\d{4}|\d{1,2}\/\d{1,2}\/\d{4})', value):
                        value = str(int_round(value[-4:]))
                    elif re.match('(\d{2}\w{3}\d{2}|\d{1,2}\/\d{1,2}\/\d{2})', value):
                        value = int_round(value[-2:])
                        if value <= curr_year:
                            value = '20{}'.format(value)
                        else:
                            value = '19{}'.format(value)
                    elif 'age' in items[j] or 'year' in items[j]:
                        try:
                            value = str(int_round(value))
                        except ValueError:
                            pass

                    # get the Value model itself
                    new_value = None
                    if items[j] in VALUES_BY_ITEM:
                        try:
                            v = int(value)
                        except ValueError:
                            pass
                        else:
                            if v in VALUES_BY_ITEM[items[j]]:
                                new_value = VALUES_BY_ITEM[items[j]][v]
                            elif '+' in VALUES_BY_ITEM[items[j]]:
                                new_value = VALUES_BY_ITEM[items[j]]['+']
                    elif items_from_data_dictionary_only:
                        continue  # skip if user only wants values from data dictionary
                    # don't include this as else because if-clause needs to go here
                    if not new_value:  # add value if it doesn't exist
                        value = value.lower()
                        if value not in VALUES:
                            val = models.Value(name=value)
                            VALUES[value] = val
                            db.session.add(val)
                        new_value = VALUES[value]

                    # data model variables
                    if items[j] in datamodel_vars:  # name appears == wanted
                        graph_data[datamodel_vars[items[j]]] = new_value.name  # get datamodel var name
                    elif items[j] in datamodel_vars.values():  # name not requested
                        graph_data[items[j]] = new_value.name
                        continue  # not included in actual dataset

                    # add variable with item and value
                    var = models.Variable(case=i,
                                          item=ITEMS[items[j]].id,
                                          value=new_value.id)
                    db.session.add(var)
                print(graph_data)
                db.session.add(models.DataModel(case=i,
                                                age_bl=graph_data['age_bl'],
                                                age_fu=graph_data['age_fu'],
                                                sex=graph_data['gender'],
                                                enrollment=graph_data['enrollment'],
                                                followup_years=int_round(graph_data['followup_years'], 1),
                                                intake_date=graph_data['intake_date']))  # placeholder
                db.session.commit()  # commit each case separately
                print('Committed case #{} (stored with name {}).'.format(i + 1, i))


def unpack_categories(categorization_csv, min_priority):
    global COLUMN_TO_CATEGORY, COLUMN_TO_DESCRIPTION, COLUMN_TO_LABEL
    with open(categorization_csv, newline='') as fh:
        reader = csv.reader(fh)
        header = None
        for i, lst in enumerate(reader):
            if i == 0:  # skip header
                header = [x.lower() if x else '' for x in lst]
                continue
            category, description, name, label, *extra = lst
            if 'priority' in header:
                priority = extra[header.index('priority') - 4]
            else:
                priority = 0
            if int(priority) >= min_priority:
                COLUMN_TO_CATEGORY[name.lower()] = category
                if category in CATEGORIES:
                    category_instance = CATEGORIES[category]
                else:  # not yet added
                    category_instance = models.Category(name=category, order=len(CATEGORIES))
                    db.session.add(category_instance)
                    db.session.commit()
                    CATEGORIES[category] = category_instance
                COLUMN_TO_DESCRIPTION[name.lower()] = description
                COLUMN_TO_LABEL[name.lower()] = label
                if 'categories' in header:  # these "categories" are really ITEMS
                    categories = extra[header.index('categories') - 4].strip()
                    if categories[0] in '0123456789':
                        i = models.Item(name=label,
                                        description=description,
                                        category=category_instance.id)
                        for cat in categories.split('||'):
                            order, value = re.split(r'[\=\s]+', cat, maxsplit=1)
                            order = int(order)
                            v = models.Value(name=value, order=order)
                            db.session.add(v)
                            VALUES_BY_ITEM[name.lower()][order] = v
                            if value[-1] == '+':
                                VALUES_BY_ITEM[name.lower()]['+'] = v  # for values greater than
                            db.session.commit()
                    else:
                        i = models.Item(name=label,
                                        description=description,
                                        category=category_instance.id,
                                        is_numeric=True)
                    ITEMS[name.lower()] = i
                    db.session.add(i)
    db.session.commit()


def add_data_dictionary(input_files, file_name, label_column, name_column, category_col,
                        descript_col, value_column, **kwargs):
    """

    :param input_files:
    :param label_column:
    :param name_column:
    :param category_col:
    :param descript_col:
    :param value_column:
    :param kwargs:
    :return:
    """
    for input_file in input_files:
        if 'xls' in input_file.split('.')[-1]:
            if category_col:
                columns_to_keep = [label_column, name_column, descript_col, value_column, category_col]
            else:
                columns_to_keep = [label_column, name_column, descript_col, value_column]
            for label, name, descript, value, category in xlsx_to_list(
                input_file,
                columns_to_keep=columns_to_keep,
                include_header=False,
                append_sheet_name=None if category_col else 'Category'
            ):
                if name is None:
                    continue
                de = models.DataEntry(
                    label=label,
                    variable=name,
                    values=value,
                    description=descript,
                    category=category
                )
                db.session.add(de)
        else:
            raise ValueError('Unrecognized file extension: {}'.format(input_file.split('.')[-1]))
    with open(input_file, 'rb') as fh:
        txt = fh.read()
        df = models.DataFile(
            filename=file_name,
            file=txt,
            md5_checksum=hashlib.md5(txt).hexdigest()
        )
        db.session.add(df)
    db.session.commit()


def main():
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--config', required=True,
                        help='File containing configuration information. '
                             'BASE_DIR, SECRET_KEY.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Run in debug mode.')
    parser.add_argument('--whooshee-dir', default=False, action='store_true',
                        help='Use whooshee directory in BASE_DIR.')
    parser.add_argument('--csv-file',
                        help='Input csv file containing separate record per line.')
    parser.add_argument('--age-bl', required=True, type=str.lower,
                        help='Variable for age (for graphing).')
    parser.add_argument('--age-fu', required=True, type=str.lower,
                        help='Variable for age (for graphing).')
    parser.add_argument('--gender', required=True, type=str.lower,
                        help='Variable for gender (for graphing).')
    parser.add_argument('--enrollment', required=True, type=str.lower,
                        help='Variable for enrollment status (for graphing).')
    parser.add_argument('--followup-years', required=True, type=str.lower,
                        help='Variable for years of presence in cohort (for graphing).')
    parser.add_argument('--intake-date', required=True, type=str.lower,
                        help='Variable for date when subject was added to cohort (for graphing).')
    parser.add_argument('--categorization-csv', required=True,
                        help='CSV/TSV containing columns Variable/Column-Category-ColumnDescription')
    parser.add_argument('--minimum-priority', type=int, default=0,
                        help='Minimum priority to allow for variable prioritization. Allow all = 0.')
    parser.add_argument('--items-from-data-dictionary-only', default=False, action='store_true',
                        help='Only collect items from the data dictionary. '
                             '(Values will still be collected from both.)')
    parser.add_argument('--tab-file', required=True,
                        help='"=="-separated file with tab information.')
    parser.add_argument('--comment-file', required=False,
                        help='"=="-separated file with comments which can be appended '
                             'to various locations (only "table" currently supported).')
    # data dictionary loading options
    parser.add_argument('--dd-input-file', nargs='+',
                        help='Path to excel document(s) containing data dictionary.'
                             ' Values are expected to be contained in the first table'
                             ' in the file.')
    parser.add_argument('--dd-file-name', default='data-dictionary.xlsx',
                        help='Filename for downloads')
    parser.add_argument('--dd-label-column', required=True,
                        help='Index of label column in document')
    parser.add_argument('--dd-category-column',
                        help='Index of category column in document')
    parser.add_argument('--dd-name-column',
                        help='Index of variable name column in document')
    parser.add_argument('--dd-description-column',
                        help='Index of description column in document')
    parser.add_argument('--dd-value-column',
                        help='Index of values column in document')

    args, unk = parser.parse_known_args()

    app.config.from_pyfile(args.config)
    prepare_config(args.debug, args.whooshee_dir)
    args = parser.parse_args()

    unpack_categories(args.categorization_csv, args.minimum_priority)
    datamodel_vars = {
        args.age_bl: 'age_bl',
        args.age_fu: 'age_fu',
        args.gender: 'gender',
        args.enrollment: 'enrollment',
        args.intake_date: 'intake_date',
        args.followup_years: 'followup_years'
    }
    if args.tab_file:
        add_tabs(args.tab_file)
    if args.comment_file:
        add_comments(args.comment_file)
    parse_csv(args.csv_file, datamodel_vars, args.items_from_data_dictionary_only)

    if args.dd_input_file:
        add_data_dictionary(
            args.dd_input_file,
            args.dd_file_name,
            args.dd_label_column,
            args.dd_name_column,
            args.dd_category_column,
            args.dd_description_column,
            args.dd_value_column,
        )


if __name__ == '__main__':
    main()
