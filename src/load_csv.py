"""Not really meant to be general purpose, but at least provides an example
of how to load data from csv into the database format.

Primary entry point is `unpack_categories` which unpacks, interprets, and
    loads the data into the database.

A second entry is `add_data_dictionary` which optionally uploads a data dictionary to
    the database. This can be accessed as a standalone module with `load_dd.py`.

"""
import csv
import hashlib
from loguru import logger
import re
from collections import defaultdict, namedtuple
from datetime import datetime
import sqlalchemy as sqla

from utils import xlsx_to_list

from dqt_api import db, app, cors, whooshee
from dqt_api import models
from dqt_api.__main__ import prepare_config
from dqt_api.manage import add_tabs, add_comments

COLUMN_TO_CATEGORY = {}
COLUMN_TO_DESCRIPTION = {}
COLUMN_TO_LABEL = {}  # column name to "label" (the visible piece)

CATEGORIES = {}  # name -> models.Category

VALUES = defaultdict(dict)  # name -> item -> models.Value
VALUES_BY_ITEM = defaultdict(dict)  # item -> id -> models.Value; for pre-defined ids

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


ItemVar = namedtuple('ItemVar', 'variable excluded')


def add_items(items, datamodel_vars, use_desc=False):
    """
    Add items to database along with their descriptions, and commit.
    :param datamodel_vars: dict of variables stored in data_model table
    :param use_desc: use variables that end in '_desc'
        these already include a descriptive value rather than a numeric reference to
        the value
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
                res.append(ItemVar(item, excluded=True))
                continue
        if item in COLUMN_TO_LABEL and (item not in desc or has_desc or not use_desc):
            # item = COLUMN_TO_LABEL[item]
            res.append(ItemVar(item, excluded=False))
            if item not in ITEMS:
                i = models.Item(name=COLUMN_TO_LABEL[item],
                                description=COLUMN_TO_DESCRIPTION[item],
                                category=CATEGORIES[COLUMN_TO_CATEGORY[item]].id)
                ITEMS[item] = i
                db.session.add(i)
        elif item in datamodel_vars:
            res.append(ItemVar(datamodel_vars[item], excluded=False))
        elif item in desc:  # desc version chosen
            res.append(ItemVar(item, excluded=True))
        else:
            logger.warning(f'Variable not marked for inclusion: "{item}".')
            res.append(ItemVar(item, excluded=True))
    db.session.commit()
    return res


def line_not_empty(lst):
    """Check if list is not empty and first value is not empty"""
    return bool(lst and lst[0])


def parse_csv(fp, datamodel_vars,
              items_from_data_dictionary_only):
    """
    Load csv file into database, committing after each case.
    :param datamodel_vars:
    :param items_from_data_dictionary_only:
    :param fp: path to csv file
    :return:
    """
    items = []
    curr_year = int(str(datetime.now().year)[-2:])
    if not CATEGORIES:
        add_categories()
    with open(fp, newline='') as fh:
        reader = csv.reader(fh)
        for i, line in enumerate(reader):
            if i == 0:  # make sure all items/variables already added to database
                items = add_items([x.lower() for x in line], datamodel_vars)
            elif line_not_empty(line):
                graph_data = defaultdict(lambda: None)  # separate summary data table
                for j, value in enumerate(line):
                    if not value.strip():  # empty/missing value: exclude
                        continue
                    value = value.lower()  # standardize to lowercase
                    curr_item = items[j].variable
                    if items[j].excluded:
                        logger.debug(f'Missing column #{j}: {curr_item} ({value.strip()})')
                        continue
                    # pre-processing values
                    # convert date to year
                    if re.match(r'(\d{2}\w{3}\d{4}|\d{1,2}\/\d{1,2}\/\d{4})', value):
                        value = str(int_round(value[-4:]))
                    elif re.match(r'(\d{2}\w{3}\d{2}|\d{1,2}\/\d{1,2}\/\d{2})', value):
                        value = int_round(value[-2:])
                        if value <= curr_year:
                            value = '20{}'.format(value)
                        else:
                            value = '19{}'.format(value)
                    elif 'age' in curr_item or 'year' in curr_item:
                        try:
                            value = str(int_round(value))
                        except ValueError:
                            pass

                    # get the Value model itself
                    new_value = None
                    if curr_item in VALUES_BY_ITEM:  # categorization/ordering already assigned
                        try:
                            v = int(value)
                        except ValueError:
                            if len(value) == 1:
                                v = int(value, 36)  # if letters were used
                            else:
                                v = None
                        if v is None:  # no categorization/numical found, maybe string value?
                            for val in VALUES_BY_ITEM[curr_item].values():
                                if val.name == value:
                                    new_value = val
                                    break
                            if not new_value:  # found a new category which expected to be defined
                                logger.info(f'No categorization found for `{curr_item}`: "{value}"')
                                order = max(VALUES_BY_ITEM[curr_item].keys()) + 1
                                logger.info(f'Adding new category to `{curr_item}`: "{value}" with order {order}')
                                new_value = models.Value(name=value, order=order)
                                VALUES[curr_item][value] = new_value
                                db.session.add(new_value)
                                VALUES_BY_ITEM[curr_item][order] = new_value
                        else:
                            if v in VALUES_BY_ITEM[curr_item]:
                                new_value = VALUES_BY_ITEM[curr_item][v]
                            elif '+' in VALUES_BY_ITEM[curr_item]:
                                new_value = VALUES_BY_ITEM[curr_item]['+']
                    elif items_from_data_dictionary_only:
                        continue  # skip if user only wants values from data dictionary
                    # don't include this as else because if-clause needs to go here
                    if not new_value:  # add value if it doesn't exist
                        if value not in VALUES[curr_item]:
                            val = models.Value(name=value)
                            VALUES[curr_item][value] = val
                            db.session.add(val)
                        new_value = VALUES[curr_item][value]

                    # data model variables
                    if curr_item in datamodel_vars:  # name appears == wanted
                        graph_data[datamodel_vars[curr_item]] = new_value.name  # get datamodel var name
                    elif curr_item in datamodel_vars.values():  # name not requested
                        graph_data[curr_item] = new_value.name
                        continue  # not included in actual dataset

                    # add variable with item and value
                    var = models.Variable(case=i,
                                          item=ITEMS[curr_item].id,
                                          value=new_value.id)
                    db.session.add(var)
                logger.debug('Cohort data: {}'.format(str(graph_data)))
                db.session.add(models.DataModel(case=i,
                                                age_bl=graph_data['age_bl'],
                                                age_fu=graph_data['age_fu'],
                                                sex=graph_data['gender'],
                                                enrollment=graph_data['enrollment'],
                                                followup_years=int_round(graph_data['followup_years'], 1),
                                                intake_date=graph_data['intake_date']))  # placeholder
                try:
                    db.session.commit()  # commit each case separately
                except sqla.exc.IntegrityError as e:
                    logger.exception('Likely duplicate primary key: drop table and re-run.', e)

                logger.info('Committed case #{} (stored with name {}).'.format(i + 1, i))


Row = namedtuple('Row', 'category description name label priority categories')


class CategorizationReader:

    def __init__(self, csv_file):
        self.fh = open(csv_file, newline='')
        self.reader = csv.reader(self.fh)
        header = next(self.reader)
        self.data = {}
        for i, column in enumerate(header):
            column = column.lower()
            for label in {'name', 'label', 'category', 'description', 'priority', 'categories'} - self.data.keys():
                if label in column:  # variable name
                    self.data[label] = i

    def _get_data(self, row, label, default=None):
        try:
            return row[self.data[label]]
        except KeyError:
            if default is None:
                raise ValueError(f'Column {label} is required.')
            return default

    def __iter__(self) -> Row:
        for row in self.reader:
            yield Row(
                category=self._get_data(row, 'category'),
                description=self._get_data(row, 'description'),
                name=self._get_data(row, 'name').lower(),
                label=self._get_data(row, 'label'),
                priority=int(self._get_data(row, 'priority', 0)),
                categories=self._get_data(row, 'categories', 0),
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fh.close()


def unpack_categories(categorization_csv, min_priority=0):
    """
    Primary entry point to move categories, items, and values into the database
    :param categorization_csv:
        columns:
            * Category: category of the variable
            * Variable description: short description of variable for hover text
            * Variable name: program shorthand to reference variable
            * Variable label: human-readable display label for the variable
            * Categories: values separated by '=='
                e.g., 1=male||2=female
            * Priority (optional): number showing importance of variable (higher is more important)
    :param min_priority: limit the priority by only allowing >= priorities; allow all=0
    :return:
    """
    global COLUMN_TO_CATEGORY, COLUMN_TO_DESCRIPTION, COLUMN_TO_LABEL
    with CategorizationReader(categorization_csv) as rows:
        for row in rows:
            if row.priority >= min_priority:
                COLUMN_TO_CATEGORY[row.name] = row.category
                if row.category in CATEGORIES:
                    category_instance = CATEGORIES[row.category]
                else:  # not yet added
                    category_instance = models.Category(name=row.category, order=len(CATEGORIES))
                    db.session.add(category_instance)
                    db.session.commit()
                    CATEGORIES[row.category] = category_instance
                COLUMN_TO_DESCRIPTION[row.name] = row.description
                COLUMN_TO_LABEL[row.name] = row.label
                if row.categories:  # these "categories" are really ITEMS
                    if re.match(r'\w[\=\s]', row.categories[:2]):
                        i = models.Item(name=row.label,
                                        description=row.description,
                                        category=category_instance.id)
                        for cat in row.categories.split('||'):
                            order, value = re.split(r'[\=\s]+', cat, maxsplit=1)
                            value = value.lower()  # standardize all values to lowercase
                            try:  # numbers first
                                order = int(order)
                            except ValueError:  # letters appear after numbers
                                order = int(order, 36)
                            v = models.Value(name=value, order=order)
                            db.session.add(v)
                            VALUES_BY_ITEM[row.name][order] = v
                            if value[-1] == '+':
                                VALUES_BY_ITEM[row.name]['+'] = v  # for values greater than
                            db.session.commit()
                    else:
                        i = models.Item(name=row.label,
                                        description=row.description,
                                        category=category_instance.id,
                                        is_numeric=True)
                    ITEMS[row.name] = i
                    db.session.add(i)
    db.session.commit()


def add_data_dictionary(input_files, file_name, label_column, name_column, category_col,
                        descript_col, value_column, **kwargs):
    """

    :param file_name:
    :param input_files: xls(x) files with columns specified below
    :param label_column: column with common name
    :param name_column: column with variable name
    :param category_col: if None, use sheet names as categories
    :param descript_col: column with description of the variable
    :param value_column: column that shows the various variables
    :param kwargs: n/a
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
            with open(input_file, 'rb') as fh:
                txt = fh.read()
                df = models.DataFile(
                    filename=file_name,
                    file=txt,
                    md5_checksum=hashlib.md5(txt).hexdigest()
                )
                db.session.add(df)
        else:
            raise ValueError('Unrecognized file extension: {}'.format(input_file.split('.')[-1]))
    db.session.commit()  # commit all files together, simplifies re-running should an error occur


def main():
    from load_utils import parser  # data dictionary loading options
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

    args, unk = parser.parse_known_args()

    app.config.from_pyfile(args.config)
    prepare_config(args.debug, args.whooshee_dir)
    args = parser.parse_args()

    logger.add('load_csv_{time}.log', backtrace=True, diagnose=True)

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
        # optionally generate and store the data dictionary
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
