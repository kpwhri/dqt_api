"""Not really meant to be general purpose, but at least provides an example
of how to load data from csv into the database format.

Primary entry point is `unpack_categories` which unpacks, interprets, and
    loads the data into the database.

A second entry is `add_data_dictionary` which optionally uploads a data dictionary to
    the database. This can be accessed as a standalone module with `load_dd.py`.

"""
import csv
import hashlib
import string

import pandas as pd
from loguru import logger
import re
from collections import defaultdict, namedtuple
import sqlalchemy as sqla
from pandas import to_numeric

from load_utils import clean_text_for_web
from utils import xlsx_to_list

from dqt_api import db, app, cors, whooshee
from dqt_api import models
from dqt_api.__main__ import prepare_config
from dqt_api.manage import add_tabs, add_comments

COLUMN_TO_DOMAIN = {}
COLUMN_TO_DESCRIPTION = {}
COLUMN_TO_LABEL = {}  # column name to "label" (the visible piece)

DOMAINS = {}  # name -> models.Category

VALUES = defaultdict(dict)  # name -> item -> models.Value
VALUES_BY_ITEM = defaultdict(dict)  # item -> id -> models.Value; for pre-defined ids

ITEMS = {}  # name -> models.Item


def clean_number(value):
    """Remove spaces and dollar signs to convert to numeric"""
    if isinstance(value, (int, float)):
        return value
    return value.replace(',', '').replace('$', '').strip()


def create_function_for_range(value: str):
    """Create function that defines a particular value/expression

    E.g., income in range $10,000 - $20,000
    """
    # remove any problems for ints
    value = clean_number(value)
    to_numeric_func = float if '.' in value else int
    if value[-1] == '+':  # gte
        value = to_numeric_func(value.rstrip('+'))
        return lambda x: to_numeric_func(clean_number(x)) >= value
    elif value[-1] == '-':  # lte
        value = to_numeric_func(value.rstrip('-'))
        return lambda x: to_numeric_func(clean_number(x)) <= value
    elif value.startswith('>='):
        value = to_numeric_func(value[2:])
        return lambda x: to_numeric_func(clean_number(x)) >= value
    elif value.startswith('<='):
        value = to_numeric_func(value[2:])
        return lambda x: to_numeric_func(clean_number(x)) <= value
    elif value.startswith('>'):
        value = to_numeric_func(value[1:])
        return lambda x: to_numeric_func(clean_number(x)) > value
    elif value.startswith('<'):
        value = to_numeric_func(value[1:])
        return lambda x: to_numeric_func(clean_number(x)) < value
    elif '-' in value:  # assumes end is EXCLUSIVE
        v1, v2 = value.split('-')
        v1 = to_numeric(clean_number(v1))
        v2 = to_numeric(clean_number(v2))
        return lambda x: v1 <= to_numeric_func(clean_number(x)) < v2


def int_round(x, base=5):
    """Round a number to the nearest 'base' """
    return int(base * round(float(x) / base))


def int_only(x):
    """Convert to float, then round to nearest 1."""
    return int_round(x, base=1)


def int_floor(x, base=5):
    """Round number down to the nearest 'base' """
    return int(float(x) - (float(x) % base))


def int_mid(x, base=5):
    """
    For rounding to still respect flter ranges, a midpoint must be selected.
    Suppose the slider bar allows 0,5,10,15,... and we have ages 14,10,8, and 5.
        If we `int_floor` only:
            14 -> 10
            10 -> 10
            8 -> 5
            5 -> 5
        And the user selects the range [0, 5] we will get both 5s (one of which is actually an 8).
    To avoid this, we'll add half the base back.
    We must keep track of the ages 5 and 10 because we don't want them to fall in the middle of the range:
        If we `int_mid(n, base=5)` without checking the prior value:
            14 -> 12
            10 -> 12
            8 -> 7
            5 -> 7
        Now the range [5, 10] will include 5, 8 but not 10 (!) since we incremented it.

    NOTE: This will not work if base=1, but that's probably expected since we're clearly int-rounding.
    """
    new_x = int_floor(x, base=base)  # get the floor
    if new_x != x:  # increment if not a range edge/border value which must remain on the edge
        new_x += base // 2  # get the integer midpoint to set value in the middle of the range
    return new_x


def add_categories():
    """
    Add categories to database based on the specification in
    'column_to_category' global variable.
    :return:
    """
    for cat in set(COLUMN_TO_DOMAIN.values()):
        c = models.Category(name=cat)
        DOMAINS[cat] = c
        db.session.add(c)
    db.session.commit()


ItemVar = namedtuple('ItemVar', 'variable excluded has_date has_age_year has_years')


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
    res = {}

    def add(it, excluded=False):
        # if 'age' or 'yr' in variable name, set these to be rounded down to nearest 5 (e.g., 'age_bl': 94 -> 90)
        has_age_year = 'age' in it or 'yr' in it or 'year' in it
        # if year is plural, suggests range (e.g., followup_years '19.1' -> 19), so allow it to be more precise
        has_years = ('yr' in it and 'yrs' in it) or ('year' in it and 'years' in it)
        # if year is date, extract year-only and round down to nearest 5 (death_yr 1999 -> 1995)
        has_date = 'dt' in it or 'date' in it
        res[item] = ItemVar(it, excluded=excluded, has_date=has_date, has_age_year=has_age_year, has_years=has_years)

    desc = {i[:-5] for i in items if i.endswith('_desc')}
    for item in items:
        has_desc = False  # HACK: certain labels I only want when they end in "_desc"
        if item.endswith('_desc'):
            if use_desc:
                item = item[:-5]
                has_desc = True
            else:  # skip if not using descending
                add(item, excluded=True)
                continue
        if item in COLUMN_TO_LABEL and (item not in desc or has_desc or not use_desc):
            # item = COLUMN_TO_LABEL[item]
            add(item, excluded=False)
            if item not in ITEMS:
                i = models.Item(name=COLUMN_TO_LABEL[item],
                                description=COLUMN_TO_DESCRIPTION[item],
                                category=DOMAINS[COLUMN_TO_DOMAIN[item]].id)
                ITEMS[item] = i
                db.session.add(i)
        elif item in datamodel_vars:
            add(item, excluded=False)
        elif item in desc:  # desc version chosen
            add(item, excluded=True)
        else:
            logger.warning(f'Variable not marked for inclusion: "{item}".')
            add(item, excluded=True)
    db.session.commit()
    return res


def line_not_empty(lst):
    """Check if list is not empty and first value is not empty"""
    return bool(lst and lst[0])


def add_values(cdf, col, item, lookup_col=None, graph_data=None, datamodel_vars: dict = None):
    """
    Add all elements from `col` to database.

    If graph_data supplied, this variable is used in the data_model to control the two graphs and table.
    """
    if lookup_col is None or lookup_col not in cdf.columns:
        lookup_col = col
    for row in cdf[[col, lookup_col]].drop_duplicates().itertuples():
        lookup_value = getattr(row, lookup_col)
        value = getattr(row, col)
        value_model = None
        if lookup_value in VALUES_BY_ITEM[item]:
            value_model = VALUES_BY_ITEM[item][lookup_value]
        elif '+' in VALUES_BY_ITEM[item]:
            for func, func_value in VALUES_BY_ITEM[item]['+']:
                if func(value):
                    value_model = func_value
        if value_model is None:  # add value if it doesn't exist
            if lookup_value in VALUES[item]:
                value_model = VALUES[item][value]
            else:
                value_model = models.Value(name=value)
                logger.warning(f'Adding new value: {value} = {item}')
                # VALUES[item][value] = val  # don't need to add since column-based
                db.session.add(value_model)
        variables = []
        for case in cdf[(cdf[lookup_col] == lookup_value) & (cdf[col] == value)].index:
            if item in ITEMS:  # ensure this is in data dictionary
                variables.append(models.Variable(
                    case=case, item=ITEMS[item].id, value=value_model.id
                ))
            if graph_data:  # add to graph
                graph_data[case][datamodel_vars[item]] = value_model.name
        db.session.bulk_save_objects(variables)
        db.session.commit()


def save_data_model(graph_data):
    logger.info(f'Saving graph data.')
    dms = []
    for case, data in graph_data.items():
        dms.append(
            models.DataModel(
                case=case,
                age_bl=graph_data[case]['age_bl'],
                age_fu=graph_data[case]['age_fu'],
                sex=graph_data[case]['gender'],
                enrollment=graph_data[case]['enrollment'],
                followup_years=graph_data[case]['followup_years'])
        )
    db.session.bulk_save_objects(dms)
    db.session.commit()
    logger.info(f'Finished saving graph data.')


def parse_csv(fp, datamodel_vars,
              items_from_data_dictionary_only, skip_rounding: set[str] = None):
    """
    Load csv file into database, committing after each case.
    :param datamodel_vars:
    :param items_from_data_dictionary_only:
    :param fp: path to csv file
    :param skip_rounding: set of columns names (i.e., variable names)
    :return:
    """
    if not DOMAINS:
        add_categories()
    logger.info(f'Parsing variables from csv file: {fp}')
    df = pd.read_csv(fp, dtype=object)
    graph_data = {i: dict() for i in range(df.shape[0])}
    df.columns = [c.lower() for c in df.columns]
    columns = [col for col in df.columns if col not in datamodel_vars.keys()]
    items = add_items(df.columns, datamodel_vars)  # ensure items exist
    sentinel = object()
    col_number = 0
    for col in list(datamodel_vars.keys()) + [sentinel] + columns:
        if col == sentinel:
            save_data_model(graph_data)
            graph_data = None
            continue
        col_number += 1
        if items[col].excluded:
            logger.debug(f'Missing column #{col_number} {col}: {items[col].variable}')
            continue

        logger.info(f'Processing column #{col_number}: {col}.')
        curr_item = items[col].variable
        # remove missing data
        cdf = pd.DataFrame(df[col].dropna().str.lower())
        cdf = cdf[~cdf[col].isin({'.', '', 'missing'})]
        # handle date: convert to year
        if items[col].has_date:
            if col in skip_rounding:
                cdf = pd.DataFrame(pd.to_datetime(cdf[col]).dt.year.apply(int_only))
                logger.warning(f'Skipping rounding for column with date: {col}')
            else:
                cdf = pd.DataFrame(pd.to_datetime(cdf[col]).dt.year.apply(int_mid))
            add_values(cdf, col, curr_item, graph_data=graph_data, datamodel_vars=datamodel_vars)
        elif items[col].has_age_year or items[col].has_years:
            if col in skip_rounding:
                cdf = pd.DataFrame(cdf[col].apply(int_only))
                logger.warning(f'Skipping rounding for column with age/year: {col}')
            else:
                cdf = pd.DataFrame(cdf[col].apply(int_mid))
            add_values(cdf, col, curr_item, graph_data=graph_data, datamodel_vars=datamodel_vars)
        elif curr_item in VALUES_BY_ITEM:  # categorisation/ordering already assigned
            values = cdf[col].unique()
            mapping = {}
            lookup_mapping = {}
            for value in values:
                if '.' in value:
                    mapping[value] = float(value)
                    lookup_mapping[value] = int(mapping[value])
                    continue
                elif len(value) == 1 and value in string.ascii_lowercase:
                    mapping[value] = int(value, 36)
                    lookup_mapping[value] = mapping[value]
                    continue
                else:
                    try:
                        mapping[value] = int(value)
                        lookup_mapping[value] = mapping[value]
                    except ValueError:
                        pass
                    else:
                        continue
                for possible_value in VALUES_BY_ITEM[curr_item].values():
                    if isinstance(possible_value, list):
                        continue  # skip '+'
                    if possible_value.name == value:
                        mapping[value] = possible_value
                        lookup_mapping[value] = possible_value
                        break
                else:  # if no value found, look in the '+' section
                    if '+' in VALUES_BY_ITEM[curr_item]:
                        for func, func_value in VALUES_BY_ITEM[curr_item]['+']:
                            if func(value):
                                # no change, func_value is models.Value which can't be fed into the `add_values` func
                                mapping[value] = value
                                lookup_mapping[value] = value
                                break
                    if value not in mapping:  # not sure what happened, let's log and add an extra variable
                        logger.warning(f'No categorization found for `{curr_item}`: "{value}"')
                        order = max((x for x in VALUES_BY_ITEM[curr_item].keys() if isinstance(x, (int, float))),
                                    default=123) + 1
                        logger.info(f'Adding new category to `{curr_item}`: "{value}" with order {order}')
                        value_model = models.Value(name=value, order=order)
                        db.session.add(value_model)
                        VALUES_BY_ITEM[curr_item][order] = value_model
                        mapping[value] = order
                        lookup_mapping[value] = order
            cdf[f'{col}_temp'] = cdf[col].apply(lambda x: mapping[x])
            cdf[f'{col}_lkp'] = cdf[col].apply(lambda x: lookup_mapping[x])
            cdf[col] = cdf[f'{col}_temp']
            del cdf[f'{col}_temp']
            add_values(cdf, col, curr_item, lookup_col=f'{col}_lkp',
                       graph_data=graph_data, datamodel_vars=datamodel_vars)
        elif items_from_data_dictionary_only:
            continue  # skip if user only wants values from data dictionary
        else:
            # these are values that need to be added directly:
            # 1. integers/floats (these will all be converted to float by models.Value
            # 2. strings without an order (these will be left as strings, without order)
            add_values(cdf, col, curr_item, graph_data=graph_data, datamodel_vars=datamodel_vars)

        try:
            db.session.commit()  # commit each column separately
        except sqla.exc.IntegrityError as e:
            logger.exception('Likely duplicate primary key: drop table and re-run.', e)

        logger.info(f'Finished column #{col_number}')
    logger.info(f'Done! Finished loading variables.')


Row = namedtuple('Row', 'domain description name label priority values')


class CategorizationReader:

    def __init__(self, csv_file, encoding='utf8'):
        self.fh = open(csv_file, newline='', encoding=encoding)
        self.reader = csv.reader(self.fh)
        header = next(self.reader)
        self.data = {}
        for i, column in enumerate(header):
            column = column.lower()
            for label in {'name', 'label', 'domain', 'description', 'priority', 'values'} - self.data.keys():
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
            # enforce limits
            description = self._get_data(row, 'description')
            if len(description) > 499:
                description = description[:496] + '...'
            yield Row(
                domain=self._get_data(row, 'domain'),
                description=description,
                name=self._get_data(row, 'name').lower(),
                label=self._get_data(row, 'label'),
                priority=int(self._get_data(row, 'priority', 0)),
                values=self._get_data(row, 'values', 0),
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fh.close()


def unpack_domains(categorization_csv, min_priority=0):
    """
    Primary entry point to move categories, items, and values into the database
    :param categorization_csv:
        columns:
            * Domain: domain of the variable
            * Variable description: short description of variable for hover text
            * Variable name: program shorthand to reference variable
            * Variable label: human-readable display label for the variable
            * Values: values separated by '=='
                e.g., 1=male||2=female
            * Priority (optional): number showing importance of variable (higher is more important)
    :param min_priority: limit the priority by only allowing >= priorities; allow all=0
    :return:
    """
    global COLUMN_TO_DOMAIN, COLUMN_TO_DESCRIPTION, COLUMN_TO_LABEL
    logger.info(f'Unpacking domains from the categorisation CSV file.')
    with CategorizationReader(categorization_csv) as rows:
        for row in rows:
            if row.priority >= min_priority:
                COLUMN_TO_DOMAIN[row.name] = row.domain
                if row.domain in DOMAINS:
                    domain_instance = DOMAINS[row.domain]
                else:  # not yet added
                    logger.info(f'Adding variables from domain {row.domain}')
                    domain_instance = models.Category(name=row.domain, order=len(DOMAINS))
                    db.session.add(domain_instance)
                    db.session.commit()
                    DOMAINS[row.domain] = domain_instance
                COLUMN_TO_DESCRIPTION[row.name] = clean_text_for_web(row.description)
                COLUMN_TO_LABEL[row.name] = clean_text_for_web(row.label)
                if row.values:  # these "categories" are really ITEMS
                    if re.match(r'\w{1,3}\s*\=', row.values.strip()):
                        i = models.Item(name=clean_text_for_web(row.label),
                                        description=clean_text_for_web(row.description),
                                        category=domain_instance.id)
                        for cat in row.values.split('||'):
                            order, value = re.split(r'[\=\s]+', cat, maxsplit=1)
                            value = value.strip().lower()  # standardize all values to lowercase
                            # get the categorization order
                            if order == '.' or value == 'missing':
                                logger.warning(f'Ignoring value for {row.name}: {cat} (assuming this is missing).')
                                continue
                            lookup_value = None
                            if '.' in order:  # handle an order with decimals (e.g., 1.5)
                                try:
                                    lookup_value = float(order)
                                except ValueError:
                                    logger.error(f'Failed to parse for {row.name} order "{order}" in "{cat}".')
                                    raise
                                else:
                                    order = int(lookup_value)
                            elif len(value) == 1 and value in string.ascii_lowercase:
                                try:
                                    order = int(order, 36)
                                except ValueError:
                                    logger.error(f'Failed to parse for {row.name} order "{order}" in "{cat}".')
                                    raise
                            else:
                                try:  # numbers first
                                    order = int(order)  # handle decimals
                                except ValueError:  # letters appear after numbers
                                    logger.error(f'Failed to parse for {row.name} order "{order}" in "{cat}".')
                                    raise

                            v = models.Value(name=value, order=order)
                            db.session.add(v)
                            # prefer lookup value so that 1.5 and 1 don't map to same order
                            VALUES_BY_ITEM[row.name][lookup_value or order] = v
                            if value[-1] in {'+', '-'} or value[0] in {'<', '>'} or '-' in value:
                                if '+' not in VALUES_BY_ITEM[row.name]:
                                    VALUES_BY_ITEM[row.name]['+'] = []  # for values greater than
                                try:
                                    func = create_function_for_range(value)
                                except ValueError:
                                    pass
                                else:
                                    if func is not None:
                                        VALUES_BY_ITEM[row.name]['+'].append((func, v))
                            db.session.commit()
                    else:
                        i = models.Item(name=clean_text_for_web(row.label),
                                        description=clean_text_for_web(row.description),
                                        category=domain_instance.id,
                                        is_numeric=True)
                    ITEMS[row.name] = i
                    db.session.add(i)
    logger.info(f'Committing updates to database.')
    db.session.commit()
    logger.info(f'Finished unpacking domains.')


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
    logger.info(f'Uploading data dictionary to database.')
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
    logger.info(f'Finished uploading data dictionary.')


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
                        help='Variable for years of presence in cohort (for graphing). Raw value will be used.')
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
    parser.add_argument('--skip-rounding', nargs='+', type=str.lower, default=set(),
                        help='Variables names (the column names, not display names) to skip rounding.')

    args, unk = parser.parse_known_args()

    app.config.from_pyfile(args.config)
    prepare_config(args.debug, args.whooshee_dir, skip_init=True)
    args = parser.parse_args()

    logger.add('load_csv_{time}.log', backtrace=True, diagnose=True)

    with app.app_context():
        logger.debug('Unpacking categories.')
        unpack_domains(args.categorization_csv, args.minimum_priority)
        datamodel_vars = {
            args.age_bl: 'age_bl',
            args.age_fu: 'age_fu',
            args.gender: 'gender',
            args.enrollment: 'enrollment',
            args.followup_years: 'followup_years'
        }
        if args.tab_file:
            logger.debug('Adding tabs from file.')
            add_tabs(args.tab_file)
        if args.comment_file:
            logger.debug('Adding comments from file.')
            add_comments(args.comment_file)
        logger.debug('Parsing CSV file.')
        parse_csv(args.csv_file, datamodel_vars, args.items_from_data_dictionary_only,
                  skip_rounding=set(args.skip_rounding) | {args.followup_years})

        if args.dd_input_file:
            # optionally generate and store the data dictionary
            logger.debug('Adding data dictionary.')
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
