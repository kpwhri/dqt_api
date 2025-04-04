"""Not really meant to be general purpose, but at least provides an example
of how to load data from csv into the database format.

Primary entry point is `unpack_categories` which unpacks, interprets, and
    loads the data into the database.

A second entry is `add_data_dictionary` which optionally uploads a data dictionary to
    the database. This can be accessed as a standalone module with `load_dd.py`.

"""
import asyncio
import csv
import hashlib

import aiocsv
import aiofiles
from loguru import logger
import re
from collections import defaultdict, namedtuple
from datetime import datetime
import sqlalchemy as sqla
from pandas import to_numeric

from load_csv_pandas import add_items, add_categories, line_not_empty, unpack_domains, \
    add_data_dictionary
from dqt_api.rounding import int_round, int_floor
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


async def parse_csv(fp, datamodel_vars, items_from_data_dictionary_only):
    """
    Load csv file into database, committing after each case.
    :param datamodel_vars:
    :param items_from_data_dictionary_only:
    :param fp: path to csv file
    :return:
    """
    curr_year = int(str(datetime.now().year)[-2:])
    if not DOMAINS:
        add_categories()
    logger.info(f'Parsing variables from csv file: {fp}')
    async with aiofiles.open(fp, newline='') as fh:
        reader = aiocsv.AsyncReader(fh)
        first_line = await reader.__anext__()
        # make sure all items/variables already added to database
        items = add_items([x.lower() for x in first_line], datamodel_vars)
        i = 0
        async for line in reader:
            i += 1
            if line_not_empty(line):
                await load_line_to_db(curr_year, datamodel_vars, i, items, items_from_data_dictionary_only, line)

    logger.info(f'Done! Finished loading variables.')


async def load_line_to_db(curr_year, datamodel_vars, i, items, items_from_data_dictionary_only, line):
    add_to_db_list = []
    graph_data = defaultdict(lambda: None)  # separate summary data table
    for j, value in enumerate(line):  # for each variable in this row
        if items[j].excluded:
            logger.debug(f'Missing column #{j}: {items[j].variable} ({value.strip()})')
            continue
        if not value.strip() or value == '.':  # empty/missing value: exclude
            continue
        value = value.lower()  # standardize to lowercase
        if value == 'missing':
            continue
        curr_item = items[j].variable
        # pre-processing values
        if items[j].has_date and len(value) >= 6:
            # convert date to year
            if re.match(r'(\d{2}\w{3}\d{4}|\d{1,2}\/\d{1,2}\/\d{4})', value):
                value = str(int_floor(value[-4:]))
            elif re.match(r'(\d{2}\w{3}\d{2}|\d{1,2}\/\d{1,2}\/\d{2})', value):
                value = int_floor(value[-2:])
                if value <= curr_year:
                    value = '20{}'.format(value)
                else:
                    value = '19{}'.format(value)
        elif items[j].has_age_year:
            try:
                value = str(int_floor(value))
            except ValueError:
                pass

        # get the Value model itself
        new_value = None
        if curr_item in VALUES_BY_ITEM:  # categorization/ordering already assigned
            lookup_value = None  # value as it appears in VALUES_BY_ITEM (e.g., 1.5)
            try:
                value_as_order = int(value)
            except ValueError:
                if '.' in value:  # handle category of, e.g,. 1.5
                    try:
                        lookup_value = float(value_as_order)
                    except ValueError:
                        pass
                    else:
                        value_as_order = int(lookup_value)

                if len(value) == 1:
                    value_as_order = int(value, 36)  # if letters were used
                else:
                    value_as_order = None
            if value_as_order is None:  # no categorization/numeric found, maybe string value?
                for val in VALUES_BY_ITEM[curr_item].values():
                    if isinstance(val, list):
                        continue  # handle '+'
                    if val.name == value:
                        new_value = val
                        break
                if not new_value:  # found a new category which expected to be defined
                    # perhaps this is in a range
                    for func, func_value in VALUES_BY_ITEM[curr_item]['+']:
                        if func(value_as_order):
                            new_value = func_value
                    if not new_value:
                        # not sure...let's log and add as extra var
                        logger.warning(f'No categorization found for `{curr_item}`: "{value}"')
                        order = max((x for x in VALUES_BY_ITEM[curr_item].keys() if isinstance(x, (int, float))),
                                    default=123) + 1
                        logger.info(f'Adding new category to `{curr_item}`: "{value}" with order {order}')
                        new_value = models.Value(name=value, order=order)
                        VALUES[curr_item][value] = new_value
                        add_to_db_list.append(new_value)
                        VALUES_BY_ITEM[curr_item][order] = new_value
            else:  # found int value
                if lookup_value and lookup_value in VALUES_BY_ITEM[curr_item]:
                    # handles orders with float values
                    new_value = VALUES_BY_ITEM[curr_item][lookup_value]
                elif value_as_order in VALUES_BY_ITEM[curr_item]:
                    new_value = VALUES_BY_ITEM[curr_item][value_as_order]
                elif '+' in VALUES_BY_ITEM[curr_item]:
                    for func, func_value in VALUES_BY_ITEM[curr_item]['+']:
                        if func(value_as_order):
                            new_value = func_value
        elif items_from_data_dictionary_only:
            continue  # skip if user only wants values from data dictionary
        # don't include this as else because if-clause needs to go here
        if not new_value:  # add value if it doesn't exist
            if value not in VALUES[curr_item]:
                val = models.Value(name=value)
                logger.warning(f'Adding new value: {value} = {curr_item}')
                VALUES[curr_item][value] = val
                add_to_db_list.append(val)
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
        add_to_db_list.append(var)
    logger.debug('Cohort data: {}'.format(str(graph_data)))
    add_to_db_list.append(models.DataModel(case=i,
                                           age_bl=graph_data['age_bl'],
                                           age_fu=graph_data['age_fu'],
                                           sex=graph_data['gender'],
                                           enrollment=graph_data['enrollment'],
                                           followup_years=int_round(graph_data['followup_years'], 1),
                                           ))
    db.session.bulk_save_objects(add_to_db_list)
    db.session.commit()


def main():
    logger.warning(f'This script is not supported! Prefer `load_csv_pandas.csv`! '
                   f'Only use this script if data is too big for in memory.')

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
        asyncio.run(parse_csv(args.csv_file, datamodel_vars, args.items_from_data_dictionary_only))

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
