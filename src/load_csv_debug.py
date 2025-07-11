"""Not really meant to be general purpose, but at least provides an example
of how to load data from csv into the database format.

Primary entry point is `unpack_categories` which unpacks, interprets, and
    loads the data into the database.

A second entry is `add_data_dictionary` which optionally uploads a data dictionary to
    the database. This can be accessed as a standalone module with `load_dd.py`.

"""
import datetime
from pathlib import Path

from loguru import logger

from dqt_api import app, db
from dqt_api import models
from dqt_api.__main__ import prepare_config
from dqt_api.manage import add_tabs, add_comments, create_with_context

from dqt_load.categories import unpack_domains
from dqt_load.data_dictionary import add_data_dictionary
from dqt_load.loader import parse_csv


def main():
    from dqt_load.argparser import parser  # data dictionary loading options
    parser.add_argument('--config', required=True,
                        help='File containing configuration information. '
                             'BASE_DIR, SECRET_KEY.')
    parser.add_argument('--debug', default=False, action='store_true',
                        help='Run in debug mode.')
    parser.add_argument('--testdb', action='store_true', default=False,
                        help='Set to use a random test database in BASE_DIR.')
    parser.add_argument('--target-columns', nargs='+', required=False, type=str.lower, default=None,
                        help='Only these columns will be targetted.')
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
    if args.testdb:
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + str(
            Path(app.config['BASE_DIR']) / f'{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
        )
    prepare_config(args.debug, args.whooshee_dir, skip_init=True)
    args = parser.parse_args()

    logger.add('load_csv_{time}.log', backtrace=True, diagnose=True)

    with app.app_context():
        if args.testdb:
            create_with_context()
        logger.debug('Unpacking categories.')
        datamodel_vars = {
            args.age_bl: 'age_bl',
            args.age_fu: 'age_fu',
            args.gender: 'gender',
            args.enrollment: 'enrollment',
            args.followup_years: 'followup_years'
        }
        target_columns = None
        if args.target_columns:
            target_columns = list(datamodel_vars.keys()) + args.target_columns
        unpack_domains(args.categorization_csv, target_columns, args.minimum_priority)
        if args.tab_file:
            logger.debug('Adding tabs from file.')
            add_tabs(args.tab_file)
        if args.comment_file:
            logger.debug('Adding comments from file.')
            add_comments(args.comment_file)
        logger.debug('Parsing CSV file.')
        parse_csv(args.csv_file, datamodel_vars, args.items_from_data_dictionary_only,
                  target_columns,
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
