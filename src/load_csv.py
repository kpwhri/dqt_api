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
from dqt_api.manage import add_tabs, add_comments, create_with_context, create_user_data_with_context

from dqt_load.categories import unpack_domains
from dqt_load.data_dictionary import add_data_dictionary
from dqt_load.loader import parse_csv


def main():
    from dqt_load.argparser import load_parser  # data dictionary loading options

    parser = load_parser()
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
            create_user_data_with_context()
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
