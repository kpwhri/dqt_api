"""
Run this file to build data dictionary only
"""
from dqt_api import app
from dqt_api.__main__ import prepare_config
from loguru import logger

from dqt_load.argparser import data_dictionary_parser
from dqt_load.data_dictionary import add_data_dictionary


def main():
    parser = data_dictionary_parser()
    args = parser.parse_args()

    app.config.from_pyfile(args.config)
    prepare_config()

    logger.add('load_data_dictionary_{time}.log', backtrace=True, diagnose=True)
    with app.app_context():
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
