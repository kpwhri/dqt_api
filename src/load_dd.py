"""
Run this file to build data dictionary only
"""
from dqt_api import app
from dqt_api.__main__ import prepare_config
from load_csv import add_data_dictionary
from load_utils import parser
from loguru import logger

if __name__ == '__main__':
    parser.add_argument('--config', required=True,
                        help='File containing configuration information. '
                             'BASE_DIR, SECRET_KEY, SQLALCHEMY_DATABASE_URI, '
                             'etc.')
    args = parser.parse_args()

    app.config.from_pyfile(args.config)
    prepare_config()

    logger.add('load_data_dictionary_{time}.log', backtrace=True, diagnose=True)
    add_data_dictionary(
        args.dd_input_file,
        args.dd_file_name,
        args.dd_label_column,
        args.dd_name_column,
        args.dd_category_column,
        args.dd_description_column,
        args.dd_value_column,
    )
