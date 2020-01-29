"""
Run this file to build data dictionary only
"""
from load_csv import add_data_dictionary
from load_utils import parser
from loguru import logger


if __name__ == '__main__':
    args = parser.parse_args()
    logger.add('load_data_dictionary_{time}.log')
    add_data_dictionary(
        args.dd_input_file,
        args.dd_file_name,
        args.dd_label_column,
        args.dd_name_column,
        args.dd_category_column,
        args.dd_description_column,
        args.dd_value_column,
    )
