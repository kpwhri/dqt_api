import argparse


def blank_parser(config=False):
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@!')
    if config:
        parser = add_config(parser)
    return parser


def data_dictionary_parser(config=True):
    parser = blank_parser(config)
    # data dictionary loading options
    parser.add_argument('--dd-input-file', nargs='+',
                        help='Path to excel document(s) containing data dictionary.'
                             ' Values are expected to be contained in the first table'
                             ' in the file.')
    parser.add_argument('--dd-file-name', default='data-dictionary.xlsx',
                        help='Displayed filename for downloads of the data dictionary')
    parser.add_argument('--dd-label-column', required=True,
                        help='Name of label column in document; the label is the common name')
    parser.add_argument('--dd-category-column',
                        help='Name of category column in document; if you want to use '
                             'excel tabs with categories, do not include this option')
    parser.add_argument('--dd-name-column',
                        help='Name of variable name column in document; variable name as opposed to common name')
    parser.add_argument('--dd-description-column',
                        help='Name of description column in document; should discuss details of the variable')
    parser.add_argument('--dd-value-column',
                        help='Name of values column in document; what the possible values are for the label')
    return parser


def load_parser(config=True):
    """Data dictionary and regular arguments"""
    parser = data_dictionary_parser(config)

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
    # specify mappings for data model variables that aren't otherwise loaded
    parser.add_argument('--enrollment-mapping', default=None,
                        help='Specify mapping with 1==still_enrolled&2==current '
                             '(underscores will be converted to spaces)')
    parser.add_argument('--gender-mapping', default=None,
                        help='Specify mapping with 1==male&2==female '
                             '(underscores will be converted to spaces)')

    # debugging options
    parser.add_argument('--testdb', action='store_true', default=False,
                        help='Set to use a random test database in BASE_DIR.')
    parser.add_argument('--target-columns', nargs='+', required=False, type=str.lower, default=None,
                        help='Only these columns will be targetted.')
    return parser


def add_config(parser=None):
    if not parser:
        parser = blank_parser()
    parser.add_argument('--config', required=True,
                        help='File containing configuration information. '
                             'BASE_DIR, SECRET_KEY, SQLALCHEMY_DATABASE_URI, '
                             'etc.')
    return parser
