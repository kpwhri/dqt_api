"""
Convert a data dictionary in XLSX form to CSV.

Example usage:
```
    xlsx-to-csv
    -i "DataDictionary.xlsx"
    -o "variable_prioritization.csv"
    --columns-to-keep "Variable description" "Variable name" "Variable label" "Categories"
    --append-sheet-name "Category"
    --ignore-empty-rows
    --join-multiline "||"
```
"""
from dqt_load.argparser import blank_parser
from dqt_load.excel import xlsx_to_csv


def main():
    parser = blank_parser()

    parser.add_argument('-i', '--input-file', dest='ifp', required=True,
                        help='Input xlsx file.')
    parser.add_argument('-o', '--output-file', dest='ofp', required=True,
                        help='Output csv file.')
    parser.add_argument('--columns-to-keep', nargs='+',
                        help='List of columns or indices to keep. Column names cannot be only numbers.')
    parser.add_argument('--columns-to-ignore', nargs='+',
                        help='List of columns or indices to ignore. Column names cannot be only numbers.')
    parser.add_argument('--do-not-ignore-case', action='store_false', default=True, dest='ignore_case',
                        help='Retain case of columns; only use if two columns are distinguished only by case.')
    parser.add_argument('--header-rows', type=int, default=1,
                        help='Number of header rows.')
    parser.add_argument('--append-sheet-name', default=None,
                        help='Name of column to include which labels worksheet of origin.')
    parser.add_argument('--ignore-empty-rows', default=False, action='store_true',
                        help='Ignore rows that are empty or only contain space/control characters.')
    parser.add_argument('--join-multiline', default=None,
                        help='Character to replace newline characters with')
    parser.add_argument('--encoding', default='utf8',
                        help='Define character encoding for creating the data dictionary csv file.')
    xlsx_to_csv(**vars(parser.parse_args()))


if __name__ == '__main__':
    main()
