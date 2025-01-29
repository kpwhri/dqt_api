import csv
from _collections_abc import Iterable

from openpyxl import load_workbook


def _unpack_columns(columns, ignore_case):
    indices = []
    named = []
    for col in columns or []:
        try:
            indices.append(int(col))
            continue
        except ValueError:
            pass
        if ignore_case:
            named.append(col.lower())
        else:
            named.append(col)
    return indices, named


def xlsx_to_list(fp, columns_to_keep=None, columns_to_ignore=None, ignore_case=True, header_rows=1,
                 append_sheet_name='SheetName', include_header=True, ignore_empty_rows=False):
    """

    :param fp:
    :param columns_to_keep:
    :param columns_to_ignore:
    :param ignore_case:
    :param header_rows:
    :param append_sheet_name:
        * sheet name pre-pended to list
        * unless columns_to_keep in which case appended to the end
    :param include_header:
    :param ignore_empty_rows:
    :return: ordered according to original excel file, unless columns_to_keep specified
    """
    if columns_to_keep:
        idxs, cols = _unpack_columns(columns_to_keep, ignore_case)
    else:
        idxs, cols = _unpack_columns(columns_to_ignore, ignore_case)
    res_col_to_idx = {}
    res_header = []
    # set up ordering when specifying the columns to keep
    if columns_to_keep:
        for i, name in enumerate(columns_to_keep):
            res_col_to_idx[name.lower() if ignore_case else name] = i
            res_header.append(name)
    if append_sheet_name:
        res_col_to_idx[append_sheet_name] = len(res_col_to_idx)
        if append_sheet_name not in res_header:  # not columns_to_keep
            res_header.append(append_sheet_name)
    wb = load_workbook(fp)
    res = []
    for ws_name in wb.get_sheet_names():
        column_indexes_keep = {}
        ws = wb.get_sheet_by_name(ws_name)
        for i, row in enumerate(ws):
            if header_rows and i < header_rows:  # header
                for j, col in enumerate(row):
                    if not col.value:
                        continue
                    identified_col = (j in idxs
                                      or col.value in cols
                                      or (col.value.lower() in cols and ignore_case)
                                      )
                    if (identified_col and columns_to_keep) or (not identified_col and columns_to_ignore):
                        col_name = col.value.lower() if ignore_case else col.value  # get column name
                        if col_name not in res_col_to_idx:  # already set for columns_to_keep
                            res_col_to_idx[col_name] = len(res_col_to_idx)
                            res_header.append(col.value)  # header retains original case
                        column_indexes_keep[j] = res_col_to_idx[col_name]

            else:  # not header row
                res_col = [''] * len(res_col_to_idx)
                for j, col in enumerate(row):
                    if j in column_indexes_keep:
                        res_col[column_indexes_keep[j]] = col.value
                if ignore_empty_rows:
                    if not [x for x in res_col if x and str(x).strip()]:
                        continue
                if append_sheet_name:
                    res_col[res_col_to_idx[append_sheet_name]] = ws_name
                res.append(res_col)
    if include_header:
        res.insert(0, res_header)
    return res


def xlsx_to_csv(ifp, ofp, columns_to_keep=None, columns_to_ignore=None, ignore_case=True, header_rows=1,
                append_sheet_name='SheetName', ignore_empty_rows=False, join_multiline=None, encoding='utf8'):
    """

    :param ifp:
    :param ofp:
    :param columns_to_keep:
    :param columns_to_ignore:
    :param ignore_case:
    :param header_rows:
    :param append_sheet_name:
    :param ignore_empty_rows:
    :param join_multiline: character to replace newline characters with
    :return:
    """
    rows = xlsx_to_list(ifp, columns_to_keep, columns_to_ignore, ignore_case, header_rows, append_sheet_name,
                        include_header=True, ignore_empty_rows=ignore_empty_rows)
    iterable_to_csv(rows[1:], ofp, rows[0], join_multiline=join_multiline, encoding=encoding)


def iterable_to_csv(iterable, csvfile, header=None, col_order=None, join_multiline=None, encoding='utf8', **kwargs):
    """
    Convert an input iterable to a csv file.

    Handles lists of lists, dictionary, and even basic input.

    :param join_multiline: character to replace newlines with
    :param iterable: list or dict, both can contain lists for the columns
    :param csvfile: output csv filename
    :param header: variable to include as header, skip header if none
    :param col_order: specify column order for embedded dict
    :param kwargs: arguments to pass to call to csv.writer
    :return:]
    """
    with open(csvfile, 'w', encoding=encoding, newline='') as out:
        writer = csv.writer(out, **kwargs)
        if header:
            writer.writerow(header)
        for row in iterable:
            if join_multiline:
                row = [join_multiline.join(x.split('\n')) for x in row]
            if is_iterable(row) and not isinstance(iterable, dict):
                writer.writerow(row)
            elif isinstance(iterable, dict):
                # embedded dictionary
                if isinstance(iterable[row], dict):
                    if col_order:
                        cols = [row]
                        for col in col_order:
                            if col in iterable[row]:
                                cols.append(iterable[row][col])
                        writer.writerow(cols)
                    elif set(header) & set(iterable[row].keys()):
                        cols = [row]
                        for col in header:
                            if col in iterable[row]:
                                cols.append(iterable[row][col])
                        writer.writerow(cols)
                    else:
                        writer.writerow([row] + list(iterable[row]))
                # for list: match each value in list with key as a separate row
                elif is_iterable(iterable):
                    for item in iterable[row]:
                        if is_iterable(item):
                            writer.writerow([row] + list(item))
                        else:
                            writer.writerow([row, item])
                else:
                    writer.writerow([row, iterable[row]])
            else:
                writer.writerow([row])
    return True


def is_iterable(obj):
    """Is iterable, but isn't a string"""
    return not isinstance(obj, str) and isinstance(obj, Iterable)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')

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
