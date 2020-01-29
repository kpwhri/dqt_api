"""
Utilities copied from pycronkd library (last updated: 2020-01-23)
"""

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
