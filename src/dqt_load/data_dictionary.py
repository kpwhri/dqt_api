import hashlib

from loguru import logger

from dqt_api import db, models
from dqt_load.excel import xlsx_to_list


def add_data_dictionary(input_files, file_name, label_column, name_column, category_col,
                        descript_col, value_column, **kwargs):
    """

    :param file_name:
    :param input_files: xls(x) files with columns specified below
    :param label_column: column with common name
    :param name_column: column with variable name
    :param category_col: if None, use sheet names as categories
    :param descript_col: column with description of the variable
    :param value_column: column that shows the various variables
    :param kwargs: n/a
    :return:
    """
    logger.info(f'Uploading data dictionary to database.')
    for input_file in input_files:
        if 'xls' in input_file.split('.')[-1]:
            if category_col:
                columns_to_keep = [label_column, name_column, descript_col, value_column, category_col]
            else:
                columns_to_keep = [label_column, name_column, descript_col, value_column]
            for label, name, descript, value, category in xlsx_to_list(
                    input_file,
                    columns_to_keep=columns_to_keep,
                    include_header=False,
                    append_sheet_name=None if category_col else 'Category'
            ):
                if name is None:
                    continue
                de = models.DataEntry(
                    label=label,
                    variable=name,
                    values=value,
                    description=descript,
                    category=category
                )
                db.session.add(de)
            with open(input_file, 'rb') as fh:
                txt = fh.read()
                df = models.DataFile(
                    filename=file_name,
                    file=txt,
                    md5_checksum=hashlib.md5(txt).hexdigest()
                )
                db.session.add(df)
        else:
            raise ValueError('Unrecognized file extension: {}'.format(input_file.split('.')[-1]))
    db.session.commit()  # commit all files together, simplifies re-running should an error occur
    logger.info(f'Finished uploading data dictionary.')

