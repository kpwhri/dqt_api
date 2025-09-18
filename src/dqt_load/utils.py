# noinspection PyUnusedImports
from dqt_api.utils import clean_text_for_web  # migrated to dqt_api to avoid any dependencies on dqt_load


def clean_number(value):
    """Remove spaces and dollar signs to convert to numeric"""
    if isinstance(value, (int, float)):
        return value
    return value.replace(',', '').replace('$', '').strip()


def line_not_empty(lst):
    """Check if list is not empty and first value is not empty"""
    return bool(lst and lst[0])


def transform_decimal(num):
    res = str(num)
    if '.' in res:
        return res[:res.index('.') + 2]
    return res
