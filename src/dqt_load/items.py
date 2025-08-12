from collections import namedtuple

from loguru import logger

from dqt_load.globals import COLUMN_TO_LABEL, ITEMS, COLUMN_TO_DESCRIPTION, COLUMN_TO_DOMAIN, DOMAINS
from dqt_api import db
from dqt_api import models

ItemVar = namedtuple('ItemVar', 'variable excluded has_date has_age_year has_years has_age')


def add_items(items, datamodel_vars, use_desc=False):
    """
    Add items to database along with their descriptions, and commit.

    :param datamodel_vars: dict of variables stored in data_model table
    :param use_desc: use variables that end in '_desc'
        these already include a descriptive value rather than a numeric reference to
        the value
    :param items: label to display for each item
    :return:
    """
    res = {}

    def add(it, excluded=False):
        # if 'age' or 'yr' in variable name, set these to be rounded down to nearest 5 (e.g., 'age_bl': 94 -> 90)
        has_age_year = 'age' in it or 'yr' in it or 'year' in it
        has_age = 'age' in it
        # if year is plural, suggests range (e.g., followup_years '19.1' -> 19), so allow it to be more precise
        has_years = ('yr' in it and 'yrs' in it) or ('year' in it and 'years' in it)
        # if year is date, extract year-only and round down to nearest 5 (death_yr 1999 -> 1995)
        has_date = 'dt' in it or 'date' in it
        res[item] = ItemVar(it, excluded=excluded, has_date=has_date, has_age=has_age,
                            has_age_year=has_age_year, has_years=has_years)

    desc = {i[:-5] for i in items if i.endswith('_desc')}
    for item in items:
        has_desc = False  # HACK: certain labels I only want when they end in "_desc"
        if item.endswith('_desc'):
            if use_desc:
                item = item[:-5]
                has_desc = True
            else:  # skip if not using descending
                add(item, excluded=True)
                continue
        if item in COLUMN_TO_LABEL and (item not in desc or has_desc or not use_desc):
            # item = COLUMN_TO_LABEL[item]
            add(item, excluded=False)
            if item not in ITEMS:
                i = models.Item(name=COLUMN_TO_LABEL[item],
                                description=COLUMN_TO_DESCRIPTION[item],
                                category=DOMAINS[COLUMN_TO_DOMAIN[item]].id)
                ITEMS[item] = i
                db.session.add(i)
        elif item in datamodel_vars:
            add(item, excluded=False)
        elif item in desc:  # desc version chosen
            add(item, excluded=True)
        else:
            logger.warning(f'Variable not marked for inclusion: "{item}".')
            add(item, excluded=True)
    db.session.commit()
    return res
