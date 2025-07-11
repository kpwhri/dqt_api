import re
import string

from loguru import logger

from dqt_load.categorization_reader import CategorizationReader
from dqt_load.globals import DOMAINS, VALUES_BY_ITEM, ITEMS, COLUMN_TO_DOMAIN, COLUMN_TO_DESCRIPTION, COLUMN_TO_LABEL

from dqt_api import db
from dqt_api import models
from dqt_load.ranges import create_function_for_range
from dqt_load.utils import clean_text_for_web


def add_categories():
    """
    Add categories to database based on the specification in
    'column_to_category' global variable.
    :return:
    """
    for cat in set(COLUMN_TO_DOMAIN.values()):
        c = models.Category(name=cat)
        DOMAINS[cat] = c
        db.session.add(c)
    db.session.commit()


def unpack_domains(categorization_csv, target_columns=None, min_priority=0):
    """
    Primary entry point to move categories, items, and values into the database
    :param categorization_csv:
        columns:
            * Domain: domain of the variable
            * Variable description: short description of variable for hover text
            * Variable name: program shorthand to reference variable
            * Variable label: human-readable display label for the variable
            * Values: values separated by '=='
                e.g., 1=male||2=female
            * Priority (optional): number showing importance of variable (higher is more important)
    :param min_priority: limit the priority by only allowing >= priorities; allow all=0
    :return:
    """
    logger.info(f'Unpacking domains from the categorisation CSV file.')
    with CategorizationReader(categorization_csv) as rows:
        for row in rows:
            if target_columns and row.name not in target_columns and row.label not in target_columns:
                continue  # skip rows not part of debugging
            if row.priority >= min_priority:
                COLUMN_TO_DOMAIN[row.name] = row.domain
                if row.domain in DOMAINS:
                    domain_instance = DOMAINS[row.domain]
                else:  # not yet added
                    logger.info(f'Adding variables from domain {row.domain}')
                    domain_instance = models.Category(name=row.domain, order=len(DOMAINS))
                    db.session.add(domain_instance)
                    db.session.commit()
                    DOMAINS[row.domain] = domain_instance
                COLUMN_TO_DESCRIPTION[row.name] = row.description  # already cleaned
                COLUMN_TO_LABEL[row.name] = row.label
                if row.values:  # these "categories" are really ITEMS
                    if re.match(r'\w{1,3}\s*\=', row.values.strip()):
                        i = models.Item(name=row.label,
                                        description=row.description,
                                        category=domain_instance.id)
                        for cat in row.values.split('||'):
                            order, value = re.split(r'[\=\s]+', cat, maxsplit=1)
                            value = value.strip().lower()  # standardize all values to lowercase
                            # get the categorization order
                            if order == '.' or value == 'missing':
                                logger.warning(f'Ignoring value for {row.name}: {cat} (assuming this is missing).')
                                continue
                            lookup_value = None
                            if '.' in order:  # handle an order with decimals (e.g., 1.5)
                                try:
                                    lookup_value = float(order)
                                except ValueError:
                                    logger.error(f'Failed to parse for {row.name} order "{order}" in "{cat}".')
                                    raise
                                else:
                                    order = int(lookup_value)
                            elif len(value) == 1 and value in string.ascii_lowercase:
                                try:
                                    order = int(order, 36)
                                except ValueError:
                                    logger.error(f'Failed to parse for {row.name} order "{order}" in "{cat}".')
                                    raise
                            else:
                                try:  # numbers first
                                    order = int(order)  # handle decimals
                                except ValueError:  # letters appear after numbers
                                    logger.error(f'Failed to parse for {row.name} order "{order}" in "{cat}".')
                                    raise

                            v = models.Value(name=clean_text_for_web(value), order=order)
                            db.session.add(v)
                            # prefer lookup value so that 1.5 and 1 don't map to same order
                            VALUES_BY_ITEM[row.name][lookup_value or order] = v
                            if value[-1] in {'+', '-'} or value[0] in {'<', '>'} or '-' in value:
                                if '+' not in VALUES_BY_ITEM[row.name]:
                                    VALUES_BY_ITEM[row.name]['+'] = []  # for values greater than
                                try:
                                    func = create_function_for_range(value)
                                except ValueError:
                                    pass
                                else:
                                    if func is not None:
                                        VALUES_BY_ITEM[row.name]['+'].append((func, v))
                            db.session.commit()
                    else:
                        i = models.Item(name=clean_text_for_web(row.label),
                                        description=clean_text_for_web(row.description),
                                        category=domain_instance.id,
                                        is_numeric=True)
                    ITEMS[row.name] = i
                    db.session.add(i)
    logger.info(f'Committing updates to database.')
    db.session.commit()
    logger.info(f'Finished unpacking domains.')
