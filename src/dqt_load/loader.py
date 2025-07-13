import string

import pandas as pd
from loguru import logger
import sqlalchemy as sa

from dqt_api import models, db
from dqt_load.rounding import round_top_and_bottom, int_only, int_mid
from dqt_load.categories import add_categories
from dqt_load.data_model import save_data_model
from dqt_load.globals import DOMAINS, VALUES_BY_ITEM
from dqt_load.items import add_items
from dqt_load.utils import clean_text_for_web
from dqt_load.values import add_values


def parse_csv(fp, datamodel_vars,
              items_from_data_dictionary_only, target_columns=None,
              skip_rounding: set[str] = None):
    """
    Load csv file into database, committing after each case.
    :param datamodel_vars:
    :param items_from_data_dictionary_only:
    :param fp: path to csv file
    :param skip_rounding: set of columns names (i.e., variable names)
    :return:
    """
    if not DOMAINS:
        add_categories()
    logger.info(f'Parsing variables from csv file: {fp}')
    df = pd.read_csv(fp, dtype=object)
    graph_data = {i: dict() for i in range(df.shape[0])}
    df.columns = [c.lower() for c in df.columns]
    datamodel_cols = list(datamodel_vars.keys())
    if target_columns:
        # avoid duplicating datamodel columns if one of these is requested
        columns = [col for col in target_columns if col not in datamodel_cols]
        df = df[datamodel_cols + columns]
    else:
        columns = [col for col in df.columns if col not in datamodel_cols]
    items = add_items(df.columns, datamodel_vars)  # ensure items exist
    sentinel = object()
    col_number = 0
    for col in datamodel_cols + [sentinel] + columns:
        if col == sentinel:
            save_data_model(graph_data)
            graph_data = None
            continue
        col_number += 1
        if items[col].excluded:
            logger.debug(f'Missing column #{col_number} {col}: {items[col].variable}')
            continue

        logger.info(f'Processing column #{col_number}: {col}.')
        curr_item = items[col].variable
        # remove missing data
        cdf = pd.DataFrame(df[col].dropna().str.lower())
        cdf = cdf[~cdf[col].isin({'.', '', 'missing'})]
        # handle date: convert to year
        item_range = None
        if items[col].has_date:
            if col in skip_rounding:
                cdf = pd.DataFrame(pd.to_datetime(cdf[col]).dt.year.apply(int_only))
                logger.warning(f'Skipping rounding for column with date: {col}')
            else:
                cdf = pd.DataFrame(pd.to_datetime(cdf[col]).dt.year.apply(int_mid))
                # must round so that the filters find the lowest and highest at a round number
                cdf = round_top_and_bottom(cdf, col)
                item_range = [cdf[col].min(), cdf[col].max(), 5]  # default step is 5
            add_values(cdf, col, curr_item, graph_data=graph_data, datamodel_vars=datamodel_vars, item_range=item_range)
        elif items[col].has_age_year or items[col].has_years:
            if col in skip_rounding:  # these are not actually identifying years/ages
                cdf = pd.DataFrame(cdf[col].apply(int_only))
                logger.warning(f'Skipping rounding for column with age/year: {col}')
            else:
                cdf = pd.DataFrame(cdf[col].apply(int_mid))
                # must round so that the filters find the lowest and highest at a round number
                cdf = round_top_and_bottom(cdf, col)
                item_range = [cdf[col].min(), cdf[col].max(), 5]  # default step is 5
            add_values(cdf, col, curr_item, graph_data=graph_data, datamodel_vars=datamodel_vars, item_range=item_range)
        elif curr_item in VALUES_BY_ITEM:  # categorisation/ordering already assigned
            values = cdf[col].unique()
            mapping = {}
            lookup_mapping = {}
            for value in values:
                if '.' in value:
                    mapping[value] = float(value)
                    lookup_mapping[value] = int(mapping[value])
                    continue
                elif len(value) == 1 and value in string.ascii_lowercase:
                    mapping[value] = int(value, 36)
                    lookup_mapping[value] = mapping[value]
                    continue
                else:
                    try:
                        mapping[value] = int(value)
                        lookup_mapping[value] = mapping[value]
                    except ValueError:
                        pass
                    else:
                        continue
                for possible_value in VALUES_BY_ITEM[curr_item].values():
                    if isinstance(possible_value, list):
                        continue  # skip '+'
                    if possible_value.name == value:
                        mapping[value] = possible_value
                        lookup_mapping[value] = possible_value
                        break
                else:  # if no value found, look in the '+' section
                    if '+' in VALUES_BY_ITEM[curr_item]:
                        for func, func_value in VALUES_BY_ITEM[curr_item]['+']:
                            if func(value):
                                # no change, func_value is models.Value which can't be fed into the `add_values` func
                                mapping[value] = value
                                lookup_mapping[value] = value
                                break
                    if value not in mapping:  # not sure what happened, let's log and add an extra variable
                        logger.warning(f'No categorization found for `{curr_item}`: "{value}"')
                        order = max((x for x in VALUES_BY_ITEM[curr_item].keys() if isinstance(x, (int, float))),
                                    default=123) + 1
                        logger.info(f'Adding new category to `{curr_item}`: "{value}" with order {order}')
                        value_model = models.Value(name=clean_text_for_web(value), order=order)
                        db.session.add(value_model)
                        VALUES_BY_ITEM[curr_item][order] = value_model
                        mapping[value] = order
                        lookup_mapping[value] = order
            cdf[f'{col}_temp'] = cdf[col].apply(lambda x: mapping[x])
            cdf[f'{col}_lkp'] = cdf[col].apply(lambda x: lookup_mapping[x])
            cdf[col] = cdf[f'{col}_temp']
            del cdf[f'{col}_temp']
            add_values(cdf, col, curr_item, lookup_col=f'{col}_lkp',
                       graph_data=graph_data, datamodel_vars=datamodel_vars)
        elif items_from_data_dictionary_only:
            continue  # skip if user only wants values from data dictionary
        else:
            # these are values that need to be added directly:
            # 1. integers/floats (these will all be converted to float by models.Value
            # 2. strings without an order (these will be left as strings, without order)
            add_values(cdf, col, curr_item, graph_data=graph_data, datamodel_vars=datamodel_vars)

        try:
            db.session.commit()  # commit each column separately
        except sa.exc.IntegrityError as e:
            logger.exception('Likely duplicate primary key: drop table and re-run.', e)

        logger.info(f'Finished column #{col_number}')
    logger.info(f'Done! Finished loading variables.')

