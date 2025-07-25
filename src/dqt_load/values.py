from loguru import logger

from dqt_api import models, db
from dqt_api.views import rounding
from dqt_load.globals import VALUES_BY_ITEM, VALUES, ITEMS
from dqt_load.ranges import get_ranges
from dqt_load.utils import clean_text_for_web, transform_decimal


def add_values(cdf, col, item, lookup_col=None, graph_data=None, datamodel_vars: dict = None,
               item_range: tuple[int, int, int] = None, rounded: int = None):
    """
    Add all elements from `col` to database.

    lookup_col:

    If graph_data supplied, this variable is used in the data_model to control the two graphs and table.
    """
    if lookup_col is None or lookup_col not in cdf.columns:
        lookup_col = None  # no lookup column
    unique_values = set()  # collect all unique values
    is_valid_range = True  # check to see if the values could be part of a range
    filter_clause = [col, lookup_col] if lookup_col is not None else [col]
    for row in cdf[filter_clause].drop_duplicates().itertuples():
        lookup_value = getattr(row, lookup_col, None) if lookup_col else None
        value = getattr(row, col)
        original_value = value
        value_model = None

        # by the appropriate value model
        if lookup_value and lookup_value in VALUES_BY_ITEM[item]:  # predefined ids for categorical
            value_model = VALUES_BY_ITEM[item][lookup_value]
            is_valid_range = False  # categorical
        elif value in VALUES_BY_ITEM[item]:  # predefined ids for categorical
            value_model = VALUES_BY_ITEM[item][value]
            is_valid_range = False  # categorical
        elif '+' in VALUES_BY_ITEM[item]:  # other predefined ids for categorical
            for func, func_value in VALUES_BY_ITEM[item]['+']:
                if func(value):
                    value_model = func_value
            is_valid_range = False  # categorical
        # not predefined -- possibly int or float value
        if value_model is None:  # add value if it doesn't exist
            # convert value to int if possible, then:
            # check if this could be a valid range: always collect values to give to Item
            # once we collect all values, we'll try to determine ranges, etc.
            val = None
            try:
                val = int(value)
            except ValueError:
                pass
            if val is None:
                try:
                    val = rounding(float(value), 0.1, 1, 0)
                except ValueError:
                    pass
            if val is None:
                is_valid_range = False
                val = value
            value = val
            # I think lookup value is only used for things like Gender, etc.
            # if this is uncommented, it will fail to load teh appropriate variables in the `add variable` section below
            # if same_col:  # if we don't have a separate lookup column
            #     lookup_value = value

            if value in VALUES[item]:
                value_model = VALUES[item][value]
            else:
                value_model = models.Value(name=clean_text_for_web(value))
                logger.warning(f'Adding new value: {value} = {item}')
                db.session.add(value_model)
                db.session.commit()
                VALUES[item][value] = value_model

        # add to set of unique values for this item
        #  - we may get multiple due to rounding (e.g., 0.53, 0.54 -> 0.5)
        #  - rounded values should get same id
        unique_values.add((value_model.name_typed, value_model.order, value_model.id))

        # add 'variable': i.e., individual-level data which connects an item and its value
        variables = []
        if lookup_value is not None:
            mask = cdf[lookup_col] == lookup_value
        else:
            mask = cdf[col] == original_value
        for case in cdf[mask].index:
            # NOTE: I'm not sure the `cdf[col] == original_value` is adding anything, but requires
            #       tracking an additional value (`original_value`: what value was before possible conversion)
            if item in ITEMS:  # ensure this is in data dictionary
                variables.append(models.Variable(
                    case=case, item=ITEMS[item].id, value=value_model.id
                ))
            if graph_data:  # add to graph
                graph_data[case][datamodel_vars[item]] = value_model.name
        db.session.bulk_save_objects(variables)
        db.session.commit()

    # get the ranges for this item
    ranges = None  # specified ranges
    if item_range:
        ranges = item_range
    elif is_valid_range:
        ranges = get_ranges(unique_values)
    # set values or ranges for the item, to allow easy loading
    item_model = ITEMS[item]
    if is_valid_range and ranges:
        if '.' in str(ranges[0]):  # is float
            item_model.is_float = True
            item_model.float_range_start = transform_decimal(ranges[0])
            item_model.float_range_end = transform_decimal(ranges[1])
            item_model.float_range_step = transform_decimal(ranges[2])
        else:  # is int
            item_model.is_float = False
            item_model.int_range_start = int(ranges[0])  # otherwise, it's a numpy int
            item_model.int_range_end = int(ranges[1])
            item_model.int_range_step = int(ranges[2])
        item_model.is_loaded = True
        if rounded:
            item_model.rounded = rounded
    else:  # list of (name, order, value.id)
        values = '||'.join(str(x[2]) for x in sorted(unique_values, key=lambda x: x[1]))  # sort by order
        if len(values) < 500:  # field limit of 500 characters
            item_model.values = values
            item_model.is_loaded = True
    db.session.commit()
