import math

from pandas import to_numeric

from dqt_api.views import rounding
from dqt_load.utils import clean_number

from dqt_api import app


def create_function_for_range(value: str):
    """Create function that defines a particular value/expression

    E.g., income in range $10,000 - $20,000
    """
    # remove any problems for ints
    value = clean_number(value)
    to_numeric_func = float if '.' in value else int
    if value[-1] == '+':  # gte
        value = to_numeric_func(value.rstrip('+'))
        return lambda x: to_numeric_func(clean_number(x)) >= value
    elif value[-1] == '-':  # lte
        value = to_numeric_func(value.rstrip('-'))
        return lambda x: to_numeric_func(clean_number(x)) <= value
    elif value.startswith('>='):
        value = to_numeric_func(value[2:])
        return lambda x: to_numeric_func(clean_number(x)) >= value
    elif value.startswith('<='):
        value = to_numeric_func(value[2:])
        return lambda x: to_numeric_func(clean_number(x)) <= value
    elif value.startswith('>'):
        value = to_numeric_func(value[1:])
        return lambda x: to_numeric_func(clean_number(x)) > value
    elif value.startswith('<'):
        value = to_numeric_func(value[1:])
        return lambda x: to_numeric_func(clean_number(x)) < value
    elif '-' in value:  # assumes end is EXCLUSIVE
        v1, v2 = value.split('-')
        v1 = to_numeric(clean_number(v1))
        v2 = to_numeric(clean_number(v2))
        return lambda x: v1 <= to_numeric_func(clean_number(x)) < v2



def get_ranges(unique_values):
    if not unique_values:
        return None
    unique_values = [x[0] for x in unique_values]  # remove order element
    prev = None
    rsteps = []
    for el in sorted(unique_values):
        if prev:
            rsteps.append(el - prev)
        prev = el
    max_range = max(unique_values)
    if int(max_range) != max_range:
        max_range += 0.1
    if len(unique_values) == 1 or len(rsteps) == 0:  # all items have same value
        return None

    ranges = [min(unique_values), max_range, min(rsteps)]
    # increase step count if larger range
    if ranges[2] == 1 and ranges[1] - ranges[0] > 20:
        ranges = [rounding(ranges[0], 5, 0, 0), rounding(ranges[1], 5, 0, 1), 5]
    elif math.isclose(ranges[2], 0.1):
        if ranges[1] - ranges[0] > 10:
            ranges = [int(rounding(ranges[0], 5, 1, 0)), int(rounding(ranges[1], 5, 1, 1)), 1]
        else:
            ranges = [int(rounding(ranges[0], 5, 1, 0)), int(rounding(ranges[1], 5, 1, 1)), 0.1]

    # sometimes the above has trouble
    # TODO: write an appropriate rounding library
    ideal_rate = app.config.get('IDEAL_BUCKET_COUNT', 20)
    current_rounding = ranges[2] * 2
    segments = (ranges[1] - ranges[0]) / ranges[2]
    if segments > ideal_rate:
        if '.' in str(ranges[2]):
            if current_rounding > 1:
                current_rounding = int(current_rounding)
                rounded = int(
                    math.ceil((ranges[1] - ranges[0]) / ideal_rate / current_rounding) * current_rounding)
            else:
                zeroes = (str(ranges[2]).split('.')[1].count('0') + 1) * 10
                rounded = int(math.ceil((ranges[1] * zeroes - ranges[0] * zeroes) / ideal_rate / (
                        current_rounding * zeroes)) * current_rounding * zeroes) / zeroes

        else:
            rounded = int(math.ceil((ranges[1] - ranges[0]) / ideal_rate / current_rounding) * current_rounding)

        new_min = int(math.ceil((ranges[0]) / rounded)) * rounded
        if new_min > ranges[0]:
            new_min = int(math.ceil((ranges[0] - rounded) / rounded)) * rounded
        new_max = int(math.ceil((ranges[1]) / rounded)) * rounded
        ranges = [new_min, new_max, rounded]
    return ranges
