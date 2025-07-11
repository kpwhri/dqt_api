def round_top_and_bottom(cdf, col):
    """
    must round so that the filters find the lowest and highest at a round number,
        otherwise, filters might show range of 1992 - 2027 by 5s instead of 1990 - 2025.
    """
    cdf[col] = cdf[col].apply(lambda x: int_floor(x) if x == cdf[col].min() else x)
    cdf[col] = cdf[col].apply(lambda x: int_ceil(x) if x == cdf[col].max() else x)
    return cdf


def int_round(x, base=5):
    """Round a number to the nearest 'base' """
    return int(base * round(float(x) / base))


def int_only(x):
    """Convert to float, then round to nearest 1."""
    return int_round(x, base=1)


def int_ceil(x, base=5):
    """Round number up to the nearest 'base' """
    return int(float(x) + (base - float(x) % base) % 5)


def int_floor(x, base=5):
    """Round number down to the nearest 'base' """
    return int(float(x) - (float(x) % base))


def int_mid(x, base=5):
    """
    For rounding to still respect filter ranges, a midpoint must be selected.
    Suppose the slider bar allows 0,5,10,15,... and we have ages 14,10,8, and 5.
        If we `int_floor` only:
            14 -> 10
            10 -> 10
            8 -> 5
            5 -> 5
        And the user selects the range [0, 5] we will get both 5s (one of which is actually an 8).
    To avoid this, we'll add half the base back.
    We must keep track of the ages 5 and 10 because we don't want them to fall in the middle of the range:
        If we `int_mid(n, base=5)` without checking the prior value:
            14 -> 12
            10 -> 12
            8 -> 7
            5 -> 7
        Now the range [5, 10] will include 5, 8 but not 10 (!) since we incremented it.

    NOTE: This will not work if base=1, but that's probably expected since we're clearly int-rounding.
    """
    new_x = int_floor(x, base=base)  # get the floor
    x = int(float(x))  # ensure same time (i.e., not a string)
    if new_x != x:  # increment if not a range edge/border value which must remain on the edge
        new_x += base // 2  # get the integer midpoint to set value in the middle of the range
    return new_x
