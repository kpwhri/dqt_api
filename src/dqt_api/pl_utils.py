import math
from collections import Counter
from itertools import zip_longest

import polars as pl

from dqt_api import db, app, models


def chunker(iterable, chunk_size, fillvalue=None):
    return zip_longest(*[iter(iterable)] * chunk_size, fillvalue=fillvalue)


def load_cases_to_polars(cases):
    """Load DataModel rows for the given case ids into a Polars DataFrame efficiently."""
    # Cast to a concrete list of ints (cases may be a set)
    case_ids = list(cases or [])
    if not case_ids:
        return pl.DataFrame({
            'case': pl.Series([], dtype=pl.Int64),
            'age_bl': pl.Series([], dtype=pl.Int64),
            'age_fu': pl.Series([], dtype=pl.Int64),
            'sex': pl.Series([], dtype=pl.Utf8),
            'enrollment': pl.Series([], dtype=pl.Utf8),
            'followup_years': pl.Series([], dtype=pl.Int64),
        })

    dfs = []
    # Only the columns we actually use later:
    schema = {
        'case': pl.Int64,
        'age_bl': pl.UInt8,
        'age_fu': pl.UInt8,
        'sex': pl.Utf8,
        'enrollment': pl.Utf8,
        'followup_years': pl.UInt8,
    }
    # Chunk the IN clause to avoid DB parameter limits
    for case_chunk in chunker(case_ids, 2000):
        chunk = [c for c in case_chunk if c is not None]
        if not chunk:
            continue
        q = (
            db.session.query(
                models.DataModel.case,
                models.DataModel.age_bl,
                models.DataModel.age_fu,
                models.DataModel.sex,
                models.DataModel.enrollment,
                models.DataModel.followup_years,
            )
            .filter(models.DataModel.case.in_(chunk))
            .yield_per(10000)  # stream rows from DB cursor
        )
        # Build rows directly as dicts (no ORM instance hydration)
        rows = (
            {
                'case': r.case,
                'age_bl': r.age_bl,
                'age_fu': r.age_fu,
                'sex': r.sex,
                'enrollment': r.enrollment,
                'followup_years': r.followup_years,
            }
            for r in q
        )
        # Materialize this chunk (Polars needs a concrete sequence)
        rows_list = list(rows)
        if rows_list:
            dfs.append(pl.DataFrame(rows_list, schema=schema))

    if not dfs:
        return pl.DataFrame(schema=schema)

    df = pl.concat(dfs, how='vertical', rechunk=True).with_columns(
        [
            pl.col('sex').cast(pl.Categorical),
            pl.col('enrollment').cast(pl.Categorical),
        ]
    )
    return df


def get_sex_by_age_pl(age_var, age_buckets, age_max, age_min, age_step, df, jitter_function, mask_value):
    sex_data = {'labels': age_buckets,  # show age range
                'datasets': []}
    sex_counts = []
    # Vectorized Polars implementation (no per-group Python DataFrame splits)
    for label, censored_hist_data in censored_histogram_by_age_pl(
            'sex', age_var, age_max, age_min, age_step, df, jitter_function, mask_value,
    ):
        sex_data['datasets'].append({
            'label': label,
            'data': censored_hist_data,
        })
        sex_counts.append({
            'id': f'sex-{label}-count'.lower(),
            'header': f'- {label}',
            'value': sum(censored_hist_data),  # already jittered and masked
        })
    return sex_counts, sex_data


def censored_histogram_by_age_pl(target_var, age_var, age_max, age_min, age_step, df,
                                 jitter_function=None, mask_value=5):
    """Generates histogram data for specified variables with age binning and censoring.

    Args:
        target_var (str): Variable name to generate histogram for (e.g. 'sex', 'enrollment')
        age_var (str): Variable name containing age data to bin by
        age_max (int): Maximum age to include
        age_min (int): Minimum age to include
        age_step (int): Size of age bins
        df (DataFrame): Polars DataFrame containing the data
        jitter_function (callable): Function to apply jittering to counts
        mask_value (int): Threshold for masking small counts

    Yields:
        tuple: (label, censored_hist_data) where:
            - label (str): Category label (capitalized)
            - censored_hist_data (list): List of jittered/masked counts per age bin
    """
    # Ensure only the necessary columns are used to avoid copies
    # Partition by target_var to iterate groups efficiently
    for label, group_df in df.select([target_var, 'case', age_var]).partition_by(target_var, as_dict=True).items():
        label = label[0].capitalize() if isinstance(label, tuple) else str(label).capitalize()
        if jitter_function is not None:
            func = lambda x: jitter_function(x, mask=mask_value, label=label)
        else:
            func = None
        ages = group_df.get_column(age_var).drop_nulls().to_list()
        censored_hist_data = histogram(ages, age_min, age_max, step=age_step, group_extra_in_top_bin=True,
                                       jitter_function=func)
        yield label, censored_hist_data


def histogram(iterable, low, high, bins=None, step=None, group_extra_in_top_bin=False, mask=0,
              jitter_function=lambda x: x):
    """Count elements from the iterable into evenly spaced bins

        >>> scores = [82, 85, 90, 91, 70, 87, 45]
        >>> histogram(scores, 0, 100, 10)
        [0, 0, 0, 0, 1, 0, 0, 1, 3, 2]

    """
    if not bins and not step:
        raise ValueError('Need to specify either bins or step.')
    if not step:
        step = (high - low + 0.0) / bins
    if not bins:
        bins = int(math.ceil((high - low + 0.0) / step))
    dist = Counter((float(x) - low) // step for x in iterable)
    res = [dist[b] for b in range(bins)]
    if group_extra_in_top_bin:
        res[-1] += sum(dist[x] for x in range(bins, int(max(dist)) + 1))
    if jitter_function is not None:
        masked = [jitter_function(r) for r in res]
    else:
        masked = [r for r in res]  # not masked
    return masked


def censored_histogram_by_age_pl2(target_var, age_var, age_max, age_min, age_step, df,
                                 jitter_function=None, mask_value=5):
    """Generates histogram data for specified variables with age binning and censoring.

    Args:
        target_var (str): Variable name to generate histogram for (e.g. 'sex', 'enrollment')
        age_var (str): Variable name containing age data to bin by
        age_max (int): Maximum age to include
        age_min (int): Minimum age to include
        age_step (int): Size of age bins
        df (DataFrame): Polars DataFrame containing the data
        jitter_function (callable): Function to apply jittering to counts
        mask_value (int): Threshold for masking small counts

    Yields:
        tuple:
            - label (str): Category label (capitalized)
            - censored_hist_data (list[int]): Jittered/masked counts per age bin
            - excluded_cases (list[int]): 'case' IDs whose bins were masked (raw>0 but masked==0)
    """
    # Compute number of bins; final bin aggregates ages >= (age_max - age_step)
    n_bins = int(math.ceil((age_max - age_min) / float(age_step)))
    if n_bins <= 0:
        return

    partitions = df.select([target_var, 'case', age_var]).partition_by(target_var, as_dict=True)
    for label in sorted(partitions.keys()):  # ensure consistent ordering
        group_df = partitions[label]
        label_str = label[0].capitalize() if isinstance(label, tuple) else str(label).capitalize()

        # Build per-row bin indices:
        # - drop null ages and ages below min
        # - ages >= age_max go to the last bin (n_bins - 1)
        group_binned = (
            group_df
            .filter(pl.col(age_var).is_not_null() & (pl.col(age_var) >= age_min))
            .with_columns(
                bin=pl.when(pl.col(age_var) >= age_max)
                .then(pl.lit(n_bins - 1))
                .otherwise(((pl.col(age_var) - age_min) / age_step).floor().cast(pl.Int64))
            )
            .with_columns(pl.col('bin').clip(lower_bound=0, upper_bound=n_bins - 1))
        )

        # Raw counts per bin
        counts_df = (
            group_binned.group_by('bin')
            .len()
            .rename({'len': 'count'})
        )

        # Materialize into a dense vector of length n_bins
        raw_counts = [0] * n_bins
        if counts_df.height > 0:
            for b, c in counts_df.iter_rows():
                raw_counts[int(b)] = int(c)

        # Apply jitter/mask function to counts to obtain censored histogram
        if jitter_function is not None:
            func = lambda x, i: jitter_function(x, mask=mask_value, label=f'{label_str}{i}')
            masked_counts = [func(v, i) for i, v in enumerate(raw_counts)]
        else:
            masked_counts = raw_counts[:]  # no masking/jitter at age-group level (e.g., or enrollment)

        # Identify bins that became zero due to masking (exclude genuinely empty bins)
        masked_bin_indices = [i for i, (r, m) in enumerate(zip(raw_counts, masked_counts)) if r > 0 and m == 0]

        # Collect 'case' IDs that fell into masked bins
        if masked_bin_indices:
            excluded_cases = (
                group_binned
                .filter(pl.col('bin').is_in(masked_bin_indices))
                .get_column('case')
                .to_list()
            )
        else:
            excluded_cases = []

        yield label_str, masked_counts, excluded_cases
