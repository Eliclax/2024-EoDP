import warnings
import numpy as np
import pandas as pd


def check_functional_dependency(df: pd.DataFrame, x: str | list[str], y: str | list[str]) -> bool:
    """
    Check that x -> y is a functional dependency in df
    """

    if isinstance(x,str):
        x = [x]
    if isinstance(y,str):
        y = [y]
    df1 = df.drop_duplicates(subset=x)
    df2 = df.drop_duplicates(subset=x+y)
    return df1.shape[0] == df2.shape[0]


def check_december(df: pd.DataFrame) -> bool:
    """
    Takes in a DataFrame with a "Year ending" column and checks to see if all its values are "December"
    """

    for month in df['Year ending']:
        if month != 'December':
            return False
    return True


def extract(
        df: pd.DataFrame,
        keys: list[str],
        other_cols: list[str] = [],
        id_name: str | None = None,
        extractor: pd.DataFrame | None = None,
        col_map: dict[str, str]  = {},
        overwrite_old: bool = False
        ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    :param df: The DataFrame to extract from
    :param keys: The key columns that will later be merged on
    :param other_cols: The other columns from df that will be merged into the extractor
    :param id_name: The name of the index of the extracted DataFrame
    :param extractor: The DataFrame to extract to
    :param col_map: A dict which specifies how to map column names from the df to the extractor
    :param overwrite_old: Whether or not to overwrite old values in column in the case of a conflict, default False
    :return: df, extractor DataFrame

    Given a DataFrame and some columns, extracts out those columns into a new DataFrame,
    replacing the the columns with an index from the new DataFrame.

    e.g.
    df =        \\
    `..a b c d  ` \\
    `0 6 X T 5.3` \\
    `1 6 X T 4.6` \\
    `2 8 Y T 8.3` \\
    `3 8 Y R 4.3` \\
    `4 8 Y T 2.5` \\
    would be split into \\
    `..id d  `    \\
    `0  0 5.3`    \\
    `1  0 4.6`    \\
    `2  1 8.3`    \\
    `3  2 4.3`    \\
    `4  1 2.5`    \\
    and         \\
    `..a b c`    \\
    `id      `    \\
    `0  6 X T`    \\
    `1  8 Y T`    \\
    `2  8 Y R`    \\
    """

    # use cases:
    # extract(df, columns): create a new df for the extraction

    # extract(df, columns, use): use an existing df for the extraction, assumes use has columns, cannot update 
    # existing "columns" combination entries, but can add to "use" DF when some unique combination of "columns"
    # is missing

    # extract(df, columns, use, col_transform): uses an existing df for the extraction, 


    """
    Brief summary of how this function works:

    THIS FUNCTION ASSUMES THAT KEYS -> OTHER_COLUMNS IS A FUNCTIONAL DEPENDENCY.
    We only care about unique values of df[keys (+ other_columns)].  This is what we are going to replace with the 
        index of the extractor.
    We will also keep the df[other_columnns] columns.  So we need df.drop_duplicates(subset=keys)[keys + other_cols]
    Transform the names of columns in (processed) df to the ones in extractor using col_map if necessary, so we
        can merge later.
    If the extractor doesn't have all columns in keys yet, create them.
    Do an outer join of extractor with (processed) df on keys, making sure index order is preserved (which is annoying lol)
    For each column in other_columns, determine if a "target column" already exists in the extractor and if so,
        merge the two columns, with either old or new values having precedence
    For each row in df, replace keys + other_columns with index in extractor.
    """

    # Preprocessing
    if extractor is None:
        extractor = pd.DataFrame()
    key_dtypes = [ 'Int64' if x.dtype == int else x.dtype for _, x in df[keys].items() ]
    other_col_dtypes = [ 'Int64' if x.dtype == int else x.dtype for _, x in df[other_cols].items() ]
    mapped_keys = [ col_map[x] if x in col_map.keys() else x for x in keys ]
    mapped_other_cols = [ col_map[x] if x in col_map.keys() else x for x in other_cols ]
    id_name = id_name or extractor.index.name or 'ID'

    # Keep only values of df[keys + other_cols] unique on keys
    dfp = df.drop_duplicates(subset=keys)[keys + other_cols]

    # Transform the names of columns in (processed) df to the ones in extractor using col_map if necessary, so we
    # can merge later.
    dfp = dfp.rename(columns=col_map)

    # If the extractor doesn't have all the columns in keys yet, create them and initialise to np.nan
    # TODO: see if this cane be improved... maybe set(extractor)?
    overlap: list[str] = list(set(extractor.columns.values.tolist()).intersection(set(mapped_other_cols)))
    for col in mapped_keys:
        if col not in extractor.columns:
            extractor[col] = np.nan

    # Do an outer join of extractor with (processed) df on keys, then reset indices
    extractor['INDEX'] = extractor.index
    extractor = extractor.merge(dfp, on=mapped_keys, how='outer', suffixes=('_xxxxx', '_yyyyy'))
    extractor = extractor.sort_values('INDEX').reset_index(drop=True).drop(columns='INDEX')

    # For each column in other_columns, determine if a "target column" already exists in the extractor and if so,
    # merge the two columns, with either old or new values having precedence depending on overwrite_old
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning) # Suppress FutureWarning
        for col in overlap:
            if overwrite_old:
                extractor[col+'_xxxxx'].update(extractor[col+'_yyyyy'])
                extractor = extractor.drop(columns=col+'_yyyyy')
                extractor = extractor.rename(columns={col+'_xxxxx': col})
            else:
                extractor[col+'_yyyyy'].update(extractor[col+'_xxxxx'])
                extractor = extractor.drop(columns=col+'_xxxxx')
                extractor = extractor.rename(columns={col+'_yyyyy': col})

    # For each row in df, replace keys + other_columns with index in extractor.
    extractor[id_name] = extractor.index
    df = df.merge(extractor[mapped_keys + [id_name]], left_on=keys, right_on=mapped_keys, how='left')
    insert_col = min([df.columns.get_loc(c) for c in keys])
    df.insert(insert_col, id_name, df.pop(id_name))
    df.drop(columns=keys+other_cols, inplace=True)
    extractor.drop(columns=id_name, inplace=True)

    # Name the index of the extractor the id_name
    extractor.index.name = id_name

    # Cast columns back to right type
    extractor = extractor.astype(dict(zip(mapped_keys + mapped_other_cols, key_dtypes + other_col_dtypes)))

    return df, extractor