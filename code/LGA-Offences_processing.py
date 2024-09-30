import pandas as pd
import locale
from util import extract, check_december, check_functional_dependency

read_loc = 'data/raw/'
write_loc = 'data/processed/'
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

df_tables = ['lga', 'police_region', 'police_service_area', 'towns', 'offence_division', 'offence_subdivision', 
             'offence_group', 'location_division', 'location_subdivision', 'location_group', 'investigation_status',
             'csa_drug_type']
df_table = {}
for table_name in df_tables:
    df_table[table_name] = pd.DataFrame()


def process_Table_01():
    global df_table

    print("Processing Table_01...")
    df = pd.read_csv(read_loc + 'Table_01.csv')

    # Remove "Year ending" columnn since it's always December
    assert check_december(df)
    df = df.drop(columns=['Year ending'])

    # Turn numeric strings to int and float
    df['Offence Count'] = df['Offence Count'].apply(lambda x : locale.atoi(x))
    df['Rate per 100,000 population'] = df['Rate per 100,000 population'].apply(lambda x : locale.atof(x) if type(x) == str else x)

    # Filter DataFrame into two based on whether or not the LGA column is "Total"
    dfa = df[df['Local Government Area'] != 'Total'].copy()
    dfb = df[df['Local Government Area'] == 'Total'].copy()

    # Process "non-Total" DataFrame, extracting the Local Government Area -> Police Region functional dependency
    assert check_functional_dependency(dfa, 'Local Government Area', 'Police Region')
    dfa, df_table['police_region'] = extract(dfa, keys=['Police Region'], id_name='PRID', extractor=df_table['police_region'])
    dfa, df_table['lga'] = extract(dfa, keys=['Local Government Area'], other_cols=['PRID'], id_name='LGAID', extractor=df_table['lga'])

    # Process "Total" DataFrame
    dfb = dfb.drop(columns='Local Government Area')
    dfb, df_table['police_region'] = extract(dfb, keys=['Police Region'], extractor=df_table['police_region'])

    # Write to csv files
    dfa.to_csv(write_loc + 'Table_01a.csv', index=False)
    dfb.to_csv(write_loc + 'Table_01b.csv', index=False)


def process_Table_02():
    global df_table

    print("Processing Table_02...")
    df = pd.read_csv(read_loc + 'Table_02.csv')

    # Remove "Year ending" columnn since it's always December
    assert check_december(df)
    df = df.drop(columns=['Year ending'])

    # Turn numeric strings to int and float
    df['Offence Count'] = df['Offence Count'].apply(lambda x : locale.atoi(x))
    df['PSA Rate per 100,000 population'] = df['PSA Rate per 100,000 population'].apply(lambda x : locale.atof(x) if type(x) == str else x)
    df['LGA Rate per 100,000 population'] = df['LGA Rate per 100,000 population'].apply(lambda x : locale.atof(x) if type(x) == str else x)

    # Extract Police Service Areas
    df, df_table['police_service_area'] = extract(df, keys=['Police Service Area'], id_name='PSAID')

    # Extract the Local Government Area -> Police Service Area is a functional dependency
    assert check_functional_dependency(df, 'Local Government Area', 'PSAID')
    df, df_table['lga'] = extract(df, keys=['Local Government Area'], other_cols=['PSAID'], id_name="LGAID", extractor=df_table['lga'])

    # Extract the Offence Group -> Offence Subdivision -> Offence Division functional dependencies
    assert check_functional_dependency(df, 'Offence Subdivision', 'Offence Division')
    assert check_functional_dependency(df, 'Offence Subgroup', 'Offence Subdivision')
    df, df_table['offence_division'] = extract(df, keys=['Offence Division'], id_name='ODID', extractor=df_table['offence_division'])
    df, df_table['offence_subdivision'] = extract(df, keys=['Offence Subdivision'], other_cols=['ODID'], id_name='OSDID', extractor=df_table['offence_subdivision'])
    df, df_table['offence_group'] = extract(df, keys=['Offence Subgroup'], other_cols=['OSDID'], col_map={'Offence Subgroup': 'Offence Group'},
                                            id_name='OGID', extractor=df_table['offence_group'])

    # Write to csv file
    df.to_csv(write_loc + 'Table_02.csv', index=False)
    

def process_Table_03():
    global df_table

    print("Processing Table_03...")
    df = pd.read_csv(read_loc + 'Table_03.csv')

    # Remove "Year ending" columnn since it's always December
    assert check_december(df)
    df = df.drop(columns=['Year ending'])

    # Turn numeric strings to int and float
    df['Offence Count'] = df['Offence Count'].apply(lambda x : locale.atoi(x))

    # Extract LGAs and towns [LGAID, Postcode, Town Name]
    df, df_table['lga'] = extract(df, keys=['Local Government Area'], id_name='LGAID', extractor=df_table['lga'])
    df, df_table['towns'] = extract(df, keys=['LGAID','Postcode','Suburb/Town Name'], id_name='TID', extractor=df_table['towns'])

    # Extract the Offence Group -> Offence Subdivisions -> Offence Division functional dependencies
    assert check_functional_dependency(df, 'Offence Subdivision', 'Offence Division')
    assert check_functional_dependency(df, 'Offence Subgroup', 'Offence Subdivision')
    df, df_table['offence_division'] = extract(df, keys=['Offence Division'], id_name='ODID', extractor=df_table['offence_division'])
    df, df_table['offence_subdivision'] = extract(df, keys=['Offence Subdivision'], other_cols=['ODID'], id_name='OSDID', extractor=df_table['offence_subdivision'])
    df, df_table['offence_group'] = extract(df, keys=['Offence Subgroup'], other_cols=['OSDID'], col_map={'Offence Subgroup': 'Offence Group'}, 
                                            id_name='OGID', extractor=df_table['offence_group'])

    # Write to csv file
    df.to_csv(write_loc + 'Table_03.csv', index=False)


def process_Table_04():
    global df_table

    print("Processing Table_04...")
    df = pd.read_csv(read_loc + 'Table_04.csv')

    # Remove "Year ending" columnn since it's always December
    assert check_december(df)
    df = df.drop(columns=['Year ending'])

    # Turn numeric strings to int and float
    df['Offence Count'] = df['Offence Count'].apply(lambda x : locale.atoi(x))

    # Extract LGAs
    df, df_table['lga'] = extract(df, keys=['Local Government Area'], id_name='LGAID', extractor=df_table['lga'])

    # Extract the Location Group -> Location Subdivision -> Location Division functional dependencies
    assert check_functional_dependency(df, 'Location Subdivision', 'Location Division')
    assert check_functional_dependency(df, 'Location Group', 'Location Subdivision')
    df, df_table['location_division'] = extract(df, keys=['Location Division'], id_name='LDID', extractor=df_table['location_division'])
    df, df_table['location_subdivision'] = extract(df, keys=['Location Subdivision'], other_cols=['LDID'], id_name='LSDID', extractor=df_table['location_subdivision'])
    df, df_table['location_group'] = extract(df, keys=['Location Group'], other_cols=['LSDID'], id_name='LGID', extractor=df_table['location_group'])

    # Write to csv file
    df.to_csv(write_loc + 'Table_04.csv', index=False)



def process_Table_05():
    global df_table

    print("Processing Table_05...")
    df = pd.read_csv(read_loc + 'Table_05.csv')

    # Remove "Year ending" columnn since it's always December
    assert check_december(df)
    df = df.drop(columns=['Year ending'])

    # Turn numeric strings to int and float
    df['Offence Count'] = df['Offence Count'].apply(lambda x : locale.atoi(x))

    # Extract LGAs
    df, df_table['lga'] = extract(df, keys=['Local Government Area'], id_name='LGAID', extractor=df_table['lga'])

    # Extract Investigation Statuses
    df, df_table['investigation_status'] = extract(df, keys=['Investigation Status'], id_name='LSID', extractor=df_table['investigation_status'])

    # Write to csv file
    df.to_csv(write_loc + 'Table_05.csv', index=False)



def process_Table_06():
    global df_table

    print("Processing Table_06...")
    df = pd.read_csv(read_loc + 'Table_06.csv')

    # Remove "Year ending" columnn since it's always December
    assert check_december(df)
    df = df.drop(columns=['Year ending'])

    # We don't need to turn numeric strings to int and float because there are no quotation marks

    # Extract LGAs
    df, df_table['lga'] = extract(df, keys=['Local Government Area'], id_name='LGAID', extractor=df_table['lga'])

    # Extract the Offence Groups -> Offence Subdivisions functional dependency
    assert check_functional_dependency(df, 'Offence Group', 'Offence Subdivision')
    df, df_table['offence_subdivision'] = extract(df, keys=['Offence Subdivision'], id_name='OSDID', extractor=df_table['offence_subdivision'])
    df, df_table['offence_group'] = extract(df, keys=['Offence Group'], other_cols=['OSDID'], id_name='OGID', extractor=df_table['offence_group'])

    # Extract the CSA Drug Types
    df, df_table['csa_drug_type'] = extract(df, keys=['CSA Drug Type'], id_name='DTID', extractor=df_table['csa_drug_type'])

    # Write to csv file
    df.to_csv(write_loc + 'Table_06.csv', index=False)


if __name__ == '__main__':
    process_Table_01()
    process_Table_02()
    process_Table_03()
    process_Table_04()
    process_Table_05()
    process_Table_06()
    for table_name in df_table.keys():
        df_table[table_name].to_csv(write_loc + table_name + '.csv')