import pandas as pd
import locale
from util import extract, check_december, check_functional_dependency

read_loc = 'data/raw/'
write_loc = 'data/processed/'
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

def offence_processing(df_: pd.DataFrame) -> None:
    """
    Takes in a Dataframe with Offence Division, Offence Subdivision, and Offence Subgroup columns and
    genereates Offence_Division.csv, Offence_Subdivion.csv, and Offence_Subgroup.csv files.
    """

    df = df_.drop_duplicates(subset=['Offence Division', 'Offence Subdivision', 'Offence Subgroup'])

    # Offence Division Processing
    dfod = pd.DataFrame(df["Offence Division"].unique(), columns=['Code'])
    dfod[['Full Name', 'Code', 'Offence']] = dfod['Code'].str.extract("(([ABCDEF]|) ?(.+))")
    dfod = dfod[['Code', 'Offence', 'Full Name']]
    dfod.sort_values(by=['Code'], inplace=True)
    dfod.reset_index(drop=True, inplace=True)
    dfod.to_csv("final/Offence_Division.csv")

    # Offence Subdivision Processing
    dfosd = pd.DataFrame(df["Offence Subdivision"].unique(), columns=['Code'])
    dfosd[['Full Name', 'Code', 'Offence']] = dfosd['Code'].str.extract("(([ABCDEF]\d{2,3}|) ?(.+))")
    dfosd['ODID'] = dfosd['Full Name'].apply(lambda x : dfod.index[dfod['Full Name'] == df[df['Offence Subdivision'] == x].iloc[0]['Offence Division']].tolist()[0])
    dfosd = dfosd[['ODID', 'Code', 'Offence', 'Full Name']]
    dfosd.sort_values(by=['Code'], inplace=True)
    dfosd.reset_index(drop=True, inplace=True)
    dfosd.to_csv("final/Offence_Subdivision.csv")

    # Offence Subgroup Processing
    dfosg = pd.DataFrame(df["Offence Subgroup"].unique(), columns=['Code'])
    dfosg[['Full Name', 'Code', 'Offence']] = dfosg['Code'].str.extract("(([ABCDEF]\d{2,3}|) ?(.+))")
    dfosg['OSDID'] = dfosg['Full Name'].apply(lambda x : dfosd.index[dfosd['Full Name'] == df[df['Offence Subgroup'] == x].iloc[0]['Offence Subdivision']].tolist()[0])
    dfosg = dfosg[['OSDID', 'Code', 'Offence', 'Full Name']]
    dfosg.sort_values(by=['Code'], inplace=True)
    dfosg.reset_index(drop=True, inplace=True)
    dfosg.to_csv("final/Offence_Subgroup.csv")


df_lga = pd.DataFrame()
df_police_region = pd.DataFrame()
df_police_service_area = pd.DataFrame()
df_locations = pd.DataFrame()
df_offence_division = pd.DataFrame()
df_offence_subdivision = pd.DataFrame()
df_offence_subgroup = pd.DataFrame()

def process_Table_01():
    df = pd.read_csv(read_loc + 'Table_01.csv')

    # Remove "Year ending" columnn since it's always December
    df = df.drop(columns=['Year ending'])

    # Turn numeric strings to int and float
    df['Offence Count'] = df['Offence Count'].apply(lambda x : locale.atoi(x))
    df['Rate per 100,000 population'] = df['Rate per 100,000 population'].apply(lambda x : locale.atof(x) if type(x) == str else x)

    # Filter DataFrame into two based on whether or not the LGA column is "Total"
    dfa = df[df['Local Government Area'] != 'Total'].copy()
    dfb = df[df['Local Government Area'] == 'Total'].copy()
    dfb = dfb.drop(columns='Local Government Area')

    # Process "non-Total" DataFrame
    dfa, df_police_region = extract(dfa, keys=['Police Region'], id_name='PRID')
    # TODO: do we put the PRID into the other_cols argument?
    dfa, df_lga = extract(dfa, keys=['PRID','Local Government Area'], id_name='LGAID')
    df_lga.to_csv(write_loc + 'LGA.csv')
    dfa.to_csv(write_loc + 'Table_01a.csv', index=False)

    # Process "Total" DataFrame
    dfb, df_police_region = extract(dfb, keys=['Police Region'], extractor=df_police_region)
    dfb.to_csv(write_loc + 'Table_01b.csv', index=False)
    df_police_region.to_csv(write_loc + 'Police_Region.csv')


def process_Table_02():
    df = pd.read_csv(read_loc + 'Table_02.csv')

    # Remove "Year ending" columnn since it's always December
    assert check_december(df)
    df = df.drop(columns=['Year ending'])

    # Turn numeric strings to int and float
    df['Offence Count'] = df['Offence Count'].apply(lambda x : locale.atoi(x))
    df['PSA Rate per 100,000 population'] = df['PSA Rate per 100,000 population'].apply(lambda x : locale.atof(x) if type(x) == str else x)
    df['LGA Rate per 100,000 population'] = df['LGA Rate per 100,000 population'].apply(lambda x : locale.atof(x) if type(x) == str else x)

    # Extract Police Service Areas
    df, df_police_service_area = extract(df, keys=['Police Service Area'], id_name='PSAID')
    df_police_service_area.to_csv(write_loc + 'Police_Service_Area.csv')

    # Check that Local Government Area -> Police Service Area is a functional dependency, then extract it
    assert check_functional_dependency(df, 'Local Government Area', 'PSAID')
    df_lga = pd.read_csv(write_loc + 'LGA.csv', index_col='LGAID')
    df, df_lga = extract(df, keys=['Local Government Area'], other_cols=['PSAID'], id_name="LGAID", extractor=df_lga)
    df_lga['PSAID'] = df_lga['PSAID'].astype('Int64')
    df_lga.to_csv(write_loc + 'LGA.csv')

    df_offence_division = pd.read_csv(write_loc + 'Offence_Division.csv', index_col = 'ODID')
    df_offence_subdivision = pd.read_csv(write_loc + 'Offence_Subdivision.csv', index_col = 'OSDID')
    df_offence_subgroup = pd.read_csv(write_loc + 'Offence_Subgroup.csv', index_col = 'OSGID')
    df, df_offence_division = extract(df, keys=['Offence Division'], id_name='ODID', extractor=df_offence_division)
    df, df_offence_subdivision = extract(df, keys=['Offence Subdivision'], other_cols=['ODID'], id_name='OSDID', extractor=df_offence_subdivision)
    df, df_offence_subgroup = extract(df, keys=['Offence Subgroup'], other_cols=['OSDID'], id_name='OSGID', extractor=df_offence_subgroup)
    df_offence_division.to_csv(write_loc + 'Offence_Division.csv')
    df_offence_subdivision.to_csv(write_loc + 'Offence_Subdivision.csv')
    df_offence_subgroup.to_csv(write_loc + 'Offence_Subgroup.csv')
    #df.drop(columns=['Offence Division','Offence Subdivision'], inplace=True)

    # Write to Table_02.csv
    df.to_csv(write_loc + 'Table_02.csv', index=False)
    

def process_Table_03():
    df = pd.read_csv(read_loc + 'Table_03.csv')

    # Remove "Year ending" columnn since it's always December
    df = df.drop(columns=['Year ending'])

    # Factor out the locations [LGA, Postcode, Town Name]
    df, df_locations = extract(df, keys=['Local Government Area','Postcode','Suburb/Town Name'], id_name='LID')
    df_locations.to_csv(write_loc + 'Locations.csv')

    # # Factor out the Offence Divisions, then Offence Subdivisions, then Offence Subgroups
    df, df_offence_division = extract(df, keys=['Offence Division'], id_name='ODID')
    df_offence_division.to_csv(write_loc + 'Offence_Division.csv')
    print(df.head())
    df, df_offence_subdivision = extract(df, keys=['Offence Subdivision'], other_cols=['ODID'], id_name='OSDID')
    df_offence_subdivision.to_csv(write_loc + 'Offence_Subdivision.csv')
    df, df_offence_subgroup = extract(df, keys=['Offence Subgroup'], other_cols=['OSDID'], id_name='OSGID')
    df_offence_subgroup.to_csv(write_loc + 'Offence_Subgroup.csv')

    # Remove commas in numbers (number is still a string, but this doesn't matter when we store it as a CSV)
    df['Offence Count'] = df['Offence Count'].apply(lambda x : x.replace(",",""))

    df.to_csv(write_loc + 'Table_03.csv', index=False)


def process_Table_04():
    df = pd.read_csv(read_loc + 'Table_04.csv')


def process_Table_05():
    df = pd.read_csv(read_loc + 'Table_05.csv')


def process_Table_06():
    df = pd.read_csv(read_loc + 'Table_06.csv')





if __name__ == '__main__':
    process_Table_03()
    process_Table_01()
    process_Table_02()
    process_Table_04()
    process_Table_05()
    process_Table_06()