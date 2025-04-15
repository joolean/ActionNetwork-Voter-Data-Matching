import marimo

__generated_with = "0.8.22"
app = marimo.App(width="medium")


@app.cell
def __(people_df, pl):
    import marimo

    __generated_with = "0.11.14"
    app = marimo.App(width="medium")


    @app.cell
    def _():
        import marimo as mo
        return mo

    #input api key 
    @app.cell
    def _(mo):
        api_key_input = "11130f62af61d56c27b6af1300cc5affee24b2dbe7f105d58746a83985a986555dc7a96e44652bb40c916f56ed3ad662e321209dafa67f2fc3401fec38b9a746"
        api_key_input
        return (api_key_input,)


    @app.cell
    def _(api_key_input, mo):
        mo.stop(api_key_input is None)
        import requests
        import polars as pl

        def download_people(limit=50, max_people=80000):
            url = "https://api.solidarity.tech/v1/users"
            headers = {"accept": "application/json", "authorization": f"Bearer {api_key_input}"}
            all_people, offset = [], 0

            while len(all_people) < max_people:
                response = requests.get(f"{url}?_limit={limit}&_offset={offset}", headers=headers)
                people_batch = [
                    {
                        "solidarity_tech_id": p["id"],  
                        "solidarity_tech_first_name": p.get("first_name"),
                        "solidarity_tech_last_name": p.get("last_name"),
                        "solidarity_tech_phone_number": p.get("phone_number"),
                        "solidarity_tech_email": p.get("email"),
                        "solidarity_tech_address1": p.get("address", {}).get("address1"),
                        "solidarity_tech_address2": p.get("address", {}).get("address2"),
                        "solidarity_tech_city": p.get("address", {}).get("city"),
                        "solidarity_tech_state": p.get("address", {}).get("state"),
                        "solidarity_tech_zip5": p.get("address", {}).get("zip_code"),
                        "solidarity_tech_registration_status": p.get("custom_user_properties", {}).get("registration-status"),
                    } for p in response.json().get("data", [])
                ]
                if not people_batch:
                    break
                all_people.extend(people_batch)
                offset += limit
                print(f"{len(all_people)} people downloaded...")

            return pl.DataFrame(all_people)

        people_df = download_people()

        mo.sql("DROP TABLE IF EXISTS people_df")
        mo.sql("CREATE TABLE people_df AS SELECT * FROM people_df")

    #Separate and remove users who already have registration status in Solidarity Tech
    @app.cell
    def separate_by_reg_status(mo):
        ppl = mo.sql("""
            SELECT *
            FROM people_df
        """)

        # Flatten the 'solidarity_tech_registration_status' List[Struct] column
        ppl = ppl.with_columns(
            pl.col("solidarity_tech_registration_status").arr.explode().alias("exploded_status")
        )

        # Access the struct fields after exploding
        ppl = ppl.with_columns(
            pl.col("exploded_status").struct.field("label").alias("registration_label")
        )

        # Drop the exploded column after extracting struct fields
        ppl = ppl.drop("exploded_status")

        # Filter out the rows with "Unknown" registration_label for further processing
        unknown_registration = ppl.filter(pl.col("registration_label").is_null())
        registered = ppl.filter(pl.col("registration_label").is_not_null())

        # Flatten data in `registered` by selecting the label from List[Struct] columns
        registered = registered.select(
            *[
                pl.col(c).arr.explode().struct.field("label").alias(c+"_label")
                if isinstance(registered[c].dtype, pl.List) and isinstance(registered[c].dtype.inner, pl.Struct)
                else pl.col(c)
                for c in registered.columns
            ]
        )

        # Flatten any other List[Struct] columns in registered
        registered = registered.with_columns(
            *[
                pl.col(c).arr.explode().struct.field("label").alias(c+"_label")
                for c in registered.columns
                if isinstance(registered[c].dtype, pl.List) and isinstance(registered[c].dtype.inner, pl.Struct)
            ]
        )

        # Save the filtered results (those with a valid registration label)
        registered.write_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/solidarity_tech_with_registration.csv")
        print(unknown_registration.head())
        print(unknown_registration.columns)

        unknown_registration_pd = unknown_registration.to_pandas()

        unknown_registration_pd.to_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/solidarity_tech_without_registration.csv")


    if __name__ == "__main__":
        app.run()
    return __generated_with, app, marimo, separate_by_reg_status


@app.cell
def __(df):
    len(df)
    return


app._unparsable_cell(
    r"""
    df8 = 
    """,
    name="__"
)


@app.cell
def __():
    import pandas as pd

        # Load voter file as a Pandas DataFrame
    voterfile = pd.read_parquet("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/voter_file.parquet")

    # Load Solidarity Tech file, ensuring ZIP code is a string
    sol_tech = pd.read_csv(
        "/Users/avonleafisher/Downloads/AllNYSVoters_20250310/solidarity_tech_without_registration.csv",
        dtype={"solidarity_tech_zip5": str}
    )
    print(len(sol_tech))

    # Ensure ZIP codes are strings and take first 5 characters
    voterfile["RZIP5"] = voterfile["RZIP5"].astype(str).str[:5]
    sol_tech["solidarity_tech_zip5"] = sol_tech["solidarity_tech_zip5"].astype(str).str[:5]

    # Convert names to uppercase for case-insensitive matching
    voterfile["LASTNAME"] = voterfile["LASTNAME"].str.upper()
    voterfile["FIRSTNAME"] = voterfile["FIRSTNAME"].str.upper()
    sol_tech["solidarity_tech_last_name"] = sol_tech["solidarity_tech_last_name"].str.upper()
    sol_tech["solidarity_tech_first_name"] = sol_tech["solidarity_tech_first_name"].str.upper()

    match_results_df = sol_tech.merge(
        voterfile,
        left_on=["solidarity_tech_last_name", "solidarity_tech_zip5"],
        right_on=["LASTNAME", "RZIP5"],
        how="inner"  # Only keep matching rows
    )

    # match_results_df = sol_tech.merge(
    #     voterfile,
    #     left_on=["solidarity_tech_first_name", "solidarity_tech_zip5"],
    #     right_on=["FIRSTNAME", "RZIP5"],
    #     how="inner"  # Only keep matching rows
    # )

    # Select relevant columns and rename them
    match_results_df = match_results_df[[
        "solidarity_tech_id", "solidarity_tech_first_name", "solidarity_tech_last_name", 
        "solidarity_tech_address1", "solidarity_tech_address2", "solidarity_tech_zip5", 
        "solidarity_tech_phone_number", "solidarity_tech_email", "FIRSTNAME", "LASTNAME", 
        "RADDNUMBER", "RSTREETNAME", "RAPARTMENT", "RZIP5", "SBOEID", "STATUS", "ENROLLMENT", 
        "MAILADD1", "MAILADD2", "MAILADD3", "MAILADD4", "DOB", "PREVNAME", "PREVADDRESS"
    ]]

    match_results_df.columns = [
        "solidarity_tech_id", "solidarity_tech_first_name", "solidarity_tech_last_name", 
        "solidarity_tech_address1", "solidarity_tech_apt_num", "solidarity_tech_zip5", 
        "solidarity_tech_phone_number", "solidarity_tech_email", "voter_first", "voter_last", 
        "voter_house_number", "voter_street_name", "voter_apt_num", "voter_zip", "boe_id", 
        "registration_status", "party_enrollment", "voter_mailing_address1", 
        "voter_mailing_address2", "voter_mailing_address3", "voter_mailing_address4", 
        "voter_dob", "voter_previous_name", "voter_previous_address"
    ]

    # Save to CSV
    match_results_df.to_csv(
        "/Users/avonleafisher/Downloads/AllNYSVoters_20250310/match_results ln.csv", index=False
    )

    return match_results_df, pd, sol_tech, voterfile


@app.cell
def __(pd):
    from nicknames import NickNamer
    from fuzzywuzzy import fuzz
    import numpy as np
    import re

    # #read in results, get rows with exact address match, write exact matches to csv, exclude from df
    # # ln = pd.read_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/match_results_fn.csv")
    # fn = pd.read_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/match_results.csv")


    # df = pd.concat([ln, fn])
    # df = fn

    df1 = pd.read_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/match_results fn.csv")
    df2 = pd.read_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/match_results ln.csv")
    df=pd.concat([df1, df2])
    # df = df.apply(lambda x: x.str.upper() if x.dtype == 'object' else x)

    # create house # and street name cols from address1 col, convert to uppercase
    df[['solidarity_tech_house_num', 'solidarity_tech_street_name']] = df['solidarity_tech_address1']\
        .str.extract(r'^(\d+)\s+(.*)', expand=True)

    df['solidarity_tech_house_num'] = df['solidarity_tech_house_num']
    df['solidarity_tech_street_name'] = df['solidarity_tech_street_name']


    import re

    # Define common suffixes to remove when isolated
    suffixes = r'\s+(STREET|ST|AVENUE|AVE|BLVD|BOULEVARD|DRIVE|DR|ROAD|RD|PARKWAY|PKWY|LANE|LN|COURT|CT|TERRACE|TER|PLACE|PL|CIRCLE|CIR|HIGHWAY|HWY|WAY|SQ|SQUARE|EXPY|EXPRESSWAY|STATION|STN|ALLEY|ALY|PLAZA|PLZ|LOOP)\b'

    # Define common directional replacements
    directions = {
        r'\bEAST\b': 'E',
        r'\bWEST\b': 'W',
        r'\bNORTH\b': 'N',
        r'\bSOUTH\b': 'S'
    }

    # Define function for advanced normalization
    def normalize_street_name(street_name):
        if pd.isna(street_name):
            return street_name  # Keep NaNs as they are

        # Standardize directional prefixes (e.g., EAST 117 -> E 117)
        for full, abbrev in directions.items():
            street_name = re.sub(full, abbrev, street_name, flags=re.IGNORECASE)

        # Handle numbered streets (e.g., 117th -> 117)
        street_name = re.sub(r'(\d+)(TH|ST|ND|RD)\b', r'\1', street_name, flags=re.IGNORECASE)

        # Remove common street suffixes only when they are isolated
        street_name = re.sub(suffixes, '', street_name, flags=re.IGNORECASE).strip()

        return street_name

    # Apply the function to both street name columns
    df['solidarity_tech_street_name_clean'] = df['solidarity_tech_street_name'].apply(normalize_street_name)
    df['voter_street_name_clean'] = df['voter_street_name'].apply(normalize_street_name)


    def clean_apt_num(apt_num):
        if pd.isna(apt_num):
            return None
        # Remove any non-alphanumeric characters (except digits and letters) and convert to uppercase
        apt_num = re.sub(r'[^a-zA-Z0-9]', '', apt_num).upper()

        # Remove terms like 'APARTMENT', 'UNIT', 'APT', 'NUM' at the beginning
        apt_num = re.sub(r'^(APARTMENT|UNIT|APT|NUM)', '', apt_num)

        # Normalize floor-related terms (e.g., 'FL', 'FLOOR', 'PH', 'PENTHOUSE')
        apt_num = re.sub(r'\b(FLOOR|FL|FLR|1ST|2ND|3RD|UPPER|LOWER|PENTHOUSE|PH|BASEMENT|B)\b', '', apt_num)

        # Handle variations like 'UNIT 4C' -> '4C' and 'C4' -> '4C'
        apt_num = re.sub(r'UNIT\s*(\d+[A-Za-z]*)', r'\1', apt_num)
        apt_num = re.sub(r'(\d+)([A-Za-z]{1})', r'\1\2', apt_num)  # Normalize 'C4' -> '4C'

        # Remove any excess spaces after stripping terms
        apt_num = apt_num.strip()

        return apt_num

    # Apply cleaning function to apt # cols
    df['clean_solidarity_tech_apt_num'] = df['solidarity_tech_apt_num'].apply(clean_apt_num)
    df['clean_voter_apt_num'] = df['voter_apt_num'].apply(clean_apt_num)

    # Create col to check if the cleaned apartment numbers match
    df['apt_num_match'] = df['clean_solidarity_tech_apt_num'] == df['clean_voter_apt_num']

    # If 'solidarity_tech_apt_num' is null, set 'apt_num_match' to "No apt # in Solidarity Tech"
    df.loc[df['solidarity_tech_apt_num'].isna(), 'apt_num_match'] = "No apt # in Solidarity Tech"


    # 1. Last name exact match
    df['last_name_match'] = (df['solidarity_tech_last_name'].fillna('') == df['voter_last'].fillna('')).astype(int)

    # 2. Fuzzy last name match
    df['fuzzy_last_name_match'] = df.apply(
        lambda row: fuzz.partial_ratio(str(row['solidarity_tech_last_name'] or ''), str(row['voter_last'] or '')) >= 80,
        axis=1
    ).astype(int)

    # 3. First name exact match
    df['first_name_match'] = (df['solidarity_tech_first_name'].fillna('') == df['voter_first'].fillna('')).astype(int)

    # 4. Nickname match
    nn = NickNamer()
    def is_nickname_match(name1, name2):
        if pd.isna(name1) or pd.isna(name2):  # Handle NaN values
            return False
        name1, name2 = str(name1).strip(), str(name2).strip()
        return name1 == name2 or name2 in nn.nicknames_of(name1) or name1 in nn.nicknames_of(name2)

    df['nickname_match'] = df.apply(lambda row: is_nickname_match(row['solidarity_tech_first_name'], row['voter_first']), axis=1).astype(int)

    # 5. Street Name Match
    df['street_name_match'] = (df['solidarity_tech_street_name_clean'].fillna('') == df['voter_street_name_clean'].fillna('')).astype(int)

    # 6. House Number Match
    df['house_num_match'] = (df['solidarity_tech_house_num'].fillna('') == df['voter_house_number'].fillna('')).astype(int)

    # 7. Apartment Number Match
    df['apt_num_match'] = (df['clean_solidarity_tech_apt_num'].fillna('') == df['clean_voter_apt_num'].fillna('')).astype(int)

    # 8. Compute match strength
    match_columns = ['nickname_match', 'last_name_match', 'fuzzy_last_name_match',
                     'first_name_match', 'street_name_match', 'house_num_match', 'apt_num_match']
    df['match_strength'] = df[match_columns].sum(axis=1)

    # 9. If match_strength < 1, remove voter columns
    columns_to_null = ['voter_first', 'voter_last', 'voter_house_number', 'voter_street_name_clean',
                       'voter_apt_num', 'voter_zip', 'boe_id', 'registration_status',
                       'party_enrollment', 'voter_mailing_address1', 'voter_mailing_address2',
                       'voter_mailing_address3', 'voter_mailing_address4', 'voter_dob', 'voter_previous_name',
                       'voter_previous_address']
    df.loc[df['match_strength'] < 1, columns_to_null] = None

    # 10. Remove duplicate rows where match_strength == 0
    df_no_match = df[df['match_strength'] == 0].drop_duplicates(subset='solidarity_tech_id', keep='first')

    # 11. Merge back high-confidence matches
    df_matched = df[df['match_strength'] > 0]
    df = pd.concat([df_matched, df_no_match])

    # 12. Assign highest match strength per ID
    df['is_highest_match_strength'] = df.groupby('solidarity_tech_id')['match_strength'].transform(lambda x: x == x.max()).astype(int)

    print(df.columns)
    # Define column order with matched fields next to each other
    new_column_order = [
        'solidarity_tech_id', 'solidarity_tech_phone_number', 
        'solidarity_tech_first_name', 'voter_first',
        'solidarity_tech_last_name', 'voter_last',
        # Address Columns
        'solidarity_tech_address1', 'voter_house_number',
        'solidarity_tech_house_num', 'voter_house_number',  # Reordered to keep house number together
        'solidarity_tech_street_name', 'voter_street_name',
        'solidarity_tech_street_name_clean', 'voter_street_name_clean',
        'clean_solidarity_tech_apt_num', 'clean_voter_apt_num', 
        'solidarity_tech_apt_num', 'voter_apt_num',  # Reordered for apartment number
        'solidarity_tech_zip5', 'voter_zip',  # Zip codes grouped together
        # Email & Registration
        'solidarity_tech_email',
        'boe_id', 'registration_status', 'party_enrollment',
        'voter_mailing_address1', 'voter_mailing_address2',
        'voter_mailing_address3', 'voter_mailing_address4', 
        'voter_dob', 'voter_previous_name', 'voter_previous_address',
        # Matching Criteria
        'last_name_match', 'fuzzy_last_name_match', 'first_name_match',
        'nickname_match', 'street_name_match', 'house_num_match', 'apt_num_match',
        'match_strength', 'is_highest_match_strength'
    ]

    # Reorder columns in DataFrame
    df = df[new_column_order]


    # Reorder the DataFrame columns
    df = df[new_column_order]
    df=df.drop_duplicates(keep='first')

    # Ensure consistent data types and handle NaN values
    for col in ['solidarity_tech_first_name', 'voter_first', 'solidarity_tech_last_name', 
                'voter_last', 'solidarity_tech_zip5', 'voter_zip']:
        df[col] = df[col].astype(str).fillna('')

    # **2. Exact Full Name & Zip Match**

    exact_name_street_match_df = df[
        (df['solidarity_tech_first_name'] == df['voter_first']) &
        (df['solidarity_tech_last_name'] == df['voter_last']) &
        (df['solidarity_tech_street_name_clean'] == df['voter_street_name_clean'])
    ]


    # Assign match type correctly
    exact_name_street_match_df.loc[:, 'match_type'] = 'Exact Full Name & Street Match'

    # Save exact matches
    exact_name_street_match_df.to_csv("/Users/avonleafisher/Downloads/voter match files/exact_name_street_matches.csv", index=False)


    # 13. Filter to best matches

    # Filter to keep only rows where 'is_highest_match_strength' is 1
    df = df[df['is_highest_match_strength'] == 1]

    # Create a temporary column to count non-null values in each row
    df['_non_null_count'] = df.notnull().sum(axis=1)

    # Sort by 'match_strength' (descending) and '_non_null_count' (descending) to prioritize most complete rows
    df = df.sort_values(by=['match_strength', '_non_null_count'], ascending=[False, False])

    # Drop duplicates based on 'solidarity_tech_id', keeping the first occurrence (the most complete row)
    df = df.drop_duplicates(subset=['solidarity_tech_id'], keep='first')

    # Drop the temporary column
    df = df.drop(columns=['_non_null_count'])
    # Save final file

    party_mapping = {
        "DEM": "Democrat",
        "BLK": "Unaffiliated",
        "WOR": "WFP",
        "REP": "Republican",
        "OTH": "Not registered"
    }
    df['phone_number'] = df['solidarity_tech_phone_number']
    df["Registration Status"] = df["party_enrollment"].map(party_mapping).fillna("Cannot Be Determined")

    df.to_csv("/Users/avonleafisher/Downloads/voter match files/final_combined_matches.csv", index=False)
    df.head(30)
    return (
        NickNamer,
        clean_apt_num,
        col,
        columns_to_null,
        df,
        df1,
        df2,
        df_matched,
        df_no_match,
        directions,
        exact_name_street_match_df,
        fuzz,
        is_nickname_match,
        match_columns,
        new_column_order,
        nn,
        normalize_street_name,
        np,
        party_mapping,
        re,
        suffixes,
    )


@app.cell
def __(df):
    len(df)
    return


@app.cell
def __(pd):
    ur = pd.read_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/solidarity_tech_without_registration.csv")
    return (ur,)


@app.cell
def __(ur):
    len(ur)
    return


@app.cell
def __(df, ur):
    ur[~ur['solidarity_tech_id'].isin(df['solidarity_tech_id'])].to_csv("/Users/avonleafisher/Downloads/not_determined.csv")
    return


@app.cell
def __(df):
    df[df['phone_number'].duplicated()]
    return


@app.cell
def __(df):
    df[df['phone_number'].duplicated()]
    return


@app.cell
def __(df):
    df[df['party_enrollment']=='REP']
    return


@app.cell
def __():
    return


@app.cell
def __(pd):
    df3 = pd.read_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/solidarity_tech_without_registration.csv")
    return (df3,)


@app.cell
def __(df3):
    df4 = df3[df3['solidarity_tech_zip5'].isna()==False]
    return (df4,)


@app.cell
def __(df4):
    df4['zip'] = [i[0:6] for i in df4['solidarity_tech_zip5']]
    return


@app.cell
def __(df4):
    zips = [
        '10001', '10002', '10003', '10004', '10005', '10006', '10007', '10009', '10010',
        '10011', '10012', '10013', '10014', '10016', '10017', '10018', '10019', '10020',
        '10021', '10022', '10023', '10024', '10025', '10026', '10027', '10028', '10029',
        '10030', '10031', '10032', '10033', '10034', '10035', '10036', '10037', '10038',
        '10039', '10040', '10041', '10044', '10048', '10069', '10103', '10111', '10112',
        '10115', '10119', '10128', '10152', '10153', '10154', '10162', '10165', '10167',
        '10169', '10170', '10171', '10172', '10173', '10177', '10271', '10278', '10279',
        '10280', '10282', '10301', '10302', '10303', '10304', '10305', '10306', '10307',
        '10308', '10309', '10310', '10312', '10314', '10451', '10452', '10453', '10454',
        '10455', '10456', '10457', '10458', '10459', '10460', '10461', '10462', '10463',
        '10464', '10465', '10466', '10467', '10468', '10469', '10470', '10471', '10472',
        '10473', '10474', '10475', '11001', '11004', '11005', '11040', '11101', '11102',
        '11103', '11104', '11105', '11106', '11201', '11203', '11204', '11205', '11206',
        '11207', '11208', '11209', '11210', '11211', '11212', '11213', '11214', '11215',
        '11216', '11217', '11218', '11219', '11220', '11221', '11222', '11223', '11224',
        '11225', '11226', '11228', '11229', '11230', '11231', '11232', '11233', '11234',
        '11235', '11236', '11237', '11238', '11239', '11354', '11355', '11356', '11357',
        '11358', '11360', '11361', '11362', '11363', '11364', '11365', '11366', '11367',
        '11368', '11369', '11370', '11371', '11372', '11373', '11374', '11375', '11377',
        '11378', '11379', '11385', '11411', '11412', '11413', '11414', '11415', '11416',
        '11417', '11418', '11419', '11420', '11421', '11422', '11423', '11426', '11427',
        '11428', '11429', '11430', '11432', '11433', '11434', '11435', '11436', '11691',
        '11692', '11693', '11694', '11697'
    ]


    df5 = df4[~df4['zip'].isin(zips)]
    return df5, zips


@app.cell
def __(df5):
    df5.to_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/ineligible.csv")
    return


@app.cell
def __(zips):
    zips
    return


@app.cell
def __(zips):
    zips['ZIPCODES'].astype('str').unique()
    return


@app.cell
def __(zips):
    zips
    return


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
