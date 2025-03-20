import marimo

__generated_with = "0.8.22"
app = marimo.App(width="medium", app_title="Voter data matching")


@app.cell
def __():
    #write voter file to parquet

    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd


    input_file = "/Users/avonleafisher/Downloads/AllNYSVoters_20250310/AllNYSVoters_20250310.txt"
    output_parquet_file = "/Users/avonleafisher/Downloads/voter_file.parquet"  

    # Define the column names and data types
    columns = [
        "LASTNAME", "FIRSTNAME", "MIDDLENAME", "NAMESUFFIX", "RADDNUMBER", "RHALFCODE", "RPREDIRECTION", "RSTREETNAME", 
        "RPOSTDIRECTION", "RAPARTMENTTYPE", "RAPARTMENT", "RADDRNONSTD", "RCITY", "RZIP5", "RZIP4", "MAILADD1", 
        "MAILADD2", "MAILADD3", "MAILADD4", "DOB", "GENDER", "ENROLLMENT", "OTHERPARTY", "COUNTYCODE", "ED", 
        "LD", "TOWNCITY", "WARD", "CD", "SD", "AD", "LASTVOTERDATE", "PREVYEARVOTED", "PREVCOUNTY", "PREVADDRESS", 
        "PREVNAME", "COUNTYVRNUMBER", "REGDATE", "VRSOURCE", "IDREQUIRED", "IDMET", "STATUS", "REASONCODE", "INACT_DATE", 
        "PURGE_DATE", "SBOEID", "VoterHistory"
    ]

    dtype_dict = {
        "LASTNAME": str, "FIRSTNAME": str, "MIDDLENAME": str, "NAMESUFFIX": str, 
        "RADDNUMBER": str, "RHALFCODE": str, "RPREDIRECTION": str, "RSTREETNAME": str, 
        "RPOSTDIRECTION": str, "RAPARTMENTTYPE": str, "RAPARTMENT": str, "RADDRNONSTD": str, 
        "RCITY": str, "RZIP5": str, "RZIP4": str, "MAILADD1": str, "MAILADD2": str, "MAILADD3": str, 
        "MAILADD4": str, "DOB": str, "GENDER": str, "ENROLLMENT": str, "OTHERPARTY": str, 
        "COUNTYCODE": str, "ED": str, "LD": str, "TOWNCITY": str, "WARD": str, "CD": str, 
        "SD": str, "AD": str, "LASTVOTERDATE": str, "PREVYEARVOTED": str, "PREVCOUNTY": str, 
        "PREVADDRESS": str, "PREVNAME": str, "COUNTYVRNUMBER": str, "REGDATE": str, 
        "VRSOURCE": str, "IDREQUIRED": str, "IDMET": str, "STATUS": str, "REASONCODE": str, 
        "INACT_DATE": str, "PURGE_DATE": str, "SBOEID": str, "VoterHistory": str
    }

    # Create an iterator for reading the CSV file in chunks
    df_iter = pd.read_csv(input_file, sep=",", encoding="ISO-8859-1", on_bad_lines="skip", 
                          chunksize=500000, dtype=dtype_dict, names=columns, header=None)

    # Initialize the ParquetWriter for the first chunk
    first_chunk = next(df_iter)
    # Convert the first chunk to a PyArrow Table
    table = pa.Table.from_pandas(first_chunk)

    # Write the first chunk to the parquet file
    pq.write_table(table, output_parquet_file)

    # Append subsequent chunks to the parquet file using the schema from the first chunk
    with pq.ParquetWriter(output_parquet_file, table.schema) as writer:
        
        writer.write_table(table)  # Write the first chunk

        chunk_count = 1
        for chunk in df_iter:
            table = pa.Table.from_pandas(chunk, schema=table.schema)
            writer.write_table(table)  # Append each chunk
            chunk_count += 1

    print(f"Data written to {output_parquet_file}")
    return (
        chunk,
        chunk_count,
        columns,
        df_iter,
        dtype_dict,
        first_chunk,
        input_file,
        output_parquet_file,
        pa,
        pd,
        pq,
        table,
        writer,
    )


@app.cell
def __(people_df, pl, unknown_registration, voterfile):
    #Pull volunteers with unknown registration status
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
        api_key_input = ""
        api_key_input
        return (api_key_input,)


    @app.cell
    def _(api_key_input, mo):
        mo.stop(api_key_input is None)
        import requests
        import polars as pl

        def download_people(limit=50, max_people=8000):
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
        output_path = "/Users/avonleafisher/Downloads/AllNYSVoters_20250310/solidarity_tech_with_registration.csv"
        registered.write_csv(output_path)
        print(f"Saved {len(registered)} registered people to {output_path}")

        return unknown_registration



    #join Solidarity Tech to voter data. 
    # update join statement and path to first/last name as needed for initial matching 
    #             ON UPPER(v.FIRSTNAME) = UPPER(ur.solidarity_tech_first_name) 
               # ON UPPER(v.LASTNAME) = UPPER(ur.solidarity_tech_LAST_name) 
            # match_results_df.write_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/match_results_fn.csv")
            # match_results_df.write_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/match_results_ln.csv")




    @app.cell
    def match_voter_data(mo, unknown_registration):
        match_results = mo.sql("""
            SELECT ur.solidarity_tech_id, ur.solidarity_tech_first_name, 
                   ur.solidarity_tech_last_name, ur.solidarity_tech_address1, 
                   ur.solidarity_tech_address2 AS solidarity_tech_apt_num, ur.solidarity_tech_zip5, 
                   ur.solidarity_tech_phone_number, ur.solidarity_tech_email,
                    v.FIRSTNAME AS voter_first, v.LASTNAME AS voter_last, 
                   v.RADDNUMBER AS voter_house_number, v.RSTREETNAME AS voter_street_name, 
                   v.RAPARTMENT AS voter_apt_num, v.RZIP5 AS voter_zip,  
                   v.SBOEID AS boe_id, v.STATUS AS registration_status, 
                   v.ENROLLMENT AS party_enrollment,
                   v.MAILADD1 AS voter_mailing_address1, v.MAILADD2 AS voter_mailing_address2, 
                   v.MAILADD3 AS voter_mailing_address3, v.MAILADD4 AS voter_mailing_address4, 
                   v.DOB AS voter_dob, v.PREVNAME AS voter_previous_name, 
                   v.PREVADDRESS AS voter_previous_address
            FROM unknown_registration AS ur
            LEFT JOIN voterfile AS v
               ON UPPER(v.LASTNAME) = UPPER(ur.solidarity_tech_last_name) 
                AND v.RZIP5 = ur.solidarity_tech_zip5;
        """)

        #output file with match results to be used in next cell
        match_results_df = pl.DataFrame(match_results)
        match_results_df.write_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/match_results_fn.csv")
        
        print(match_results_df.head())

        return match_results_df

    if __name__ == "__main__":
        app.run()

    return (
        __generated_with,
        app,
        marimo,
        match_voter_data,
        separate_by_reg_status,
    )


@app.cell
def __():
    import pandas as pd
    from nicknames import NickNamer
    from fuzzywuzzy import fuzz
    import numpy as np
    import re

    #read in results, get rows with exact address match, write exact matches to csv, exclude from df
    ln = pd.read_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/match_results_fn.csv")
    fn = pd.read_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250310/match_results_ln.csv")

    df = pd.concat([ln, fn])
    df = df.apply(lambda x: x.str.upper() if x.dtype == 'object' else x)

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
        'nickname_match', 'street_name_match', 'house_num_match',
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

    # Remove exact matches from the main DataFrame
    df = df[~df['solidarity_tech_id'].isin(exact_name_street_match_df['solidarity_tech_id'].unique())]

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
    df.to_csv("/Users/avonleafisher/Downloads/voter match files/final_combined_matches.csv", index=False)
    df.head(30)

    return (
        NickNamer,
        clean_apt_num,
        col,
        columns_to_null,
        df,
        df_matched,
        df_no_match,
        directions,
        exact_name_street_match_df,
        fn,
        fuzz,
        is_nickname_match,
        ln,
        match_columns,
        new_column_order,
        nn,
        normalize_street_name,
        np,
        pd,
        re,
        suffixes,
    )


@app.cell
def __(df):
    df
    return


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
