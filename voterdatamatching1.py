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
        api_key_input = ""
        api_key_input
        return (api_key_input,)


    @app.cell
    def _(api_key_input, mo):
        mo.stop(api_key_input is None)
        import requests
        import polars as pl

        def download_people(limit=100, max_people=200000):
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
                        "solidarity_tech_boe_id": p.get("custom_user_properties", {}).get("boe-state-file-id")

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

        # Load the data
        ppl = mo.sql("""
            SELECT *
            FROM people_df
        """)

        # Preview selected columns 
        print(f"Preview of 'solidarity_tech_boe_id' and 'solidarity_tech_registration_status':\n")
        print(ppl.select(["solidarity_tech_boe_id", "solidarity_tech_registration_status"]).head())


        # Flatten the registration_status field
        if isinstance(ppl.schema.get("solidarity_tech_registration_status"), pl.List):
            ppl = ppl.with_columns(
                pl.col("solidarity_tech_registration_status")
                .arr.explode()
                .struct.field("label")
                .alias("registration_label")
            )
        else:
            ppl = ppl.with_columns(
                pl.col("solidarity_tech_registration_status").cast(pl.Utf8).alias("registration_label")
            )

        # Flatten VAN ID as List[Struct]
        if isinstance(ppl.schema.get("solidarity_tech_boe_id"), pl.List):
            print("Flattening 'solidarity_tech_boe_id' field...")
            ppl = ppl.with_columns(
                pl.col("solidarity_tech_boe_id")
                .arr.explode()
                .struct.field("label")
                .alias("solidarity_tech_boe_id")
            )
        elif "solidarity_tech_boe_id" in ppl.columns:
            print(f"'solidarity_tech_boe_id' is not a List[Struct]. Casting to Utf8.\n")
            ppl = ppl.with_columns(
                pl.col("solidarity_tech_boe_id").cast(pl.Utf8).alias("voter_boe_id")
            )

        # Filter by BOE ID
        unknown_registration = ppl.filter(pl.col("solidarity_tech_boe_id").is_null())
        registered = ppl.filter(pl.col("solidarity_tech_boe_id").is_not_null())

        # ---- FLATTEN REMAINING List[Struct] FIELDS ----
        struct_list_cols = [
            c for c in registered.columns
            if isinstance(registered.schema[c], pl.List) and isinstance(registered.schema[c].inner, pl.Struct)
        ]

        for col in struct_list_cols:
            registered = registered.with_columns(
                pl.col(col).arr.explode().struct.field("label").alias(f"{col}_label")
            ).drop(col)

        # Save clean registered data
        registered.write_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250402/solidarity_tech_with_registration.csv")

        # Save unknown registration data
        unknown_registration_pd = unknown_registration.to_pandas()
        unknown_registration_pd.to_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250402/solidarity_tech_unknown.csv")


        return

    if __name__ == "__main__":
        app.run()
    return __generated_with, app, marimo, separate_by_reg_status


@app.cell
def __():
    import pandas as pd

    voterfile = pd.read_parquet( "/Users/avonleafisher/Downloads/AllNYSVoters_20250402/voterfile.parquet", engine="pyarrow")

    # Load Solidarity Tech file, ensuring ZIP code is a string
    sol_tech = pd.read_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250402/solidarity_tech_unknown.csv", dtype={"solidarity_tech_zip5": str})

    print(len(sol_tech))
    print(len(voterfile))

    # Ensure ZIP codes are strings and take first 5 characters
    voterfile["RZIP5"] = voterfile["RZIP5"].astype(str).str[:5]
    sol_tech["solidarity_tech_zip5"] = sol_tech["solidarity_tech_zip5"].astype(str).str[:5]

    # Convert names to uppercase for case-insensitive matching
    voterfile["LASTNAME"] = voterfile["LASTNAME"].str.upper()
    voterfile["FIRSTNAME"] = voterfile["FIRSTNAME"].str.upper()
    sol_tech["solidarity_tech_last_name"] = sol_tech["solidarity_tech_last_name"].str.upper()
    sol_tech["solidarity_tech_first_name"] = sol_tech["solidarity_tech_first_name"].str.upper()

    # Match on last name + ZIP
    match_ln = sol_tech.merge(
        voterfile,
        left_on=["solidarity_tech_last_name", "solidarity_tech_zip5"],
        right_on=["LASTNAME", "RZIP5"],
        how="inner"
    )

    # Match on first name + ZIP
    match_fn = sol_tech.merge(
        voterfile,
        left_on=["solidarity_tech_first_name", "solidarity_tech_zip5"],
        right_on=["FIRSTNAME", "RZIP5"],
        how="inner"
    )

    # Combine match results
    match_results_df = pd.concat([match_ln, match_fn], ignore_index=True)



    # Select and rename relevant columns
    match_results_df = match_results_df.rename(columns={
        "solidarity_tech_address2": "solidarity_tech_apt_num",
        "FIRSTNAME": "voter_first",
        "LASTNAME": "voter_last",
        "RADDNUMBER": "voter_house_number",
        "RSTREETNAME": "voter_street_name",
        "RAPARTMENT": "voter_apt_num",
        "RZIP5": "voter_zip",
        "SBOEID": "voter_boe_id",
        "STATUS": "registration_status",
        "ENROLLMENT": "party_enrollment",
        "MAILADD1": "voter_mailing_address1",
        "MAILADD2": "voter_mailing_address2",
        "MAILADD3": "voter_mailing_address3",
        "MAILADD4": "voter_mailing_address4",
        "DOB": "voter_dob",
        "PREVNAME": "voter_previous_name",
        "PREVADDRESS": "voter_previous_address"
    })[[
        "solidarity_tech_id", "solidarity_tech_first_name", "solidarity_tech_last_name", 
        "solidarity_tech_address1", "solidarity_tech_apt_num", "solidarity_tech_zip5", 
        "solidarity_tech_phone_number", "solidarity_tech_email", "voter_first", "voter_last", 
        "voter_house_number", "voter_street_name", "voter_apt_num", "voter_zip", "voter_boe_id", 
        "registration_status", "party_enrollment", "voter_mailing_address1", 
        "voter_mailing_address2", "voter_mailing_address3", "voter_mailing_address4", 
        "voter_dob", "voter_previous_name", "voter_previous_address"
    ]]


    # Save to CSV
    match_results_df.to_csv(
        "/Users/avonleafisher/Downloads/AllNYSVoters_20250402/match_results_fn_and_ln.csv",
        index=False
    )
    return match_fn, match_ln, match_results_df, pd, sol_tech, voterfile


@app.cell
def __(pd):
    from nicknames import NickNamer
    from fuzzywuzzy import fuzz
    import numpy as np
    import re

    df=pd.read_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250402/match_results_fn_and_ln.csv")

    #uppercase string cols
    df = df.apply(lambda x: x.str.upper() if x.dtype == 'object' else x)

    # create house # and street name cols from address1 col, convert to uppercase
    df[['solidarity_tech_house_num', 'solidarity_tech_street_name']] = df['solidarity_tech_address1']\
        .str.extract(r'^(\d+)\s+(.*)', expand=True)

    df['solidarity_tech_house_num'] = df['solidarity_tech_house_num']
    df['solidarity_tech_street_name'] = df['solidarity_tech_street_name']

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

    # 9. If match_strength <=2 1, remove voter columns
    columns_to_null = ['voter_first', 'voter_last', 'voter_house_number', 'voter_street_name_clean',
                       'voter_apt_num', 'voter_zip', 'voter_boe_id', 'registration_status',
                       'party_enrollment', 'voter_mailing_address1', 'voter_mailing_address2',
                       'voter_mailing_address3', 'voter_mailing_address4', 'voter_dob', 'voter_previous_name',
                       'voter_previous_address']

    df.loc[df['match_strength'] <= 1, columns_to_null] = None

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
        'voter_boe_id', 'registration_status', 'party_enrollment',
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

    # Filter to keep only rows where 'is_highest_match_strength' is 1
    df = df[df['is_highest_match_strength'] == 1]

    # Create a temporary column to count non-null values in each row
    df['_non_null_count'] = df.notnull().sum(axis=1)

    # Sort by 'match_strength' (descending) and '_non_null_count' (descending) to prioritize most complete rows
    df = df.sort_values(by=['match_strength', '_non_null_count'], ascending=[False, False])

    #label duplicates
    df['is_duplicate'] = df.duplicated(subset=['solidarity_tech_id'], keep='first').astype(int)

    # Generate ZIP code ranges for each borough
    manhattan_zips = list(range(10001, 10293))        # 10001–10292
    brooklyn_zips = list(range(11201, 11240)) + [11249]
    queens_zips = (
        list(range(11004, 11006)) +    # Still keep this
        list(range(11101, 11121)) +
        list(range(11351, 11698)) +
        [11001]  # Add this if you want to include it
    )
    bronx_zips = list(range(10451, 10476))            # 10451–10475
    staten_island_zips = list(range(10301, 10315))    # 10301–10314

    # Combine all borough ZIPs and convert to 5-character strings
    nyc_zip_codes = [
        str(zipcode).zfill(5) for zipcode in (
            manhattan_zips + brooklyn_zips + queens_zips + bronx_zips + staten_island_zips
        )
    ]

    #clean zips
    df['solidarity_tech_zip5'] = (
        df['solidarity_tech_zip5']
        .astype(str)
        .str.strip()
        .str.replace(".0", "", regex=False)
        .str.split("-").str[0]
        .str.zfill(5)
    )


    #Flag exact full match
    df['exact_full_match'] = (
        (df['solidarity_tech_first_name'] == df['voter_first']) &
        (df['solidarity_tech_last_name'] == df['voter_last']) &
        (df['solidarity_tech_street_name_clean'] == df['voter_street_name_clean'])
    )

    #Find ambiguous IDs (same ID with multiple match_strength >= 5)
    match_counts = df[df['match_strength'] >= 5].groupby('solidarity_tech_id').size()
    ambiguous_ids = match_counts[match_counts > 1].index

    # Assign match_types
    def assign_match_type(row):
        zip5 = row.get("solidarity_tech_zip5", "")
        if pd.notna(zip5) and zip5 not in nyc_zip_codes:
            return "Ineligible: Address outside of NYC"
        if row['solidarity_tech_id'] in ambiguous_ids:
            return "Cannot be determined"
        if row['exact_full_match']:
            return "Perfect match"
        if row['match_strength'] >= 5:
            return "Strong match"
        if 2 <= row['match_strength'] <= 4:
            return "Weak match"
        return "No match"

    df['match_type'] = df.apply(assign_match_type, axis=1)

    df['match_type'] = df.apply(
        lambda row: 'No match'
        if (
            (
                (row['first_name_match'] == 0 and row['last_name_match'] == 0) or
                (row['first_name_match'] == 0 and row['fuzzy_last_name_match'] == 0) or
                (row['nickname_match'] == 0 and row['last_name_match'] == 0)
            )
            and row.get('street_name_match', 0) != 1
        )
        else row['match_type'],
        axis=1
    )

    party_mapping = {
        "DEM": "Democrat",
        "BLK": "Unaffiliated",
        "WOR": "WFP",
        "REP": "Republican",
        "OTH": "Not registered"
    }
    df['phone_number'] = df['solidarity_tech_phone_number']
    df["Registration Status"] = df["party_enrollment"].map(party_mapping).fillna("Cannot Be Determined")
    df.loc[df['match_type'] == 'Ineligible: Address outside of NYC', "Registration Status"] = 'Not eligible'
    df.loc[df['match_type'] == 'No match', "Registration Status"] = 'Not Registered'


    df.to_csv("/Users/avonleafisher/Downloads/AllNYSVoters_20250402/final_combined_matches.csv", index=False)
    return (
        NickNamer,
        ambiguous_ids,
        assign_match_type,
        bronx_zips,
        brooklyn_zips,
        clean_apt_num,
        col,
        columns_to_null,
        df,
        df_matched,
        df_no_match,
        directions,
        fuzz,
        is_nickname_match,
        manhattan_zips,
        match_columns,
        match_counts,
        new_column_order,
        nn,
        normalize_street_name,
        np,
        nyc_zip_codes,
        party_mapping,
        queens_zips,
        re,
        staten_island_zips,
        suffixes,
    )


if __name__ == "__main__":
    app.run()
