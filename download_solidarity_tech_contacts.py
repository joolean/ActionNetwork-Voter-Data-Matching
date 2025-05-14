#Before running, update file paths and input api key 

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
