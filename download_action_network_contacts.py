#Before running, update file paths and input api key 

import marimo

__generated_with = "0.11.14"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return mo


# input api key
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
        url = "https://nocodb.socialists.nyc/api/v2/tables/mlymauwjqvwfrby/records"
        headers = {"accept": "application/json", "xc-token": f"{api_key_input}"}
        all_people, offset = [], 0

        while len(all_people) < max_people:
            response = requests.get(
                f"{url}?limit={limit}&offset={offset}&where=&viewId=vwsr98q0072vs2rl",
                headers=headers
            )
            people_batch = [
                {
                    "action_network_first_name": p.get("first_name"),
                    "action_network_last_name": p.get("last_name"),
                    "action_network_phone_number": p.get("can2_phone"),
                    "action_network_email": p.get("email"),
                    "action_network_address": p.get("can2_user_address"),
                    "action_network_city": p.get("can2_user_city"),
                    "action_network_county": p.get("can2_county"),
                    "action_network_state": p.get("can2_state_abbreviated"),
                    "action_network_zip_code": p.get("zip_code"),
                } for p in response.json().get("list", [])
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


@app.cell
def store_to_file(mo):

    # Load the data
    ppl = mo.sql("""
        SELECT *
        FROM people_df
    """)

    # Save unknown registration data
    ppl_pd = ppl.to_pandas()
    ppl_pd.to_csv("/tmp/action_network_all.csv")


if __name__ == "__main__":
    app.run()
