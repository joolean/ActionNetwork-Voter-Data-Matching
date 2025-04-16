import marimo

__generated_with = "0.8.22"
app = marimo.App(width="medium")


@app.cell
def __():
    import csv
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    input_path = "/Users/avonleafisher/Downloads/AllNYSVoters_20250402/AllNYSVoters_20250402.txt"
    cleaned_path = "/Users/avonleafisher/Downloads/AllNYSVoters_20250402/cleaned_voterfile.txt"
    output_path = "/Users/avonleafisher/Downloads/AllNYSVoters_20250402/voterfile.parquet"

    columns = [
        "LASTNAME", "FIRSTNAME", "MIDDLENAME", "NAMESUFFIX", "RADDNUMBER", "RHALFCODE",
        "RPREDIRECTION", "RSTREETNAME", "RPOSTDIRECTION", "RAPARTMENTTYPE", "RAPARTMENT",
        "RADDRNONSTD", "RCITY", "RZIP5", "RZIP4", "MAILADD1", "MAILADD2", "MAILADD3",
        "MAILADD4", "DOB", "GENDER", "ENROLLMENT", "OTHERPARTY", "COUNTYCODE", "ED", "LD",
        "TOWNCITY", "WARD", "CD", "SD", "AD", "LASTVOTERDATE", "PREVYEARVOTED",
        "PREVCOUNTY", "PREVADDRESS", "PREVNAME", "COUNTYVRNUMBER", "REGDATE", "VRSOURCE",
        "IDREQUIRED", "IDMET", "STATUS", "REASONCODE", "INACT_DATE", "PURGE_DATE",
        "SBOEID", "VOTERHISTORY"
    ]

    # Clean and filter valid lines. Flag invalid lines for further cleaning. 
    with open(input_path, encoding="ISO-8859-1") as infile, open(cleaned_path, "w", encoding="ISO-8859-1") as outfile:
        reader = csv.reader(infile, delimiter=",", quotechar='"')
        writer = csv.writer(outfile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for i, row in enumerate(reader, 1):
            if len(row) == 47:
                writer.writerow(row)
            else:
                print(f"Skipping invalid line {i} (fields: {len(row)})")

    # Define function to enforce string schema
    def enforce_string_types(df):
        for col in columns:
            if col not in df.columns:
                df[col] = ""
            df[col] = df[col].astype(str)
        return df[columns]

    # Read cleaned file in chunks
    chunk_iter = pd.read_csv(
        cleaned_path,
        sep=",",
        quotechar='"',
        doublequote=True,
        quoting=0,
        header=None,
        names=columns,
        chunksize=100000,
        dtype=str,
        encoding="ISO-8859-1",
        engine='python'
    )

    # Write to Parquet
    first_chunk = enforce_string_types(next(chunk_iter))
    print("Processing chunk 1 with", len(first_chunk), "rows")
    table = pa.Table.from_pandas(first_chunk)
    pqwriter = pq.ParquetWriter(output_path, table.schema)
    pqwriter.write_table(table)

    for i, chunk in enumerate(chunk_iter, start=2):
        print(f"Processing chunk {i} with {len(chunk)} rows")
        chunk = enforce_string_types(chunk)
        table = pa.Table.from_pandas(chunk)
        pqwriter.write_table(table)

    pqwriter.close()
    print("Parquet writing complete.")

    return (
        chunk,
        chunk_iter,
        cleaned_path,
        columns,
        csv,
        enforce_string_types,
        first_chunk,
        i,
        infile,
        input_path,
        outfile,
        output_path,
        pa,
        pd,
        pq,
        pqwriter,
        reader,
        row,
        table,
        writer,
    )


@app.cell
def __(pd):
    parquet_path = "/Users/avonleafisher/Downloads/AllNYSVoters_20250402/voterfile.parquet"

    # NYC county codes from the FOIL layout:
    nyc_county_codes = {"24", "31", "41", "43", "60"}  # Kings, New York, Queens, Richmond, Bronx

    df = pd.read_parquet(parquet_path, engine="pyarrow")
    print(f"Loaded {len(df):,} total rows")

    # Filter to NYC counties
    df_nyc = df[df["COUNTYCODE"].isin(nyc_county_codes)]
    print(f"Filtered to {len(df_nyc):,} NYC rows")

    # Overwrite the Parquet file with the filtered data
    df_nyc.to_parquet(parquet_path, index=False, engine="pyarrow")
    print("NYC-only data written back to Parquet")
    return df, df_nyc, nyc_county_codes, parquet_path


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
