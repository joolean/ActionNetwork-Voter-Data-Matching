import marimo

__generated_with = "0.8.22"
app = marimo.App(width="medium")


@app.cell
def __():
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    input_path = ""
    output_path =  ""

    # Define column names as per FOIL data layout
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

    # NYC county codes: Bronx, Kings, New York, Queens, Richmond
    nyc_county_codes = {"03", "24", "31", "41", "43"}

    # Read file using the correct delimiter settings for quotes and comma separation
    chunk_iter = pd.read_csv(
        input_path,
        sep=",",
        quotechar="“",  
        doublequote=False,  
        quoting=3,  
        header=None,
        names=columns,
        chunksize=100000,
        dtype=str,
        encoding="ISO-8859-1",  # Use encoding that handles special characters
        engine='python',  # Use the Python engine as fallback for handling quotes
        on_bad_lines='skip'  # Skip lines with too many fields
    )

    parquet_writer = None

    for i, chunk in enumerate(chunk_iter):
        # Filter for New York City counties
        chunk_filtered = chunk[chunk["COUNTYCODE"].isin(nyc_county_codes)]
        if chunk_filtered.empty:
            continue

        # Convert the filtered chunk to a PyArrow table
        table = pa.Table.from_pandas(chunk_filtered, preserve_index=False)

        # Initialize the ParquetWriter if it's the first chunk
        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(output_path, table.schema)
        
        # Write the chunk to Parquet
        parquet_writer.write_table(table)
        print(f"Processed chunk {i + 1} with {len(chunk_filtered)} NYC rows")

    # Close the ParquetWriter to finalize the file
    if parquet_writer:
        parquet_writer.close()

    return (
        chunk,
        chunk_filtered,
        chunk_iter,
        columns,
        i,
        input_path,
        nyc_county_codes,
        output_path,
        pa,
        parquet_writer,
        pd,
        pq,
        table,
    )


if __name__ == "__main__":
    app.run()
