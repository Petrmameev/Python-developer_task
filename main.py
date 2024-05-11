import json

import pandas as pd
from bson import decode_file_iter


def bson_to_dataframe(filepath):
    with open(filepath, "rb") as file:
        data = [doc for doc in decode_file_iter(file)]
    return pd.DataFrame(data)


filepath = "sample_collection.bson"
df = bson_to_dataframe(filepath)
# print(df)


def bson_to_dataframe(data_bson):
    data = []
    with open(data_bson, "rb") as f:
        for document in decode_file_iter(f):
            data.append(document)
    return pd.DataFrame(data)


payments = bson_to_dataframe("sample_collection.bson")


def aggregate_payments(payments_df, dt_from, dt_upto, group_type):
    start_date = pd.to_datetime(dt_from)
    end_date = pd.to_datetime(dt_upto)
    payments_df["dt"] = pd.to_datetime(payments_df["dt"])
    mask = (payments_df["dt"] >= start_date) & (payments_df["dt"] <= end_date)
    filtered_payments = payments_df.loc[mask]
    filtered_payments_to_resample = filtered_payments[["dt", "value"]]

    if group_type == "day":
        full_range = pd.date_range(start=start_date, end=end_date, freq="D")
        resampled = filtered_payments_to_resample.resample("D", on="dt").sum()
        resampled = resampled.reindex(full_range, fill_value=0)
        dataset = resampled["value"].tolist()

    elif group_type == "month":
        full_range = pd.date_range(start=start_date, end=end_date, freq="MS")
        resampled = filtered_payments_to_resample.resample("MS", on="dt").sum()
        resampled = resampled.reindex(full_range, fill_value=0)
        dataset = resampled["value"].tolist()

    elif group_type == "hour":
        full_range = pd.date_range(start=start_date, end=end_date, freq="h")
        resampled = filtered_payments_to_resample.resample("h", on="dt").sum()
        resampled = resampled.reindex(full_range, fill_value=0)
        dataset = resampled["value"].tolist()

    labels = resampled.index.strftime("%Y-%m-%dT%H:%M:%S").tolist()
    formatted_json = (
        f'{{"dataset": {json.dumps(dataset)},\n"labels": {json.dumps(labels)}}}'
    )
    return formatted_json
