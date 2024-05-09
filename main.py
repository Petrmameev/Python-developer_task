import json

import pandas as pd
from bson import decode_file_iter

data = "sample_collection.metadata.json"


# def bson_to_dataframe(filepath):
#     with open(filepath, 'rb') as file:
#         data = [doc for doc in decode_file_iter(file)]
#     return pd.DataFrame(data)
# filepath = 'sample_collection.bson'
# df = bson_to_dataframe(filepath)
# print(df.head())


def bson_to_dataframe(data_bson):
    data = []
    with open(data_bson, "rb") as f:
        for document in decode_file_iter(f):
            data.append(document)
    return pd.DataFrame(data)


data_bson = "sample_collection.bson"
payments = bson_to_dataframe("sample_collection.bson")


def aggregate_payments(payments_df, dt_from, dt_upto, group_type):
    start_date = pd.to_datetime(dt_from)
    end_date = pd.to_datetime(dt_upto)
    payments_df["dt"] = pd.to_datetime(payments_df["dt"])
    mask = (payments_df["dt"] >= start_date) & (payments_df["dt"] <= end_date)
    filtered_payments = payments_df.loc[mask]
    filtered_payments_to_resample = filtered_payments[["dt", "value"]]
    if group_type == "day":
        resampled = filtered_payments_to_resample.resample("D", on="dt").sum()
    elif group_type == "month":
        # 'MS' stands for month start frequency
        resampled = filtered_payments_to_resample.resample("MS", on="dt").sum()
    elif group_type == "hour":
        resampled = filtered_payments_to_resample.resample("h", on="dt").sum()

    dataset = resampled["value"].tolist()
    labels = resampled.index.strftime("%Y-%m-%dT%H:%M:%S").tolist()

    response = {"dataset": dataset, "labels": labels}
    return json.dumps(response)


# input_data = {
#     "dt_from": "2022-09-01T00:00:00",
#     "dt_upto": "2022-12-31T23:59:00",
#     "group_type": "month"
# }

# input_data = {
#    "dt_from": "2022-10-01T00:00:00",
#    "dt_upto": "2022-11-30T23:59:00",
#    "group_type": "day"
# }

input_data = {
    "dt_from": "2022-02-01T00:00:00",
    "dt_upto": "2022-02-02T00:00:00",
    "group_type": "hour",
}


result = aggregate_payments(
    payments, input_data["dt_from"], input_data["dt_upto"], input_data["group_type"]
)

print(result)
