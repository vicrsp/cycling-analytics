# %% [markdown]
# ## PerfPro FIT files EDA

# %%
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime

import zipfile
from os import listdir
from os.path import isfile, join

import fitparse

# %% [markdown]
# ### Processing the zipped files

# %% [markdown]
# #### Extract the .fit files

# %%
path_to_read_zip_files = r'D:\perfpro_data\raw'
path_to_extract_zip_files = r'D:\perfpro_data\extracted'

zip_files = [f for f in listdir(path_to_read_zip_files) if isfile(join(path_to_read_zip_files, f))]

for file in zip_files:
    with zipfile.ZipFile(join(path_to_read_zip_files, file), 'r') as zip_ref:        
        for file_name in zip_ref.namelist():
            if file_name.endswith('.fit'):
                 zip_ref.extract(file_name, path_to_extract_zip_files)
                 
fit_files = [f for f in listdir(path_to_extract_zip_files) if isfile(join(path_to_extract_zip_files, f))]

# %% [markdown]
# ### Process .fit files

# %%
def fit_file_to_pandas(file_path):
    fitfile = fitparse.FitFile(file_path)
    # Iterate over all messages of type "record"
    # (other types include "device_info", "file_creator", "event", etc)
    file_data = []
    for record in fitfile.get_messages("record"):
        # Gets the record timestamp
        timestamp = next((x for x in record.fields if x.name == 'timestamp'), None)
        if timestamp is not None:
            for data in record:
                # Print the name and value of the data (and the units if it has any)
                if data.name != 'timestamp':
                    if data.units:
                        file_data.append((timestamp.value, data.name, data.value, data.units))
                    else:
                        file_data.append((timestamp.value, data.name, data.value, None))

    file_data_df = pd.DataFrame(file_data, columns=['timestamp', 'name', 'value', 'unit'])
    file_data_df['timestamp'] = file_data_df['timestamp'].apply(lambda x: x.strftime("%m-%d-%Y %H:%M:%S"))
    return file_data_df
# %%
# parse all files to the database
from data_model import CyclingAnalyticsDataModel

# create the data model
db_file = r'D:\perfpro_data\history.db'
md = CyclingAnalyticsDataModel(db_file)

for fit_file in tqdm(fit_files):
    file_path = join(path_to_extract_zip_files, fit_file)
    file_df = fit_file_to_pandas(file_path)
    fit_file_fields = fit_file.split("-")

    user = fit_file_fields[0].strip()
    train_name = " - ".join(fit_file_fields[2:])
    train_date = file_df['timestamp'][0]

    train_id = md.create_train(train_date, name=train_name, user=user)
    file_df['train_id'] = train_id
    md.save_train_fit_values(file_df)


