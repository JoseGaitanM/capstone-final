import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import random



# Define list of subscription types
subscriptions = [
    {"subscription": "Basic", "numberOfChannels": 50, "extras": {}},
    {"subscription": "Premium", "numberOfChannels": 100, "extras": {"HBO": "4", "Cinemax": "3"}},
    {"subscription": "Ultimate", "numberOfChannels": 200, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "3", "Sports Package": "2"}},
    {"subscription": "Sports", "numberOfChannels": 75, "extras": {"Sports Package": "5"}},
    {"subscription": "Entertainment", "numberOfChannels": 75, "extras": {"Showtime": "4", "Kids Package": "3"}},
    {"subscription": "News", "numberOfChannels": 50, "extras": {"CNN": "5", "Fox News": "3"}},
    {"subscription": "Movies", "numberOfChannels": 100, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "3"}},
    {"subscription": "Family", "numberOfChannels": 75, "extras": {"Kids Package": "5", "DVR": "3"}},
    {"subscription": "Premium Plus", "numberOfChannels": 200, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "4", "Sports Package": "3", "DVR": "2"}},
    {"subscription": "Custom", "numberOfChannels": 150, "extras": {}}
]

# Create DataFrame from list of subscriptions
df = pd.DataFrame(subscriptions)

# Write DataFrame to Parquet file
pq.write_table(pa.Table.from_pandas(df), 'data/files/suscriptions/subscriptions.parquet')
