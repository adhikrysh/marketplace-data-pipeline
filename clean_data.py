import pandas as pd

customers_df = pd.read_csv("customers.csv")
"""first_name/ last_name is not .strip(), email columns missing, join_date with inconsistent formats and some of the years are in the city column, some cities are in the column right of cities (misaligned) """


listings_df = pd.read_csv("listings.csv")
"""listing_date has similar issue - inconsistent formats and some years are on the right of listing_date"""

orders_df = pd.read_csv("orders.csv")
"""order_status has incorrect spelling and case. event timestamp is in inconsistent format"""