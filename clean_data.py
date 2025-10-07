import pandas as pd

#instructions for AI
""" 
•  Direct: Formulate precise prompts to guide the AI. 
•  Validate & Debug: Test the AI's output for correctness, efficiency, and adherence to best 
practices. 
•  Critique  &  Integrate:  Combine  and  refactor  AI-generated  code  into  a  robust,  well-
designed solution. 
•  Justify: Explain why your final design is superior to the AI's initial suggestions.
"""

customers_df = pd.read_csv('customers.csv')
#problems: missing email (clmn 4), date incorrect formats (clm 5), city (clmn 6 contains date) & clmn 7 contains countries 

#for every column in customers_df, if column is type string -> remove any whitespace
customers_df_obj = customers_df.select_dtypes(['object'])

customers_df[customers_df_obj.columns] = customers_df_obj.apply(lambda x: x.str.strip())

#if customers_df['email'] is empty -> fill up with na 
customers_df['email'] = customers_df['email'].replace('', pd.NA).fillna('na')

#column 6 has some 2023 or 2024 values
#if column 6 is 2023/ 2024 then join column 5 and 6. column 7 to transfer to column 6. 
#convert new column 5 to YYYY-MM-DD. 







import pandas as pd

customers_df = pd.read_csv("customers.csv")
"""first_name/ last_name is not .strip(), email columns missing, join_date with inconsistent formats and some of the years are in the city column, some cities are in the column right of cities (misaligned) """


listings_df = pd.read_csv("listings.csv")
"""listing_date has similar issue - inconsistent formats and some years are on the right of listing_date"""

orders_df = pd.read_csv("orders.csv")
"""order_status has incorrect spelling and case. event timestamp is in inconsistent format"""