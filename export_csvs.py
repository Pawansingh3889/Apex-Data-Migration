import duckdb
import pandas as pd

conn = duckdb.connect('data/apex.duckdb', read_only=True)

print('All tables:')
print(conn.execute('SHOW ALL TABLES').fetchdf().to_string())

conn.execute("COPY main_marts.fct_server_health TO 'data/fct_server_health.csv' (HEADER, DELIMITER ',')")
print('Exported fct_server_health.csv')

conn.execute("COPY main_marts.fct_delivery_by_state TO 'data/fct_delivery_by_state.csv' (HEADER, DELIMITER ',')")
print('Exported fct_delivery_by_state.csv')

conn.execute("COPY main_marts.dim_category_sentiment TO 'data/dim_category_sentiment.csv' (HEADER, DELIMITER ',')")
print('Exported dim_category_sentiment.csv')

print('\n--- Column names ---')
for f in [
    'data/processed_logs.csv',
    'data/delivery_analysis.csv',
    'data/category_sentiment.csv',
    'data/fct_server_health.csv',
    'data/fct_delivery_by_state.csv',
    'data/dim_category_sentiment.csv'
]:
    df = pd.read_csv(f, nrows=0)
    print(f'\n{f}:')
    print(list(df.columns))

conn.close()
print('\nDone.')