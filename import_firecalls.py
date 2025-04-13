import pandas as pd
import pyodbc

# ✅ Path to the downloaded CSV file
file_path = r'C:\Users\szulc\Downloads\fire-department-calls-2019-2022.csv'

# Load CSV
df = pd.read_csv(file_path, sep=';', quotechar='"')

# Rename columns to match your SQL table
df.rename(columns={
    'Battalion #': 'battalion',
    '# Calls': 'calls',
    '# Units responded': 'units_responded',
    'Call Type': 'call_type',
    'Category Type': 'category_type',
    'Week': 'week',
    'Year': 'year',
    'Date': 'date',
    'Week Number': 'week_number'
}, inplace=True)

# Sanitize column values
df['week_number'] = df['week_number'].astype(str).str.replace('Week', '').str.strip()
df['week_number'] = pd.to_numeric(df['week_number'], errors='coerce')

for col in ['battalion', 'call_type', 'category_type', 'date']:
    df[col] = df[col].fillna('').astype(str).str.strip()

# Connect to SQL Server
conn = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=localhost;DATABASE=JCFireData;Trusted_Connection=yes;'
)
cursor = conn.cursor()

# Insert into table
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO fire_calls (
            battalion, calls, units_responded, call_type, category_type,
            week, year, date, week_number
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    row['battalion'],
    int(row['calls']) if not pd.isnull(row['calls']) else None,
    int(row['units_responded']) if not pd.isnull(row['units_responded']) else None,
    row['call_type'],
    row['category_type'],
    int(row['week']) if not pd.isnull(row['week']) else None,
    int(row['year']) if not pd.isnull(row['year']) else None,
    row['date'],
    int(row['week_number']) if not pd.isnull(row['week_number']) else None)

conn.commit()
cursor.close()
conn.close()

print("✅ Import complete!")

