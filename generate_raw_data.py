import pandas as pd
import random
from datetime import datetime, timedelta
import csv

with open('dbt/seeds/raw_sales_data.csv', 'r') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    # Extract number from "ORD-2999" format
    last_order_id_str = rows[-1]['order_id']
    last_order_id = int(last_order_id_str.split('-')[1])  # Split by '-' and take the second part

# Configuration
rows = 2000
data = []

# Possible values for realism
regions = ['North America', 'EMEA', 'APAC', 'LATAM', None]
status = ['completed', 'pending', 'cancelled', 'returned']

for i in range(rows):
    order_id = last_order_id + i
    user_id = random.randint(5000, 5500) # Repeat users for analysis
    
    # Simulate messy dates
    base_date = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))
    order_date = base_date.strftime('%Y-%m-%d') if i % 10 != 0 else base_date.isoformat()
    
    data.append({
        'order_id': f"ORD-{order_id}",
        'customer_id': user_id,
        'order_date': order_date,
        'region': random.choice(regions),
        'gross_amount': round(random.uniform(20.0, 500.0), 2),
        'tax_rate': "0.08", # String format to force transformation
        'discount_code': random.choice(['SAVE10', 'WELCOME', None, None, None]),
        'order_status': random.choice(status)
    })

with open('dbt/seeds/raw_sales_data.csv', 'a', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['order_id', 'customer_id', 'order_date', 'region', 'gross_amount', 'tax_rate', 'discount_code', 'order_status'])
    writer.writerows(data)

df = pd.DataFrame(data)
# df.to_csv('dbt/seeds/raw_sales_data.csv', index=False)
print(f"Generated {len(df)} rows and saved to raw_sales_data.csv")