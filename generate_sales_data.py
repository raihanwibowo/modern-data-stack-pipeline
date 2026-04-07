import csv
import random
from datetime import datetime, timedelta

# Read existing data to get the last order_id
with open('dbt/seeds/raw_sales.csv', 'r') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    last_order_id = int(rows[-1]['order_id'])

# Product IDs and prices
products = {
    501: 29.99, 502: 49.99, 503: 19.99, 504: 39.99, 505: 15.99,
    506: 24.99, 507: 89.99, 508: 199.99, 509: 34.99, 510: 44.99,
    511: 54.99, 512: 74.99, 513: 129.99, 514: 64.99, 515: 84.99,
    516: 94.99, 517: 29.99, 518: 109.99, 519: 149.99, 520: 59.99,
    521: 79.99, 522: 169.99, 523: 44.99, 524: 139.99, 525: 249.99,
    526: 34.99, 527: 54.99, 528: 119.99, 529: 24.99, 530: 179.99,
    531: 99.99, 532: 74.99, 533: 44.99, 534: 159.99, 535: 89.99,
    536: 29.99, 537: 219.99, 538: 64.99, 539: 189.99, 540: 54.99,
    541: 124.99, 542: 299.99, 543: 79.99, 544: 149.99, 545: 109.99,
    546: 39.99, 547: 269.99, 548: 59.99, 549: 179.99, 550: 94.99
}

# Customer IDs (101-200)
customer_ids = list(range(101, 201))

# Date range: 2024-01-01 to now (April 1, 2026)
start_date = datetime(2024, 1, 1)
end_date = datetime.now()
date_range_days = (end_date - start_date).days

# Generate 300 new rows with random dates between 2024-01-01 and now
new_rows = []

for i in range(1, 301):
    order_id = last_order_id + i
    customer_id = random.choice(customer_ids)
    product_id = random.choice(list(products.keys()))
    quantity = random.randint(1, 12)
    price = products[product_id]
    
    # Random date between 2024-01-01 and now
    random_days = random.randint(0, date_range_days)
    order_date = (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d')
    
    new_rows.append({
        'order_id': order_id,
        'customer_id': customer_id,
        'product_id': product_id,
        'quantity': quantity,
        'price': price,
        'order_date': order_date
    })

# Append to existing file
with open('dbt/seeds/raw_sales.csv', 'a', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['order_id', 'customer_id', 'product_id', 'quantity', 'price', 'order_date'])
    writer.writerows(new_rows)

print(f"Added 300 new rows. Last order_id: {last_order_id + 300}")
print(f"Date range: 2024-01-01 to {end_date.strftime('%Y-%m-%d')}")
