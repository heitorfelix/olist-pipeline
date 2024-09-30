import os
import pandas as pd
import random
import numpy as np
import shutil

from datetime import timedelta
import uuid

pd.options.mode.chained_assignment = None
ORDERS_DATE_COLUMNS = ['order_purchase_timestamp','order_approved_at','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date']
ORDER_ITEMS_DATE_COLUMNS = ['shipping_limit_date']

def convert_dates(df, dates):
    for date in dates:
        df[date] = pd.to_datetime(df[date])
    return df

# def parse_order_items(order_items):
#     order_items['shipping_limit_date'] = pd.to_datetime(order_items['shipping_limit_date'] )
#     return order_items

def full_load_all_tables():

    orders = pd.read_csv('data/olist_orders_dataset.csv').convert_dtypes()

    orders = convert_dates(df = orders, dates = ORDERS_DATE_COLUMNS)

    orders['order_purchase_timestamp'].min(), orders['order_purchase_timestamp'].max()

    min_time = orders.order_purchase_timestamp.min().strftime('%Y-%m-%d_%H-%M-%S')
    max_time = orders.order_purchase_timestamp.max().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"{min_time}_{max_time}.parquet"

    orders.to_parquet(f'./data/full/orders/{filename}')
    print(f"Loaded ./data/full/orders/{filename}")

    #payments
    order_payments = pd.read_csv('data/olist_order_payments_dataset.csv').convert_dtypes()
    filename = f"{min_time}_{max_time}.parquet"
    order_payments.to_parquet(f'./data/full/order_payments/{filename}')
    print(f"Loaded ./data/full/order_payments/{filename}")

    #items
    order_items = pd.read_csv('data/olist_order_items_dataset.csv').convert_dtypes()
    order_items = convert_dates(df=order_items, dates = ORDER_ITEMS_DATE_COLUMNS)
    filename = f"{min_time}_{max_time}.parquet"
    order_items.to_parquet(f'./data/full/order_items/{filename}')
    print(f"Loaded ./data/full/order_items/{filename}")


    #reviews

    reviews = pd.read_csv(f'data/olist_order_reviews_dataset.csv').convert_dtypes()
    reviews.to_parquet(f'./data/full/order_reviews/order_reviews.parquet')
    print("Loaded ./data/full/order_reviews/order_reviews.parquet")
    #customers
    for table in ['customers', 'geolocation', 'products', 'sellers']:
        df = pd.read_csv(f'data/olist_{table}_dataset.csv').convert_dtypes()
        df.to_parquet(f'./data/full/{table}/{table}.parquet')
        print(f"Loaded ./data/full/{table}/{table}.parquet")
    
def create_dirs():
    tables = ['customers', 'geolocation', 'order_items', 'orders', 'order_payments', 'order_reviews', 'products', 'sellers']
    tables_cdc = ['order_items', 'orders', 'order_payments']
    os.makedirs('data/cdc', exist_ok=True)

    for table in tables:
        directory = f'data/full/{table}'
        os.makedirs(directory, exist_ok=True)
        print(f'Created {directory}')

    for table in tables_cdc:
        directory = f'data/cdc/{table}'
        os.makedirs(directory, exist_ok=True)
        print(f'Created {directory}')

# Função para gerar um número aleatório com base em probabilidades
def choose_weighted_random(n=10):
    weights = [3 / (i**2) for i in range(1, n)]  # Cauda longa
    total_weight = sum(weights)
    probabilities = [weight / total_weight for weight in weights]  # Normalizando as probabilidades
    return np.random.choice(range(1, n), p=probabilities)

# Função para gerar itens do pedido
def generate_order_items(order_id, created_at, order_items, product_ids, product_ids_w, seller_ids, seller_ids_w):
    quantity = choose_weighted_random()
    order_items_rows = []
    
    for i in range(0, quantity):
        product_id = random.choices(product_ids, product_ids_w, k=1)[0]
        seller_id = random.choices(seller_ids, seller_ids_w, k=1)[0]

        price_counts = order_items[order_items['product_id'] == product_id]['price'].value_counts(normalize=True)
        prices = price_counts.index.tolist()
        prices_w = price_counts.values.tolist()

        price = random.choices(prices, prices_w, k=1)[0]

        freight_counts = order_items[order_items['product_id'] == product_id]['freight_value'].value_counts(normalize=True)
        freights = freight_counts.index.tolist()
        freights_w = freight_counts.values.tolist()

        freight = random.choices(freights, freights_w, k=1)[0]

        order_item_row = {
            'order_id': order_id,
            'order_item_id': i + 1,
            'product_id': product_id,
            'seller_id': seller_id,
            'shipping_limit_date': created_at + timedelta(days=5),
            'price': float(price),
            'freight_value': float(freight),
            'Op': 'I',
            'modified_at': created_at
        }
        order_items_rows.append(order_item_row)
        
    return order_items_rows

# Função para gerar pagamentos do pedido
def generate_order_payments(order_items_rows, created_at, payment_types, payment_types_w):
    total_price = sum(item['price'] + item['freight_value'] for item in order_items_rows)
    payment_type = random.choices(payment_types, payment_types_w, k=1)[0]
    
    row = {
        'order_id': order_items_rows[0]['order_id'],
        'payment_sequential': 1,
        'payment_type': payment_type,
        'payment_installments': 1,
        'payment_value': total_price,
        'Op': 'I',
        'modified_at': created_at
    }
    return row 

# Função para gerar timestamps aleatórios baseados em uma data de referência
def random_timestamp_after(base_date, days=0, hours=0):
    return base_date + timedelta(days=random.randint(days, days + 5), hours=random.randint(hours, hours + 23), minutes=random.randint(0, 60))

# Função principal para gerar dados
def generate_random_orders_data(days_after=60):
    orders = pd.read_parquet("./data/full/orders/2016-09-04_21-15-19_2018-10-17_17-30-18.parquet")
    order_items = pd.read_parquet("./data/full/order_items/2016-09-04_21-15-19_2018-10-17_17-30-18.parquet")
    order_payments = pd.read_parquet("./data/full/order_payments/2016-09-04_21-15-19_2018-10-17_17-30-18.parquet")

    # Define probabilidades
    product_counts = order_items['product_id'].value_counts(normalize=True)
    product_ids = product_counts.index.tolist()
    product_ids_w = product_counts.values.tolist()

    seller_counts = order_items['seller_id'].value_counts(normalize=True)
    seller_ids = seller_counts.index.tolist()
    seller_ids_w = seller_counts.values.tolist()

    payment_type_counts = order_payments['payment_type'].value_counts(normalize=True)
    payment_types = payment_type_counts.index.tolist()
    payment_types_w = payment_type_counts.values.tolist()

    random.seed(42)

    # Definir arrays e variáveis
    customer_ids = orders['customer_id'].unique()
    max_date = orders['order_estimated_delivery_date'].max()
    order_rows = []
    order_items_rows = []
    order_payments_rows = []

    for day_after in range(1, days_after):
        n_orders = int(5 * choose_weighted_random(n=1000))
        
        for _ in range(n_orders):
            generated_uuid = uuid.uuid4()
            order_id = generated_uuid.hex

            order_purchase_timestamp = random_timestamp_after(max_date, days=day_after)

            random_row = {
                'order_id': order_id,
                'customer_id': random.choice(customer_ids),
                'order_status': 'created',
                'order_purchase_timestamp': order_purchase_timestamp,
                'order_approved_at': pd.NA,
                'order_delivered_carrier_date': pd.NA,
                'order_delivered_customer_date': pd.NA,
                'order_estimated_delivery_date': pd.NA,
                'Op': 'I',
                'modified_at': order_purchase_timestamp
            }

            order_rows.append(random_row.copy())

            approved_at = random_timestamp_after(order_purchase_timestamp, hours=1)
            random_row['order_status'] = 'approved'
            random_row['Op'] = 'U'
            random_row['modified_at'] = approved_at
            random_row['order_approved_at'] = approved_at
            random_row['order_estimated_delivery_date'] = random_timestamp_after(order_purchase_timestamp, hours=random.randint(7, 14)).date()

            order_rows.append(random_row.copy())

            delivered_carrier_date = random_timestamp_after(approved_at, days=day_after + 3)
            random_row['order_status'] = 'shipped'
            random_row['Op'] = 'U'
            random_row['modified_at'] = delivered_carrier_date
            random_row['order_delivered_carrier_date'] = delivered_carrier_date

            order_rows.append(random_row.copy())

            final_order_status = random.choices(['delivered', 'canceled'], weights=[0.7, 0.3], k=1)[0]

            if final_order_status == 'delivered':
                delivered_customer = random_timestamp_after(delivered_carrier_date, days=day_after + 5)
                random_row['order_delivered_customer_date'] = delivered_customer
                random_row['order_status'] = 'delivered'
                random_row['Op'] = 'U'
                random_row['modified_at'] = delivered_customer

                order_rows.append(random_row.copy())
            elif final_order_status == 'canceled':
                random_row['order_status'] = 'canceled'
                random_row['Op'] = 'U'
                random_row['modified_at'] = approved_at

                order_rows.append(random_row.copy())

            new_order_items = generate_order_items(order_id, order_purchase_timestamp, order_items, product_ids, product_ids_w, seller_ids, seller_ids_w)
            order_items_rows += new_order_items
            order_payments_rows.append(generate_order_payments(new_order_items, order_purchase_timestamp, payment_types, payment_types_w))

    return pd.DataFrame(order_rows), pd.DataFrame(order_items_rows), pd.DataFrame(order_payments_rows)

def split_cdc_data_into_parquets(new_orders, new_order_items, new_order_payments):
    
    date_col = 'modified_at'

    new_orders[date_col] = pd.to_datetime(new_orders[date_col])  # Garantir que a coluna de datas está no formato correto
    df_sorted = new_orders.sort_values(date_col).convert_dtypes()  # Ordenar pelo timestamp
    df_sorted = convert_dates(df_sorted, ORDERS_DATE_COLUMNS)
    folder_path = './data/cdc'

    # Criar pasta se não existir
    os.makedirs(folder_path, exist_ok=True)

    # Iterar por cada data única e salvar os dados
    for date, group in df_sorted.groupby(df_sorted[date_col].dt.isocalendar().week):
        dt_min = group[date_col].min().strftime('%Y-%m-%d_%H-%M-%S')
        dt_max = group[date_col].max().strftime('%Y-%m-%d_%H-%M-%S')
        ids = group.query("order_status=='created'")['order_id'].unique()
        
        filename = f"{dt_min}_{dt_max}.parquet"
        group.to_parquet(os.path.join('./data/cdc/orders', filename))

        items = new_order_items[new_order_items['order_id'].isin(ids)]
        filename = f"{dt_min}_{dt_max}.parquet"
        if len(items)>0:
            items = convert_dates(items, ORDER_ITEMS_DATE_COLUMNS).convert_dtypes()
            items.to_parquet(os.path.join('./data/cdc/order_items', filename))

        payments = new_order_payments[new_order_payments['order_id'].isin(ids)]
        filename = f"{dt_min}_{dt_max}.parquet"
        if len(payments)>0:
            payments = payments.convert_dtypes()
            payments.to_parquet(os.path.join('./data/cdc/order_payments', filename))
    print('Loaded CDC parquet files')



if os.path.exists("data/cdc"):
    shutil.rmtree("data/cdc")
    print("Removed CDC")

create_dirs()
full_load_all_tables()
new_orders, new_order_items, new_order_payments =  generate_random_orders_data(days_after=60)
split_cdc_data_into_parquets(new_orders, new_order_items, new_order_payments)


