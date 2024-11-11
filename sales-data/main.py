import pandas as pd

def generate_monthly_summary(df: pd.DataFrame):
    """
    Generator function that yields monthly summaries of sales data.
    
    Parameters:
    - df: DataFrame containing sales data with columns `date`, `sales`, and `customer_id`.
    
    Yields:
    - A tuple (month, total_sales, unique_customers) for each month.
    """

    df['order_date'] = pd.to_datetime(df['order_date'])
    df['month'] = df['order_date'].dt.to_period('M')
    
    for month, group in df.groupby('month'):
        total_sales = group['price'].sum()
        unique_customers = group['customer_id'].nunique()
        yield month, total_sales, unique_customers

def main():
    df = pd.read_csv("test.csv")
    df = df.dropna()
    df = df[(df['price'] > 15) & (df['quantity'] > 0)]
    print(f"data: {df}")

    for month, total_sales, unique_customers in generate_monthly_summary(df):
        print(f"Month: {month}, Total Sales: {total_sales}, Unique Customers: {unique_customers}")

if __name__=="__main__":
    main()