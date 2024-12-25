import pandas as pd
from sqlalchemy import create_engine
import warnings
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')

def extract_data(file_path):
    """Extract data from a CSV file."""
    # Specify the encoding
    data = pd.read_csv(file_path, encoding='ISO-8859-1')
    return data

def transform_data(data):
    """Perform data cleaning and transformations."""
    # Convert 'InvoiceDate' to datetime
    data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'], errors='coerce')
    
    # Drop rows with missing values in critical columns
    data.dropna(subset=['InvoiceNo', 'CustomerID', 'InvoiceDate', 'Quantity', 'UnitPrice'], inplace=True)
    
    # Remove invalid or negative values for 'Quantity' and 'UnitPrice'
    data = data[(data['Quantity'] > 0) & (data['UnitPrice'] > 0)].copy()
    
    # Calculate total cost per row
    data['TotalCost'] = data['Quantity'] * data['UnitPrice']
    
    # Rename columns for consistency with database schema
    data.rename(columns={
        'InvoiceNo': 'invoice_no',
        'InvoiceDate': 'invoice_date',
        'CustomerID': 'customer_id',
        'TotalCost': 'total_cost',
        'Country': 'country_name'
    }, inplace=True)
    
    # Select relevant columns
    transformed_data = data[['invoice_no', 'invoice_date', 'customer_id', 'total_cost', 'country_name']]
    
    return transformed_data

def load_data_manually(data, table_name, db_url):
    """Load data into PostgreSQL manually."""
    from sqlalchemy import create_engine, Table, MetaData
    
    # Create SQLAlchemy engine
    engine = create_engine(db_url)
    connection = engine.connect()
    metadata = MetaData(bind=engine)
    table = Table(table_name, metadata, autoload_with=engine)
    
    # Prepare data for insertion
    data_records = data.to_dict(orient='records')  # Convert to a list of dicts
    
    # Insert data into the table
    connection.execute(table.insert(), data_records)
    connection.close()
    print(f"Data successfully loaded into table '{table_name}'.")

def plot_sales_data():
    # Create engine and establish connection
    engine = create_engine(db_url)
    
    # Query sales data from the database
    query = "SELECT invoice_date, SUM(total_cost) AS daily_sales FROM ecommerce_data GROUP BY invoice_date ORDER BY invoice_date;"
    
    # Fetch the data into a DataFrame
    with engine.connect() as connection:
        result = connection.execute(query)
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    # Plotting the sales data using Matplotlib
    plt.figure(figsize=(10, 6))
    plt.plot(data['invoice_date'], data['daily_sales'], marker='o', color='b', label='Daily Sales')
    plt.title('Daily Sales Over Time')
    plt.xlabel('Date')
    plt.ylabel('Total Sales ($)')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

def plot_sales_by_country():
    # Query sales data by country
    query = "SELECT country_name, SUM(total_cost) AS total_sales FROM ecommerce_data GROUP BY country_name ORDER BY total_sales DESC;"
    engine = create_engine(db_url)
    
    with engine.connect() as connection:
        result = connection.execute(query)
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    # Plotting the sales by country
    plt.figure(figsize=(10, 6))
    plt.bar(data['country_name'], data['total_sales'], color='g')
    plt.title('Total Sales by Country')
    plt.xlabel('Country')
    plt.ylabel('Total Sales ($)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def test_script():
    # Create engine and establish connection
    engine = create_engine(db_url)

    # Use engine.connect() to create a connection
    with engine.connect() as connection:
        # Query the data from the table
        query = "SELECT * FROM ecommerce_data LIMIT 5;"
        
        # Execute the query and fetch the results into a DataFrame
        result = connection.execute(query)
        
        # Convert result into a pandas DataFrame
        data = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Print the results
    print("Fetched data from the database:")
    print(data)

# Example usage
file_path = r'C:\Users\legen\Downloads\data.csv'
db_url = 'postgresql+psycopg2://postgres:postgres@localhost:5432/ecommerce_db'

try:
    raw_data = extract_data(file_path)
    print("Extracted data preview:")
    print(raw_data.head())
    
    transformed_data = transform_data(raw_data)
    print("Transformed data preview:")
    print(transformed_data.head())
    # test_script()
    # plot_sales_data()
    # Run the function to plot sales by country
    plot_sales_by_country()
    # load_data_manually(transformed_data, 'ecommerce_data', db_url)
except Exception as e:
    print(f"Error: {e}")
