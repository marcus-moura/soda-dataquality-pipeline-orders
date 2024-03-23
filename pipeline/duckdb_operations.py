import duckdb
import pandas as pd

def duck_read_csv_to_dataframe(path_file: str) -> pd.DataFrame:
    return duckdb.read_csv(path_file).df()

def duck_transform_data(df: pd.DataFrame) -> pd.DataFrame:
        return duckdb.sql("""
                SELECT 
                    OrderID AS order_id,
                    CustomerID AS customer_id,
                    EmployeeID AS employee_id,
                    DATETRUNC('MONTH', OrderDate)::DATE AS order_reference_month,	
                    ShippedDate::TIMESTAMP AS shipped_date,
                    Freight AS cost_freight,
                    ShipName AS ship_date,	
                    REGEXP_REPLACE(ShipAddress, ',\\s*(\\d+)', '') AS order_delivery_address,
                    REGEXP_EXTRACT(ShipAddress, '\\s*(\\d+)')::INT64 AS order_delivery_number,
                    ShipCity AS order_delivery_city,	
                    ShipRegion AS order_delivery_region,	
                    ShipPostalCode AS order_delivery_postal_code,
                    REPLACE(ShipCountry, 'z', 's') AS order_delivery_country,	
                FROM df_bigquery_raw
                WHERE ShipCountry = 'Brazil'
            
            """).df()