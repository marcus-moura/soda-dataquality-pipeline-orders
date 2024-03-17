import duckdb
import pandas as pd

def duck_load_csv_to_dataframe(path_file: str) -> pd.DataFrame:
    return duckdb.read_csv(path_file).df()

def duck_transform_data(df: pd.DataFrame) -> pd.DataFrame:
        return duckdb.sql("""
                    SELECT 
                        OrderID AS id_pedido,
                        CustomerID AS id_cliente,
                        EmployeeID AS id_responsavel_pedido,
                        DATE_TRUNC(OrderDate, MONTH)::DATE AS mes_referencia_pedido,	
                        ShippedDate AS data_envio,
                        Freight AS valor_frete,
                        ShipName AS responsavel_envio,	
                        REGEXP_REPLACE(ShipAddress, ",\\s*(\\d+)", "") AS endereco_entrega_pedido,
                        REGEXP_EXTRACT(ShipAddress, ",\\s*(\\d+)")::INT64 AS numero_entrega_pedido,
                        ShipCity AS cidade_entrega_pedido,	
                        ShipRegion AS estado_entrega_pedido,	
                        ShipPostalCode AS cep_entrega_pedido,
                        REPLACE(ShipCountry, "z", "s") AS pais_entrega_pedido,	
                    FROM df
                    WHERE ShipCountry = "Brazil"
            
            """).df()