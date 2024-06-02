CREATE VIEW Global_Electronics_Retailer.Source_Check AS
SELECT 'customers' AS table_name, COUNT(*) AS source_row_read FROM Global_Electronics_Retailer.customers
UNION
SELECT 'exchange_rates' AS table_name, COUNT(*) AS Source_Row_Read FROM Global_Electronics_Retailer.exchange_rates
UNION
SELECT 'products' AS table_name, COUNT(*) AS Source_Row_Read FROM Global_Electronics_Retailer.products
UNION
SELECT 'sales' AS table_name, COUNT(*)  AS Source_Row_Read FROM Global_Electronics_Retailer.sales
UNION
SELECT 'stores' AS table_name, COUNT(*)  AS Source_Row_Read FROM Global_Electronics_Retailer.stores