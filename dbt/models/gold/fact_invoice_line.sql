WITH ds1_duplicates AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['1', 'unique_invoice_line_no']) }} AS row_id,
        '1' AS data_source_id,
        unique_invoice_line_no,
        unique_invoice_no,
        CONCAT(client_tax_id, '-', '1') AS vendor_no_id,
        CONCAT(seller_tax_id, '-', '1') AS buyer_no_id,
        ROUND(item_net_price * item_qty, 3) AS spend,
        iban,
        invoice_date,
        invoice_no,
        item_desc,
        item_net_price,
        item_qty,
        ROW_NUMBER() OVER (PARTITION BY unique_invoice_line_no ORDER BY client) AS rn
    FROM {{ ref('ds1_invoice_lines') }}
)
,ds2_duplicates AS(
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['2', 'unique_invoice_line_no']) }} AS row_id,
        '2' AS data_source_id,
        unique_invoice_line_no,
        unique_invoice_no,
        {{ dbt_utils.generate_surrogate_key(["'ds2-superstore-vendor'", "'2'"])}} AS vendor_no_id,
        {{ dbt_utils.generate_surrogate_key(["'2'", 'client', 'client_address']) }} AS buyer_no_id,
        ROUND(item_rate * item_qty, 3) AS spend,   --we don't include discounts and shipping in this calculation to make spend consistent accross data sources
        '' as iban,
        invoice_date,
        order_id AS invoice_no,
        item_desc,
        item_rate AS item_net_price,
        item_qty,
        ROW_NUMBER() OVER (PARTITION BY unique_invoice_line_no ORDER BY item_rate) AS rn
    FROM {{ ref('ds2_invoice_lines') }}
)
/* Add here next data source logic:
--,ds3_duplicates AS(
)
*/
SELECT
    {{ fact_invoice_line_columns() }}
FROM ds1_duplicates
WHERE rn = 1

UNION ALL
SELECT
    {{ fact_invoice_line_columns() }}
FROM ds2_duplicates
WHERE rn = 1
/*
UNION ALL
SELECT
    {{ fact_invoice_line_columns() }}
FROM ds3_duplicates
WHERE rn = 1
...
add here the next data source
*/