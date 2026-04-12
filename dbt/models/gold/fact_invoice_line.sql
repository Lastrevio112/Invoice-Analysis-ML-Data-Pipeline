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
/* Add here next data source logic:
--,WITH ds2_duplicates AS(
)
*/
SELECT
    {{ fact_invoice_line_columns() }}
FROM ds1_duplicates
WHERE rn = 1
/*
UNION ALL
SELECT
    {{ fact_invoice_line_columns() }}
FROM ds2_duplicates
WHERE rn = 1
...
add here the next data source
*/