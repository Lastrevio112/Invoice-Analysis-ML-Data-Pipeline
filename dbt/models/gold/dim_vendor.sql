WITH ds1_duplicates AS (
    SELECT
        '1' AS data_source_id,
        CONCAT(client_tax_id, '-', '1') AS vendor_no_id,
        client_tax_id AS vendor_no,
        client AS vendor_desc,
        NULL as vendor_address,                          -- Not available for DS 1
        ROW_NUMBER() OVER (PARTITION BY client_tax_id ORDER BY client) AS rn
    FROM {{ ref('ds1_invoice_lines') }}
)
/* Add here next data source logic:
--,WITH ds1_duplicates AS(
)
*/
SELECT
    {{ dim_vendor_columns() }}
FROM ds1_duplicates
WHERE rn = 1
/*
UNION ALL
SELECT
...
add here the next data source
*/