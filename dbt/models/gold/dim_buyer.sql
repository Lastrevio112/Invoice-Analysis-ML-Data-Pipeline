WITH ds1_duplicates AS (
    SELECT
        '1' AS data_source_id,
        CONCAT(seller_tax_id, '-', '1') AS buyer_no_id,
        seller_tax_id AS buyer_no,
        seller AS buyer_desc,
        NULL as buyer_address,                          -- Not available for DS 1
        ROW_NUMBER() OVER (PARTITION BY seller_tax_id ORDER BY seller) AS rn
    FROM {{ ref('ds1_invoice_lines') }}
)
/* Add here next data source logic:
--,WITH ds1_duplicates AS(
)
*/
SELECT
    {{ dim_buyer_columns() }}
FROM ds1_duplicates
WHERE rn = 1
/*
UNION ALL
SELECT
...
add here the next data source
*/