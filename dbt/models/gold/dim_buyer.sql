WITH ds1_duplicates AS (
    SELECT
        '1' AS data_source_id,
        CONCAT(seller_tax_id, '-', '1') AS buyer_no_id,
        seller_tax_id AS buyer_no,
        seller AS buyer_desc,
        '' as buyer_address,                          -- Not available for DS 1
        ROW_NUMBER() OVER (PARTITION BY seller_tax_id ORDER BY seller) AS rn
    FROM {{ ref('ds1_invoice_lines') }}
)
,ds2_duplicates AS(
    SELECT
        '2' AS data_source_id,
        {{ dbt_utils.generate_surrogate_key(["'2'", 'client', 'client_address']) }} AS buyer_no_id,
        {{ dbt_utils.generate_surrogate_key(['client', 'client_address']) }} AS buyer_no,
        client AS buyer_desc,
        client_address AS buyer_address,
        ROW_NUMBER() OVER (PARTITION BY client, client_address ORDER BY client) AS rn
    FROM {{ ref('ds2_invoice_lines')}}
)
/* Add here next data source logic:
--,ds3_duplicates AS(
)
*/
SELECT
    {{ dim_buyer_columns() }}
FROM ds1_duplicates
WHERE rn = 1

UNION ALL
SELECT {{ dim_buyer_columns() }}
FROM ds2_duplicates
WHERE rn = 1
/*
UNION ALL
SELECT
...
add here the next data source
*/