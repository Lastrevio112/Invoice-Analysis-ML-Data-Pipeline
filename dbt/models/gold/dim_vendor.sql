WITH ds1_duplicates AS (
    SELECT
        '1' AS data_source_id,
        CONCAT(client_tax_id, '-', '1') AS vendor_no_id,
        client_tax_id AS vendor_no,
        client AS vendor_desc,
        '' as vendor_address,                          -- Not available for DS 1
        ROW_NUMBER() OVER (PARTITION BY client_tax_id ORDER BY client) AS rn
    FROM {{ ref('ds1_invoice_lines') }}
)
,ds2_duplicates AS(
    SELECT
        '2' AS data_source_id,
        {{ dbt_utils.generate_surrogate_key(["'ds2-superstore-vendor'", "'2'"])}} AS vendor_no_id,
        'ds2-superstore-vendor' AS vendor_no,
        'SuperStore' AS vendor_desc,                           --always same vendor for this data source
        'Ozark Highlands Road, St. Louis' AS vendor_address,
        1 AS rn                                                --this table should have only one row for this ds since all invoices come from the same vendor
    FROM {{ ref('ds2_invoice_lines')}}
    LIMIT 1
)
/* Add here next data source logic:
--,ds3_duplicates AS(
)
*/
SELECT
    {{ dim_vendor_columns() }}
FROM ds1_duplicates
WHERE rn = 1

UNION ALL

SELECT
    {{ dim_vendor_columns() }}
FROM ds2_duplicates
WHERE rn = 1
/*
UNION ALL
SELECT
...
add here the next data source
*/