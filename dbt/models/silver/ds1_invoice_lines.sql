{{
  config(
    unique_key='unique_invoice_line_no'
  )
}}

WITH flattened AS (
  SELECT
    -- header fields
    JSON_VALUE(raw_json, '$.gt_parse.header.client')         AS client,
    JSON_VALUE(raw_json, '$.gt_parse.header.client_tax_id')  AS client_tax_id,
    JSON_VALUE(raw_json, '$.gt_parse.header.iban')           AS iban,
    SAFE_CAST(JSON_VALUE(raw_json, '$.gt_parse.header.invoice_date') AS DATE)   AS invoice_date,
    JSON_VALUE(raw_json, '$.gt_parse.header.invoice_no')     AS invoice_no,
    JSON_VALUE(raw_json, '$.gt_parse.header.seller')         AS seller,
    JSON_VALUE(raw_json, '$.gt_parse.header.seller_tax_id')  AS seller_tax_id,

    -- summary fields
    SAFE_CAST(JSON_VALUE(raw_json, '$.gt_parse.summary.total_net_worth') AS FLOAT64) AS total_net_worth,
    SAFE_CAST(JSON_VALUE(raw_json, '$.gt_parse.summary.total_vat')       AS FLOAT64) AS total_vat,

    -- explode the items array
    JSON_QUERY_ARRAY(raw_json, '$.gt_parse.items') AS items,

    raw_json

  FROM `invoiceanalysispipeline.bronze.ds_1_raw_json`
)
SELECT
  -- unique key generation for this specific data source
  CONCAT(
    COALESCE(invoice_no, 'missingin'), '-', 
    COALESCE(client_tax_id, 'missingcti'), '-', 
    COALESCE(seller_tax_id, 'missingsti'), '-',
    CAST(ROW_NUMBER() OVER (PARTITION BY invoice_no, client_tax_id, seller_tax_id ORDER BY JSON_VALUE(item, '$.item_desc')   ) AS STRING)
    ) AS unique_invoice_line_no,

  CONCAT(
    COALESCE(invoice_no, 'missingin'), '-', 
    COALESCE(client_tax_id, 'missingcti'), '-', 
    COALESCE(seller_tax_id, 'missingsti')
    ) AS unique_invoice_no,

  --fields from raw json
  client,
  client_tax_id,
  iban,
  invoice_date,
  invoice_no,
  seller,
  seller_tax_id,
  total_net_worth,
  total_vat,

  -- extract fields from each exploded item
  JSON_VALUE(item, '$.item_desc')                                          AS item_desc,
  SAFE_CAST(JSON_VALUE(item, '$.item_net_price') AS FLOAT64)               AS item_net_price,
  SAFE_CAST(SAFE_CAST(JSON_VALUE(item, '$.item_qty') AS FLOAT64) AS INT64) AS item_qty,  --bigquery can't convert 1.0 from string to int so it needs to be passed to float in between
  SAFE_CAST(JSON_VALUE(item, '$.item_vat')       AS FLOAT64)               AS item_vat

FROM flattened,
UNNEST(COALESCE(JSON_QUERY_ARRAY(raw_json, '$.gt_parse.items'), ARRAY<JSON>[]))  AS item --this line of code makes sure malformed receipts produce zero item rows rather than disappearing entirely from our lineage