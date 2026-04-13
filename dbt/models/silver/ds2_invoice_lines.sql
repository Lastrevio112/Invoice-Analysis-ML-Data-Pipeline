{{
  config(
    unique_key='unique_invoice_line_no'
  )
}}

WITH flattened AS (
  SELECT
    -- header fields
    JSON_VALUE(raw_json, '$.header.client')         AS client,
    JSON_VALUE(raw_json, '$.header.client_address')  AS client_address,
    JSON_VALUE(raw_json, '$.header.invoice_date')   AS invoice_date,

    -- summary fields
    JSON_VALUE(raw_json, '$.summary.discount')  AS discount,
    JSON_VALUE(raw_json, '$.summary.order_id')  AS order_id,
    JSON_VALUE(raw_json, '$.summary.shipping')  AS shipping,

    -- explode the items array
    JSON_QUERY_ARRAY(raw_json, '$.gt_parse.items') AS items,

    raw_json

  FROM {{source('bronze', 'ds_2_raw_json')}}
)
,exploded AS (
  SELECT
    --fields from raw json
    client,
    client_address,
    invoice_date,
    discount,
    order_id,
    shipping,

    -- extract fields from each exploded item
    JSON_VALUE(item, '$.item_desc') AS item_desc,
    JSON_VALUE(item, '$.item_rate') AS item_rate,
    JSON_VALUE(item, '$.item_qty')  AS item_qty

  FROM flattened,
  UNNEST(COALESCE(JSON_QUERY_ARRAY(raw_json, '$.items'), ARRAY<JSON>[]))  AS item
  WHERE JSON_VALUE(item, '$.item_rate') IS NOT NULL OR JSON_VALUE(item, '$.item_qty') IS NOT NULL --when both item_rate and item_qty are null, it means the upstream model mistook an item category that sits below the item for an actual item. this needs to be cleaned downstream.
)
SELECT
  -- unique key generation for this specific data source
  {{ dbt_utils.generate_surrogate_key(['order_id', 'item_desc']) }} AS unique_invoice_line_no,    --invoice PDFs have only one line for this DS, but same invoice can be found in multiple files if it has multiple lines. therefore, the combination between item desc and order_id should be unique.
  {{ dbt_utils.generate_surrogate_key(['order_id', 'client']) }} AS unique_invoice_no,

  --Cleaning up fields from previous CTE
  SAFE_CAST(client AS STRING) AS client,
  SAFE_CAST(client_address AS STRING) AS client_address,
  SAFE_CAST(invoice_date AS DATE) AS invoice_date,

  SAFE_CAST(REGEXP_REPLACE(discount, r'[$,]', '') AS FLOAT64) AS discount, --replace $ and , characters with nothing and then convert to float
  SAFE_CAST(order_id AS STRING) AS order_id,
  SAFE_CAST(REGEXP_REPLACE(shipping, r'[$,]', '') AS FLOAT64) AS shipping,

  SAFE_CAST(item_desc AS STRING) AS item_desc,
  SAFE_CAST(REGEXP_REPLACE(item_rate, r'[$,]', '') AS FLOAT64) AS item_rate,
  SAFE_CAST(REGEXP_REPLACE(item_qty, r'[$,]', '') AS INT64) AS item_qty
  
FROM exploded