SELECT item_qty
FROM {{ ref('fact_invoice_line') }}
WHERE item_qty <= 0