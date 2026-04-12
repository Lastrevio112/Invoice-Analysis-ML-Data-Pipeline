{% macro fact_invoice_line_columns() %}
    row_id,                  
    data_source_id,
    unique_invoice_line_no,  
    unique_invoice_no,
    vendor_no_id,
    buyer_no_id,
    spend, 
    iban,
    invoice_date,
    invoice_no,
    item_desc,
    item_net_price,
    item_qty  
{% endmacro %}