from pydantic import AliasChoices, BaseModel, Field
from typing import Optional

class InvoiceHeader_DS2(BaseModel):
    invoice_date: Optional[str] = Field(
        default=None, 
        description="The invoice or order date",
        examples=["Date", "Date:"],
        validation_alias=AliasChoices("Date", "Order date")
    )
    client: Optional[str] = Field(
        default=None, 
        description="The billed client or customer name",
        examples=["Bill To:", "Bill To:"],
        validation_alias=AliasChoices("Bill To:", "Bill To", "Customer Name", "Buyer")
    )
    client_address: Optional[str] = Field(
        default=None, 
        description="The shipping or delivery address",
        examples=["Ship To:", "Ship To"],
        validation_alias=AliasChoices("Ship To:", "Address", "Ship To")
    )

class InvoiceItem_DS2(BaseModel):
    item_desc: Optional[str] = Field(
        default=None, 
        description="The item or product description",
        examples=["Item"],
        validation_alias=AliasChoices("Item", "Item description", "Item name", "Product name")
    )
    item_qty: Optional[str] = Field(
        default=None, 
        description="The quantity ordered for this product",
        examples=["Quantity"],
        validation_alias=AliasChoices("Quantity", "Item quantity")
    )
    item_rate: Optional[str] = Field(
        default=None,
        description="The unit price or rate",
        examples=["Rate"],
        validation_alias=AliasChoices("Rate", "Price", "Item net price", "Unit cost")
    )

class InvoiceSummary_DS2(BaseModel):
    order_id: Optional[str] = Field(
        default=None,
        description="The order or invoice ID/number",
        examples=["Order ID:", "Order ID"],
        validation_alias=AliasChoices("Order ID", "order")
    )
    discount: Optional[str] = Field(
        default=None, 
        description="Discount amount applied",
        examples=["Discount"],
        validation_alias=AliasChoices("Discount")
    )
    shipping: Optional[str] = Field(
        default=None, 
        description="Shipping fee",
        examples=["Shipping:", "Shipping"],
        validation_alias=AliasChoices("Shipping", "Shipping fee")
    )

class InvoiceDocument_DS2(BaseModel):
    header: InvoiceHeader_DS2
    items: list[InvoiceItem_DS2]
    summary: InvoiceSummary_DS2