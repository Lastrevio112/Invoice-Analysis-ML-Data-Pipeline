from pydantic import AliasChoices, BaseModel, Field
from typing import Optional

class InvoiceHeader_DS1(BaseModel):
    invoice_no: Optional[str] = Field(default=None, validation_alias=AliasChoices("Invoice no", "Invoice Number", "Invoice number"))
    invoice_date: Optional[str] = Field(default=None, validation_alias=AliasChoices("Invoice date", "Date"))
    seller: Optional[str] = Field(default=None, validation_alias=AliasChoices("Seller", "Vendor", "Supplier"))
    client: Optional[str] = Field(default=None, validation_alias=AliasChoices("Buyer", "Client", "Customer"))
    seller_tax_id: Optional[str] = Field(default=None, validation_alias=AliasChoices("Seller tax id", "seller_tax_id"))
    client_tax_id: Optional[str] = Field(default=None, validation_alias=AliasChoices("Client tax id", "client_tax_id"))
    iban: Optional[str] = Field(default=None, validation_alias=AliasChoices("IBAN", "iban", "Account number"))

class InvoiceItem_DS1(BaseModel):
    item_desc: Optional[str] = Field(default=None, validation_alias=AliasChoices("Description", "description", "Item description"))
    item_qty: Optional[str] = Field(default=None, validation_alias=AliasChoices("Quantity", "Item quantity"))
    item_net_price: Optional[str] = Field(default=None, validation_alias=AliasChoices("Price", "Item net price", "Net price"))
    item_vat: Optional[str] = Field(default=None, validation_alias=AliasChoices("vat", "Item vat", "VAT", "Tax"))

class InvoiceSummary_DS1(BaseModel):
    total_net_worth: Optional[str] = Field(default=None, validation_alias=AliasChoices("total_net_worth", "total worth", "Total worth"))
    total_vat: Optional[str] = Field(default=None, validation_alias=AliasChoices("total_vat", "total vat", "Total VAT", "Total tax"))

class GtParse_DS1(BaseModel):
    header: InvoiceHeader_DS1
    items: list[InvoiceItem_DS1]
    summary: InvoiceSummary_DS1

class InvoiceDocument_DS1(BaseModel):
    gt_parse: GtParse_DS1