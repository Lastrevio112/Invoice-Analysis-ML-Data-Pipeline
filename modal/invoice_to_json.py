import gc
import json
import os
import tempfile
from pathlib import Path
from typing import Optional

import modal

app = modal.App("invoice-extractor")

# Initializing modal image - these do not run in my local Docker container, nor on GitHub actions
modal_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "docling[vlm]",
        "torch",
        "bitsandbytes",
        "pydantic",
    )
)

# This will be added once through the windows powershell terminal and then cached by Modal.
hf_cache = modal.Volume.from_name("hf-cache", create_if_missing=True)


@app.function(
    gpu="L4",   #The second worst GPU, but will do the job.
    image=modal_image,
    timeout=900,
    volumes={"/root/.cache/huggingface": hf_cache},
)
def extract_invoice(img_bytes: bytes):
    # This function reads the jpeg that was converted to bytes and returns a JSON with the appropriate structure defined through Pydantic.
    import gc
    import json
    import os
    import tempfile
    from pathlib import Path

    from pydantic import AliasChoices, BaseModel, Field

    import torch
    from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import VlmExtractionPipelineOptions
    from docling.document_extractor import (
        DocumentExtractor,
        ExtractionFormatOption,
        ExtractionVlmPipeline,
        ImageDocumentBackend,
    )

    # PYDANTIC MODELS FOR DATA VALIDATION:
    class InvoiceHeader(BaseModel):
        invoice_no: Optional[str] = Field(default=None, validation_alias=AliasChoices("Invoice no", "Invoice Number", "Invoice number"))
        invoice_date: Optional[str] = Field(default=None, validation_alias=AliasChoices("Invoice date", "Date"))
        seller: Optional[str] = Field(default=None, validation_alias=AliasChoices("Seller", "Vendor", "Supplier"))
        client: Optional[str] = Field(default=None, validation_alias=AliasChoices("Buyer", "Client", "Customer"))
        seller_tax_id: Optional[str] = Field(default=None, validation_alias=AliasChoices("Seller tax id", "seller_tax_id"))
        client_tax_id: Optional[str] = Field(default=None, validation_alias=AliasChoices("Client tax id", "client_tax_id"))
        iban: Optional[str] = Field(default=None, validation_alias=AliasChoices("IBAN", "iban", "Account number"))
    class InvoiceItem(BaseModel):
        item_desc: Optional[str] = Field(default=None, validation_alias=AliasChoices("Description", "description", "Item description"))
        item_qty: Optional[str] = Field(default=None, validation_alias=AliasChoices("Quantity", "Item quantity"))
        item_net_price: Optional[str] = Field(default=None, validation_alias=AliasChoices("Price", "Item net price", "Net price"))
        item_vat: Optional[str] = Field(default=None, validation_alias=AliasChoices("vat", "Item vat", "VAT", "Tax"))
    class InvoiceSummary(BaseModel):
        total_net_worth: Optional[str] = Field(default=None, validation_alias=AliasChoices("total_net_worth", "total worth", "Total worth"))
        total_vat: Optional[str] = Field(default=None, validation_alias=AliasChoices("total_vat", "total vat", "Total VAT", "Total tax"))
    class GtParse(BaseModel):
        header: InvoiceHeader
        items: list[InvoiceItem]
        summary: InvoiceSummary
    class InvoiceDocument(BaseModel):
        gt_parse: GtParse


    acc_options = AcceleratorOptions(device=AcceleratorDevice.CUDA)
    pipeline_options = VlmExtractionPipelineOptions(accelerator_options=acc_options)

    # This needs to be de-allocated at the end to avoid memory leaks (because it's VRAM and not regular RAM), although it's not absolutely necessary if it runs on Modal, it's a good safety guard.
    extractor = DocumentExtractor(
        allowed_formats=[InputFormat.IMAGE],
        extraction_format_options={
            InputFormat.IMAGE: ExtractionFormatOption(
                pipeline_cls=ExtractionVlmPipeline,
                pipeline_options=pipeline_options,
                backend=ImageDocumentBackend,
            )
        }
    )

    try:
        # Write to a temp file and delete right after to make it easier for the extractor to parse
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            f.write(img_bytes)
            tmp_path = Path(f.name)
        try:
            result = extractor.extract(
                source=tmp_path,
                template=InvoiceDocument,   #The pydantic model that defines the expected JSON structure
                raises_on_error=True,
            )
        finally:
            os.unlink(tmp_path)

        return json.loads(result.json())["pages"][0]["extracted_data"]

    finally:
        # manual garbage collection to free up VRAM
        del extractor
        gc.collect()
        torch.cuda.empty_cache()


@app.local_entrypoint()
def main(image_path: str):
    img_bytes = Path(image_path).read_bytes()
    result = extract_invoice.remote(img_bytes)
    print(json.dumps(result, indent=2))