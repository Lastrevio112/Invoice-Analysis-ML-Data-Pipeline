import gc
import json
import os
import tempfile
from pathlib import Path

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
def extract_invoice(img_bytes: bytes, pydantic_model: type):
    # This function reads the jpeg that was converted to bytes and returns a JSON with the appropriate structure defined through Pydantic.
    import gc
    import json
    import os
    import tempfile
    from pathlib import Path

    from pydantic import AliasChoices, BaseModel, Field
    from typing import Optional

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
                template=pydantic_model,   #The pydantic model that defines the expected JSON structure, this is passed by process_new_files_for_all_ds.py by using the ds-model mapping in registry.py
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
def main(image_path: str, pydantic_model: type):
    img_bytes = Path(image_path).read_bytes()
    result = extract_invoice.remote(img_bytes, pydantic_model)
    print(json.dumps(result, indent=2))