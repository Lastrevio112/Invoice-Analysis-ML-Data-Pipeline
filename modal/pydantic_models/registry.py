import sys
sys.path.append('/workspace/modal') #So that this is visible from workspace/ and not only from workspace/modal/

from pydantic_models.ds1 import InvoiceDocument_DS1
# Later we can add ds2, etc.

# This will be imported by invoice_to_json.py to map models to data source ids:
DS_MODEL_REGISTRY = {
    1: InvoiceDocument_DS1,
}