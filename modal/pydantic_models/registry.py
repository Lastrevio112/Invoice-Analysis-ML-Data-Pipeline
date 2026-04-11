import sys
sys.path.append('/workspace/modal') #So that this is visible from workspace/ and not only from workspace/modal/

from pydantic_models.ds1 import InvoiceDocument_DS1
# Later we can add ds2, etc.

# These will be imported by invoice_to_json.py to map models to data source ids:
DS_MODEL_REGISTRY = {
    1: InvoiceDocument_DS1,
    # add new mappings here...
}

# Unfortunately we need two registries because modal.com can't serialize class parameters so we need to pass the parameter as a string. 
# Ugly workaround but I haven't found another solution.
DS_MODEL_NAME_REGISTRY = {k: v.__name__ for k, v in DS_MODEL_REGISTRY.items()}