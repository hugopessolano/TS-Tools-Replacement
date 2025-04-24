from schemas.validation_schemas import ValidationModel, i18nField, Variant
from schemas.config_schemas import StoreConfig, DEFAULT_CONFIG
from typing import get_origin, get_args, Union
import json
from log_db import stdout_logger
import pandas as pd

def is_field_type_i18n(model: type[ValidationModel], field_name: str) -> bool:
    """
    Verifica si el tipo de anotación de un campo en un modelo Pydantic
    es i18nField o Optional[i18nField].
    """
    if field_name not in model.model_fields:
        return False

    field_info = model.model_fields[field_name]
    annotation = field_info.annotation

    if annotation is i18nField:
        return True

    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is Union:
        if i18nField in args:
             return True

    return False


def parse_i18n(payload_arguments:dict,key:str, store_config:StoreConfig) -> None:
    try:
        json_argument = json.loads(payload_arguments[key])
        # Si ya está en formato i18n, directamente se mantiene como body
        if len([key for key in list(json_argument.keys()) if key in store_config.enabled_languages]) > 0:
            payload_arguments[key] = json_argument
    except (json.JSONDecodeError, TypeError):
        value = None if pd.isna(payload_arguments[key]) else payload_arguments[key]
        payload_arguments[key] = {lang: value for lang in store_config.modify_languages}
    
def reformat_payload(payload_arguments:dict, 
                     keys_to_reformat:list, 
                     validation_model:type[ValidationModel],
                     store_config:StoreConfig = DEFAULT_CONFIG) -> dict:
    """
    reformats the payload arguments based on the expected schema and configured languages.
    """
    values = list()
    for key in keys_to_reformat:
        if is_field_type_i18n(validation_model, key):
            parse_i18n(payload_arguments, key, store_config)
        if key in ('option_value_1','option_value_2','option_value_3') and validation_model == Variant:
            if 'values' in payload_arguments.keys():
                stdout_logger.error(f"Payload contains both 'values' and '{[key for key in payload_arguments.keys() if key in ('option_value_1','option_value_2','option_value_3')]}'. Please remove one of them.")
                raise ValueError(f"Payload contains both 'values' and '{[key for key in payload_arguments.keys() if key in ('option_value_1','option_value_2','option_value_3')]}'. Please remove one of them.")
            values.append(payload_arguments.pop(key))
    if len(values) > 0:
        payload_arguments['values'] = values                
    return payload_arguments

