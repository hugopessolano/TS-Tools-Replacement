from pydantic import BaseModel
from typing import Optional, Any, List, Union

class ValidationModel(BaseModel):
    """
    Modelo base para la validación de datos.
    Se puede extender para incluir validaciones específicas según el endpoint.
    """
    def model_post_init(self, __context: Any) -> None:
        """
        Se ejecuta después de la inicialización y validación.
        Elimina los atributos de esta instancia que tengan valor None.
        """
        super().model_post_init(__context)

        fields_to_check = list(self.model_fields.keys())

        for field_name in fields_to_check:
            if getattr(self, field_name, None) is None:
                try:
                    delattr(self, field_name)
                except AttributeError:
                    pass
    
    def json(self, **kwargs: Any) -> str:
        """
        Serializa el modelo a JSON.
        """
        return super().model_dump(**kwargs)


class i18nField(ValidationModel):
    en:Optional[str] = None
    es:Optional[str] = None
    pt:Optional[str] = None

class Variant(ValidationModel):
    #values:Optional[i18nField] = None

    id: Optional[Union[str,int]] = None
    image_id: Optional[Union[str,int]] = None
    product_id: Optional[Union[str,int]] = None
    position: Optional[Union[str,int]] = None
    price: Optional[Union[str, float, int]] = None
    compare_at_price: Optional[Union[str,float,int]] = None
    promotional_price: Optional[Union[str,float,int]] = None
    stock_management: Optional[bool] = True
    stock: Optional[Union[str,int]] = None
    weight: Optional[Union[str,int,float]] = None
    width: Optional[Union[str,int,float]] = None
    height: Optional[Union[str,int,float]] = None
    depth: Optional[Union[str,int,float]] = None
    sku: Optional[Union[str,int]] = None
    option_value_1: Optional[i18nField] = None
    option_value_2: Optional[i18nField] = None
    option_value_3: Optional[i18nField] = None
    values: Optional[List[i18nField]] = None
    barcode: Optional[Union[str,int,float]] = None
    mpn: Optional[Union[str,int,float]] = None
    age_group: Optional[str] = None
    gender: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    cost: Optional[Union[str,int,float]] = None


class Categories(ValidationModel):
    description:Optional[i18nField] = None
    handle:Optional[i18nField] = None
    name:Optional[i18nField] = None

class Product(ValidationModel):
    attributes:Optional[i18nField] = None
    categories:Optional[List[Categories]] = None
    description:Optional[i18nField] = None
    handle:Optional[i18nField] = None
    name:Optional[i18nField] = None
    variants:Optional[List[Variant]] = None

validation_class_map = {
    'variants': Variant,
    'categories': Categories,
    'products': Product
}

if __name__ == '__main__':
    test = i18nField(es='Test')
    test2 = Variant(values=test)
    test3 = Categories(description=test, handle=test, name=test)
    test4 = Product(attributes=test, categories=[test3], description=test, handle=test, name=test, variants=[test2])
    print('ok')