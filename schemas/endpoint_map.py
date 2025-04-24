import re
from typing import Dict, Optional, List
from pydantic import BaseModel, RootModel
try:
    from schemas.endpoint_config import ENDPOINTS_LIST
    from schemas.validation_schemas import ValidationModel, validation_class_map
except ImportError:
    from endpoint_config import ENDPOINTS_LIST
    from validation_schemas import ValidationModel, validation_class_map

# Modelo para un endpoint individual
class Endpoint(BaseModel):
    path: str
    positional_arguments_count: Optional[int] = 0
    validation_model: Optional[type[ValidationModel]] = None

    def path_with_positional_args(self) -> str:
        """
        Retorna el path con los parámetros reemplazados de forma posicional.
        Ejemplo: "/products/{product_id}/variants" -> "/products/{arg_1}/variants"
        """
        replaced = self.path
        matches = re.findall(r"{(.*?)}", self.path)
        for i, param in enumerate(matches, start=1):
            replaced = replaced.replace("{" + param + "}", f"{{arg_{i}}}")
        return replaced

# Modelo para agrupar endpoints por ruta
class EndpointGroup(BaseModel):
    all: Optional[Endpoint] = None       # Recurso principal (e.g. listar todos)
    single: Optional[Endpoint] = None    # Recurso individual (ej.: /products/{arg_1} o el final con parámetro)
    children: Dict[str, "EndpointGroup"] = {}  # Subgrupos para rutas anidadas

    def __getattr__(self, item):
        """
        Permite acceder a los hijos como atributos.
        Ejemplo: endpoint_map.products.variants en lugar de children["variants"]
        """
        if item in self.children:
            return self.children[item]
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item}'")

    class Config:
        arbitrary_types_allowed = True

# Necesario para la autorefencia en children (Pydantic v2)
EndpointGroup.model_rebuild()

# Contenedor principal utilizando RootModel
class EndpointMap(RootModel[Dict[str, EndpointGroup]]):
    def __getattr__(self, item):
        # self.root contiene el diccionario con las agrupaciones
        if item in self.root:
            return self.root[item]
        raise AttributeError(f"'EndpointMap' object has no attribute '{item}'")


# Función para construir el árbol de endpoints con el comportamiento esperado
def build_tree_corrected(endpoints: List[str]) -> Dict[str, Dict]:
    tree = {}
    for ep in endpoints:
        segments = [seg for seg in ep.split("/") if seg]
        if not segments:
            continue

        # El primer segmento es el grupo raíz
        root = segments[0]
        if root not in tree:
            tree[root] = {"all": None, "single": None, "children": {}}

        if len(segments) == 1:
            tree[root]["all"] = ep
        elif len(segments) == 2:
            tree[root]["single"] = ep
        else:
            # Para endpoints con más de 2 segmentos:
            # - Asumimos que el segundo segmento es un parámetro (ej.: {arg_1})
            # - El resto de los segmentos anidados se procesa
            # - Si el último segmento es un parámetro, se asigna a 'single' del grupo correspondiente.
            # Primero procesamos los segmentos literales desde el tercer segmento hasta el penúltimo.
            current_node = tree[root]
            # Procesar desde el tercer segmento hasta el penúltimo
            for seg in segments[2:-1]:
                if not (seg.startswith("{") and seg.endswith("}")):
                    if seg not in current_node["children"]:
                        current_node["children"][seg] = {"all": None, "single": None, "children": {}}
                    current_node = current_node["children"][seg]
                # Si el segmento es un parámetro intermedio, lo omitimos para la agrupación.
            last_seg = segments[-1]
            if last_seg.startswith("{") and last_seg.endswith("}"):
                # Si el último segmento es un parámetro, lo asignamos al 'single' del grupo actual
                current_node["single"] = ep
            else:
                # Si el último segmento es literal, asignarlo a 'all'
                if last_seg not in current_node["children"]:
                    current_node["children"][last_seg] = {"all": None, "single": None, "children": {}}
                current_node["children"][last_seg]["all"] = ep
    return tree

# Función recursiva para transformar el árbol en objetos EndpointGroup.
def tree_to_endpoint_group(tree_node: Dict) -> EndpointGroup:
    # Al crear cada objeto Endpoint se calcula la cantidad de argumentos
    def make_endpoint(ep_path: Optional[str]) -> Optional[Endpoint]:
        if ep_path is None:
            return None
        count = len(re.findall(r"{(.*?)}", ep_path))
        validation_model = None # Default

        # Lógica para encontrar el modelo de validación
        segments = [seg for seg in ep_path.split("/") if seg]
        if segments:
            lookup_segment = segments[-1]
            if lookup_segment.startswith("{") and lookup_segment.endswith("}") and len(segments) > 1:
                lookup_segment = segments[-2]

            lookup_key = lookup_segment.lower()
            validation_model = validation_class_map.get(lookup_key)

        return Endpoint(
            path=ep_path,
            positional_arguments_count=count,
            validation_model=validation_model
        )


    group = EndpointGroup(
        all=make_endpoint(tree_node["all"]),
        single=make_endpoint(tree_node["single"]),
        children={k: tree_to_endpoint_group(v) for k, v in tree_node["children"].items()}
    )
    return group

def build_endpoint_map() -> EndpointMap:
    # Construir el árbol usando la versión corregida
    tree = build_tree_corrected(ENDPOINTS_LIST)
    # Transformar el árbol a EndpointGroup para cada grupo raíz
    root_groups = {resource: tree_to_endpoint_group(node) for resource, node in tree.items()}
    # Crear la instancia de EndpointMap usando RootModel
    endpoint_map: EndpointMap = EndpointMap.model_validate(root_groups)
    return endpoint_map


if __name__ == '__main__':
    endpoint_map = build_endpoint_map()
    print('ok')