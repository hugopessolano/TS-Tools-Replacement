import re
from typing import Dict, Optional, List
from pydantic import BaseModel, RootModel
from endpoint_config import ENDPOINTS_LIST

# Modelo para un endpoint individual
class Endpoint(BaseModel):
    path: str
    positional_arguments_count: Optional[int] = 0

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
    single: Optional[Endpoint] = None    # Recurso individual (e.g. /products/{arg_1})
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

# Necesario para la autorefencia en children
EndpointGroup.model_rebuild()


class EndpointMap(RootModel[Dict[str, EndpointGroup]]):
    def __getattr__(self, item):
        # 'self.root' contiene el diccionario con las agrupaciones
        if item in self.root:
            return self.root[item]
        raise AttributeError(f"'EndpointMap' object has no attribute '{item}'")


# Función para construir el árbol de endpoints
def build_tree(endpoints: List[str]) -> Dict[str, Dict]:
    tree = {}
    for ep in endpoints:
        # Eliminamos cadenas vacías al separar
        segments = [seg for seg in ep.split("/") if seg]
        if not segments:
            continue
        root = segments[0]
        if root not in tree:
            tree[root] = {"all": None, "single": None, "children": {}}
        if len(segments) == 1:
            tree[root]["all"] = ep
        elif len(segments) == 2:
            tree[root]["single"] = ep
        else:
            # Para endpoints con más de 2 segmentos:
            # Se asume que el segundo segmento es un parámetro (por ejemplo, {arg_1})
            # y el resto son rutas anidadas.
            current = tree[root]["children"]
            # Recorremos los segmentos a partir del tercero
            for index, seg in enumerate(segments[2:], start=2):
                if seg not in current:
                    current[seg] = {"all": None, "single": None, "children": {}}
                # Si es el último segmento, asignamos el endpoint al atributo 'all'
                if index == len(segments) - 1:
                    current[seg]["all"] = ep
                current = current[seg]["children"]
    return tree

# Función recursiva para transformar el árbol en objetos EndpointGroup
def tree_to_endpoint_group(tree_node: Dict) -> EndpointGroup:
    group = EndpointGroup(
        all=Endpoint(path=tree_node["all"]) if tree_node["all"] is not None else None,
        single=Endpoint(path=tree_node["single"]) if tree_node["single"] is not None else None,
        children={k: tree_to_endpoint_group(v) for k, v in tree_node["children"].items()}
    )
    return group

def build_endpoint_map() -> EndpointMap:
    # Construir el árbol a partir de la lista
    tree = build_tree(ENDPOINTS_LIST)

    # Transformar el árbol a EndpointGroup para cada grupo raíz
    root_groups = {resource: tree_to_endpoint_group(node) for resource, node in tree.items()}

    # Crear la instancia de EndpointMap usando RootModel
    endpoint_map:EndpointMap = EndpointMap.model_validate(root_groups)
    return endpoint_map

if __name__ == '__main__':
    endpoint_map = build_endpoint_map()
    endpoint_map.products.variants.single.path_with_positional_args()
    print("Endpoint 'products' - All:", endpoint_map.products.all.path)
    print("Endpoint 'products' - Single:", endpoint_map.products.single.path)
    print("Endpoint 'products' - Variants All:", endpoint_map.products.variants.all.path)

    # Ejemplo con conversión a argumentos posicionales:
    print("Products Single (con argumentos posicionales):", endpoint_map.products.single.path_with_positional_args())
