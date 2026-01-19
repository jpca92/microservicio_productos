from typing import List, Optional
from pydantic import BaseModel, HttpUrl

class Producto(BaseModel):
    sku: str
    Referencia_del_producto: str
    Nombre_producto: str
    Descripcion_producto: Optional[str] = None
    Keywords: Optional[str] = None
    MetaTagDescription: Optional[str] = None
    Categoria: Optional[str] = None
    Marca: Optional[str] = None
    Link: Optional[str] = None
    Talla: str
    Imagen_url: Optional[str] = None
    Precio: float
    Inventario: int
    
class PayloadProductos(BaseModel):
    productos: List[Producto]
