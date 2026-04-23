from typing import Generic, TypeVar
from pydantic import BaseModel

T = TypeVar('T')

class ApiResponse(BaseModel, Generic[T]):
    success: bool
    data: T | None = None
    message: str | None = None
    error: str | None = None
    warning: str | None = None
    
    
# FastAPI serializa automáticamente a:
# {
#   "success": true,
#   "data": {"id": 1, "name": "John"},
#   "message": "User retrieved successfully",
#   "error": null
# }