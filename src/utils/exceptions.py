class BaseAppException(Exception):
    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code


class ResourceNotFoundException(BaseAppException):
    def __init__(self, message: str):
        super().__init__(message, status_code=404)

class ValidationException(BaseAppException):
    def __init__(self, message: str):
        super().__init__(message, status_code=400)

class UnauthorizedException(BaseAppException):
    def __init__(self, message: str):
        super().__init__(message, status_code=401)