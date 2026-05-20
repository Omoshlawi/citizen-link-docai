"""
Custom exceptions and FastAPI exception handlers.

All errors follow the same JSON shape:
  { "error": { "code": "SNAKE_CASE_CODE", "message": "Human readable message" } }
"""

from fastapi import Request
from fastapi.responses import JSONResponse


class AppError(Exception):
    """Base class for all application errors."""

    def __init__(self, code: str, message: str, status_code: int = 500):
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class AuthError(AppError):
    def __init__(self, message: str = "Forbidden"):
        super().__init__(code="FORBIDDEN", message=message, status_code=403)


class NotFoundError(AppError):
    def __init__(self, message: str):
        super().__init__(code="NOT_FOUND", message=message, status_code=404)


class ValidationError(AppError):
    def __init__(self, message: str):
        super().__init__(code="VALIDATION_ERROR", message=message, status_code=422)


class ProcessingError(AppError):
    def __init__(self, message: str = "Document processing failed."):
        super().__init__(code="PROCESSING_ERROR", message=message, status_code=500)


async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "INTERNAL_ERROR", "message": "An unexpected error occurred."}},
    )
