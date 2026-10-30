"""The FastAPI adapter of the model API. It runs only in the serving image.

The route reads the raw body and passes it to the service, so the key check and the rate limit come before any validation answer. The OpenAPI document
describes the request and the answers from the models below, which use the same limits as the service. The adapter holds no rule of its own.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Literal

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from scripts.features.serving_contract_client import CCN, YEAR_MAX, YEAR_MIN
from scripts.serving import metrics
from scripts.serving.body import BodyBudget, BodyRefused, BodyTooSlow
from scripts.serving.service import ModelService, Reply

KEY_HEADER = "X-API-Key"
REQUEST_HEADER = "X-Request-ID"


class PredictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    hai_type: Literal["HAI_1", "HAI_2", "HAI_3", "HAI_4", "HAI_5", "HAI_6"]
    ccn: str = Field(pattern=CCN.pattern, description="The six character CMS Certification Number of the hospital.")
    window_year: int = Field(ge=YEAR_MIN, le=YEAR_MAX, description="The year the feature window starts.")


class PredictResponse(BaseModel):
    request_id: str
    hai_type: str
    ccn: str
    window_year: int
    prediction: float = Field(description="The predicted standardized infection ratio of the hospital-window.")
    model_version: str
    served_by: Literal["stable", "canary"]
    fallback: bool
    features_as_of: str
    feature_schema_sha256: str


class ErrorResponse(BaseModel):
    error: str


async def read_body(request: Request, budget: BodyBudget) -> bytes:
    """Read the body piece by piece. The size limit counts the bytes received, so a body with no declared length cannot pass it. One deadline covers it."""
    chunks: list[bytes] = []
    stream = request.stream().__aiter__()
    while True:
        try:
            async with asyncio.timeout(budget.remaining()):
                chunk = await stream.__anext__()
        except StopAsyncIteration:
            return b"".join(chunks)
        except TimeoutError:
            raise BodyTooSlow("the body did not arrive in time") from None
        budget.add(chunk)
        chunks.append(chunk)


def create_app(service: ModelService) -> FastAPI:
    app = FastAPI(
        title="HAI prediction API",
        version="1",
        description="Predictions of hospital-acquired infection ratios from public hospital data. Synthetic and portfolio scope, not for clinical use.",
        openapi_url="/v1/openapi.json",
        docs_url=None,
        redoc_url=None,
    )

    def send(reply: Reply) -> JSONResponse:
        return JSONResponse(reply.body, status_code=reply.status, headers=reply.headers)

    @app.post(
        "/v1/predict",
        response_model=None,
        responses={
            200: {"model": PredictResponse},
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            422: {"model": ErrorResponse},
            429: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
        openapi_extra={"requestBody": {"required": True, "content": {"application/json": {"schema": PredictRequest.model_json_schema()}}}},
        summary="Predict the infection ratio of one hospital-window",
    )
    async def predict(request: Request) -> JSONResponse:
        admitted = service.admit(request.headers.get(KEY_HEADER))
        if isinstance(admitted, Reply):
            return send(admitted)
        budget = BodyBudget(clock=time.monotonic)
        try:
            budget.declare(request.headers.get("content-length"))
            raw = await read_body(request, budget)
        except BodyRefused as error:
            return send(service.reject_body(admitted, error.status, error.error))
        try:
            payload: Any = json.loads(raw)
        except ValueError:
            payload = None
        return send(await run_in_threadpool(service.finish, admitted, request.headers.get(REQUEST_HEADER), payload))

    @app.get("/v1/health", response_model=None, summary="Report whether each HAI type has a loaded stable model")
    def health() -> JSONResponse:
        return send(service.health())

    @app.get("/metrics", include_in_schema=False)
    def scrape() -> PlainTextResponse:
        return PlainTextResponse(service.metrics.render(), media_type="text/plain; version=0.0.4; charset=utf-8")

    def count(request: Request, status: int) -> None:
        service.metrics.observe(metrics.route_label(request.url.path), metrics.method_label(request.method), metrics.status_label(status), 0.0)

    @app.exception_handler(StarletteHTTPException)
    async def refused(request: Request, error: StarletteHTTPException) -> JSONResponse:
        count(request, error.status_code)
        return JSONResponse({"error": "not_found" if error.status_code == 404 else "refused"}, status_code=error.status_code)

    @app.exception_handler(Exception)
    async def failed(request: Request, error: Exception) -> JSONResponse:
        if metrics.route_label(request.url.path) != "/v1/predict":
            count(request, 500)
        return JSONResponse({"error": "internal"}, status_code=500)

    return app
