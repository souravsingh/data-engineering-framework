app/routers/propensity.py
=========================
/predict   — single-entity scoring
/batch     — up to 100 entities in one call
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Security, status
from fastapi.security.api_key import APIKeyHeader

from app.config import Settings, get_settings
from app.db.redshift import RedshiftClient
from app.models.request import BatchPropensityRequest, PropensityRequest
from app.models.response import (
    BatchPropensityResponse,
    FeatureProvenance,
    HealthResponse,
    PropensityResponse,
    ScoreStatus,
)
from app.services.feature_store import FeatureStoreService
from app.services.scorer import ScorerService

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Security ──────────────────────────────────────────────────────────────────

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    api_key: Annotated[str | None, Security(_api_key_header)],
    settings: Settings = Depends(get_settings),
) -> None:
    """Validate X-API-Key header when api_keys are configured."""
    if not settings.api_keys:
        return  # No keys configured → open (dev mode)
    if api_key not in settings.api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
        )


# ── Dependency helpers ────────────────────────────────────────────────────────

def get_db(request: Request) -> RedshiftClient:
    return request.app.state.db


def get_scorer(request: Request) -> ScorerService:
    return request.app.state.scorer


# ── Single predict ─────────────────────────────────────────────────────────────

@router.post(
    "/predict",
    response_model=PropensityResponse,
    status_code=status.HTTP_200_OK,
    summary="Score a single entity",
    dependencies=[Depends(verify_api_key)],
)
async def predict(
    body: PropensityRequest,
    request: Request,
    db: RedshiftClient = Depends(get_db),
    scorer: ScorerService = Depends(get_scorer),
    settings: Settings = Depends(get_settings),
) -> PropensityResponse:
    """
    Score one entity for conversion propensity.

    - Validates JSON input via Pydantic (400 on failure)
    - Enriches with Redshift features
    - Runs model inference
    - Returns validated PropensityResponse (500 if output validation fails)
    """
    t0 = time.perf_counter()

    feature_svc = FeatureStoreService(db, settings)

    try:
        feature_set = await feature_svc.get_merged_features(body)
    except Exception as exc:
        logger.exception("Feature fetch failed for %s", body.entity_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Feature retrieval error: {exc}",
        )

    try:
        score = scorer.score(feature_set, body.entity_id)
    except Exception as exc:
        logger.exception("Scoring failed for %s", body.entity_id)
        latency_ms = int((time.perf_counter() - t0) * 1000)
        return PropensityResponse(
            entity_id=body.entity_id,
            status=ScoreStatus.SCORING_ERROR,
            error_detail=str(exc),
            latency_ms=latency_ms,
        )

    latency_ms = int((time.perf_counter() - t0) * 1000)

    status_val = (
        ScoreStatus.PARTIAL if feature_set.missing_features else ScoreStatus.SUCCESS
    )

    response = PropensityResponse(
        entity_id=body.entity_id,
        status=status_val,
        score=score,
        provenance=FeatureProvenance(
            redshift_features_used=feature_set.redshift_features_used,
            json_features_used=feature_set.json_features_used,
            missing_features=feature_set.missing_features,
            feature_snapshot_timestamp=feature_set.snapshot_timestamp,
        ),
        latency_ms=latency_ms,
    )

    # Output schema validation is enforced by Pydantic at serialisation time.
    # Any constraint violation raises a 500 automatically via FastAPI.
    return response


# ── Batch predict ─────────────────────────────────────────────────────────────

@router.post(
    "/batch",
    response_model=BatchPropensityResponse,
    status_code=status.HTTP_200_OK,
    summary="Score up to 100 entities",
    dependencies=[Depends(verify_api_key)],
)
async def batch_predict(
    body: BatchPropensityRequest,
    request: Request,
    db: RedshiftClient = Depends(get_db),
    scorer: ScorerService = Depends(get_scorer),
    settings: Settings = Depends(get_settings),
) -> BatchPropensityResponse:
    """Score a batch of entities concurrently (max 100 per request)."""
    t0 = time.perf_counter()

    async def _score_one(req: PropensityRequest) -> PropensityResponse:
        t1 = time.perf_counter()
        feature_svc = FeatureStoreService(db, settings)
        try:
            feature_set = await feature_svc.get_merged_features(req)
            score = scorer.score(feature_set, req.entity_id)
            status_val = (
                ScoreStatus.PARTIAL if feature_set.missing_features else ScoreStatus.SUCCESS
            )
            return PropensityResponse(
                entity_id=req.entity_id,
                status=status_val,
                score=score,
                provenance=FeatureProvenance(
                    redshift_features_used=feature_set.redshift_features_used,
                    json_features_used=feature_set.json_features_used,
                    missing_features=feature_set.missing_features,
                    feature_snapshot_timestamp=feature_set.snapshot_timestamp,
                ),
                latency_ms=int((time.perf_counter() - t1) * 1000),
            )
        except Exception as exc:
            logger.exception("Batch scoring failed for %s", req.entity_id)
            return PropensityResponse(
                entity_id=req.entity_id,
                status=ScoreStatus.SCORING_ERROR,
                error_detail=str(exc),
                latency_ms=int((time.perf_counter() - t1) * 1000),
            )

    results = await asyncio.gather(*[_score_one(r) for r in body.requests])

    succeeded = sum(1 for r in results if r.status in {ScoreStatus.SUCCESS, ScoreStatus.PARTIAL})
    failed = len(results) - succeeded

    return BatchPropensityResponse(
        total=len(results),
        succeeded=succeeded,
        failed=failed,
        results=list(results),
        latency_ms=int((time.perf_counter() - t0) * 1000),
    )


# ── Health ────────────────────────────────────────────────────────────────────

@router.get(
    "/health",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Liveness + readiness probe",
    include_in_schema=False,
)
async def health(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> HealthResponse:
    db: RedshiftClient = request.app.state.db
    scorer: ScorerService = request.app.state.scorer

    redshift_ok = await db.health_check()
    if not redshift_ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redshift connection unavailable",
        )

    return HealthResponse(
        status="ok",
        version=settings.app_version,
        redshift_connected=redshift_ok,
        model_loaded=scorer.is_loaded,
    )
