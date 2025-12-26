import logging
from fastapi import APIRouter, Depends, HTTPException
from services.factory import get_service_factory, ServiceFactory
from .schemas import ClusterRequest, ClusterResponse
import time
import json

router = APIRouter()
logger = logging.getLogger(__name__)


def _get_factory() -> ServiceFactory:
    return get_service_factory()


@router.post("/rk-cluster-analysis", response_model=ClusterResponse)
def rk_analysis(payload: ClusterRequest, factory: ServiceFactory = Depends(_get_factory)):

    try:
        svc = factory.create("rk_cluster")
    except KeyError:
        logger.error("Rk cluster service not registered; payload=%s", getattr(payload, "dict", lambda: {})())
        raise HTTPException(status_code=500, detail="rk cluster service 未注册")

    start_pc = time.perf_counter()
    try:
        res = svc.cluster_analysis(payload.dict())
    except ValueError as e:
        elapsed_ms = (time.perf_counter() - start_pc) * 1000
        logger.warning("rk-cluster-analysis ValueError: %s; payload=%s; elapsed_ms=%.2fms",
                       e, getattr(payload, "dict", lambda: {})(), elapsed_ms)
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        elapsed_ms = (time.perf_counter() - start_pc) * 1000
        logger.exception("rk-cluster-analysis RuntimeError: %s; payload=%s; elapsed_ms=%.2fms",
                         e, getattr(payload, "dict", lambda: {})(), elapsed_ms)
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start_pc) * 1000
        logger.exception("rk-cluster-analysis unexpected error: %s; payload=%s; elapsed_ms=%.2fms",
                         e, getattr(payload, "dict", lambda: {})(), elapsed_ms)
        raise HTTPException(status_code=500, detail=f"内部错误: {e}")

    elapsed_ms = (time.perf_counter() - start_pc) * 1000

    try:
        res_repr = json.dumps(res, ensure_ascii=False, default=str)
    except Exception:
        try:
            res_repr = str(res)
        except Exception:
            res_repr = "<unserializable result>"

    logger.info(
        "rk-cluster-analysis success: elapsed_ms=%.2fms result=%s",
         elapsed_ms, res_repr
    )

    return ClusterResponse(**res)
