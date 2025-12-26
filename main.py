from connects.kafka_client import KafkaClient
from connects.db_client import DBClient
from fastapi import FastAPI
from services.factory import get_service_factory
from services.rk_process_analysis_service import register as register_rk, router as rk_router
from services.rk_cluster_analysis_service import register as register_rk_cluster, router as rk_cluster_router
from core.config import get_settings
from core.logging import setup_logging

setup_logging()
settings = get_settings()
factory = get_service_factory()

service_kwargs = dict()

app = FastAPI(title=settings.APP_NAME)
app.include_router(rk_router, prefix="/rk", tags=["rk"])
app.include_router(rk_cluster_router, prefix="/rk_cluster", tags=["rk_cluster"])


@app.on_event("startup")
async def startup():
    app.state.kafka_client = KafkaClient()
    app.state.db_client = DBClient()
    register_rk(factory, settings=settings, kafka_client=app.state.kafka_client, **service_kwargs)
    register_rk_cluster(factory, settings=settings, db_client=app.state.db_client, **service_kwargs)

    await factory.startup_all()


@app.on_event("shutdown")
async def shutdown():
    await factory.shutdown_all()
