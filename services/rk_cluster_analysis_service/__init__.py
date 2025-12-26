from .api import router
from .service import RuKeClusterService

__all__ = ["router", "RuKeClusterService"]


def register(factory, settings=None, **service_kwargs):

    factory.register("rk_cluster", lambda **kw: RuKeClusterService(**{**service_kwargs, **kw}))