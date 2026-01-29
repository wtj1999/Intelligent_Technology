from .api import router
from .service import RuKeRecommendService

__all__ = ["router", "RuKeRecommendService"]


def register(factory, settings=None, **service_kwargs):

    factory.register("rk_recommend", lambda **kw: RuKeRecommendService(**{**service_kwargs, **kw}))