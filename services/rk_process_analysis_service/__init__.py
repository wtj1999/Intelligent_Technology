from .api import router
from .service import RuKeService

__all__ = ["router", "RuKeService"]


def register(factory, settings=None, **service_kwargs):

    factory.register("rk", lambda **kw: RuKeService(**{**service_kwargs, **kw}))
