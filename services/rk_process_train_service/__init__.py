from .api import router
from .service import RuKeTrainService

__all__ = ["router", "RuKeTrainService"]


def register(factory, settings=None, **service_kwargs):

    factory.register("rk_train", lambda **kw: RuKeTrainService(**{**service_kwargs, **kw}))