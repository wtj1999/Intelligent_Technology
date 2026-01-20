from .api import router
from .service import RuKeTrainService, RuKeTrainServiceV1

__all__ = ["router", "RuKeTrainService"]


def register(factory, settings=None, **service_kwargs):

    # factory.register("rk_train", lambda **kw: RuKeTrainService(**{**service_kwargs, **kw}))
    factory.register("rk_train", lambda **kw: RuKeTrainServiceV1(**{**service_kwargs, **kw}))