import numpy as np
import json
from services.base import BaseService
from typing import Any, Dict
from sqlalchemy import text
from fastapi import HTTPException


class RuKeRecommendService(BaseService):
    def __init__(self, db_client=None):
        self.db_client = db_client
        self._ready = False
        self.table = 'ai_iot_rkdh_process'

    async def startup(self) -> None:
        self._ready = True

    async def shutdown(self) -> None:
        self._ready = False

    def info(self) -> Dict[str, Any]:
        return {"name": "RuKeRecommendService", "ready": self._ready}

    def _to_clean_array(self, lst):
        if not lst:
            return np.array([], dtype=float)
        out = []
        for v in lst:
            if v is None:
                continue
            try:
                fv = float(v)
            except Exception:
                continue
            if np.isnan(fv) or np.isinf(fv):
                continue
            out.append(fv)
        return np.asarray(out, dtype=float)

    def _stats(self, arr: np.ndarray, q: float):
        """
        返回字典 {count, mean, std, quantile}
        若 arr 为空返回 values 都为 None 且 count 为 0
        """
        if arr.size == 0:
            return {
                "count": 0,
                "mean": None,
                "std": None,
                "var": None,
                "quantile": None,
                "arr": None
            }
        mean_v = float(np.nanmean(arr))
        std_v = float(np.nanstd(arr, ddof=0))
        if q is None:
            q = 0.99
        try:
            qf = float(q)
        except Exception:
            qf = 0.99
        qf = max(0.0, min(1.0, qf))
        try:
            quant_v = float(np.nanpercentile(arr, qf * 100.0))
        except Exception:
            quant_v = None
        return {
            "count": int(arr.size),
            "mean": round(mean_v, 6) if mean_v is not None else None,
            "std": round(std_v, 6) if std_v is not None else None,
            "quantile": round(quant_v, 6) if quant_v is not None else None,
            "arr": arr.tolist()
        }

    def param_recommend(self, payload):
        device_code = payload.get("DEVICECODE")
        start_time = payload.get("STARTTIME")
        end_time = payload.get("ENDTIME")
        station_idx = int(payload.get("STATIONIDX"))
        threshold_quantile = payload.get("THRESHOLDQUANTILE")

        sql = text(f"""
                                    SELECT devicecode, devicetime, station, pressures
                                    FROM `{self.table}`
                                    WHERE devicecode = :device_code
                                      AND station = :station_idx
                                      AND devicetime BETWEEN :start_time AND :end_time
                                    ORDER BY devicetime ASC
                                """)

        try:
            df = self.db_client.read_sql(sql, params={"device_code": device_code, "station_idx": station_idx,
                                                      "start_time": start_time,
                                                      "end_time": end_time})
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"数据库查询失败: {e}")

        if df.empty:
            raise HTTPException(status_code=404, detail="未查询到任何数据")

        FirstUpPressureMaxList = []
        FirstDownPressureMaxList = []
        FirstUpPressureSlopeList = []
        FirstDownPressureSlopeList = []
        SecondUpPressureMaxList = []
        SecondDownPressureMaxList = []
        SecondUpPressureSlopeList = []
        SecondDownPressureSlopeList = []
        ThirdUpPressureMaxList = []
        ThirdDownPressureMaxList = []
        ThirdUpPressureSlopeList = []
        ThirdDownPressureSlopeList = []

        for _, row in df.iterrows():
            pressures = row.get('pressures')
            if pressures:
                pressures = json.loads(pressures)
                FirstDownPressureMaxList.append(pressures.get('FirstDownPressureMax'))
                FirstUpPressureMaxList.append(pressures.get('FirstUpPressureMax'))
                FirstDownPressureSlopeList.append(pressures.get('FirstDownPressureSlope'))
                FirstUpPressureSlopeList.append(pressures.get('FirstUpPressureSlope'))
                SecondDownPressureMaxList.append(pressures.get('SecondDownPressureMax'))
                SecondUpPressureMaxList.append(pressures.get('SecondUpPressureMax'))
                SecondDownPressureSlopeList.append(pressures.get('SecondDownPressureSlope'))
                SecondUpPressureSlopeList.append(pressures.get('SecondUpPressureSlope'))
                ThirdDownPressureMaxList.append(pressures.get('ThirdDownPressureMax'))
                ThirdUpPressureMaxList.append(pressures.get('ThirdUpPressureMax'))
                ThirdDownPressureSlopeList.append(pressures.get('ThirdDownPressureSlope'))
                ThirdUpPressureSlopeList.append(pressures.get('ThirdUpPressureSlope'))

        try:
            if threshold_quantile is None:
                tq = 0.99
            else:
                tq = float(threshold_quantile)
        except Exception:
            tq = 0.99
        tq = max(0.0, min(1.0, tq))

        raw_map = {
            "FirstUpPressureMax": FirstUpPressureMaxList,
            "FirstDownPressureMax": FirstDownPressureMaxList,
            "FirstUpPressureSlope": FirstUpPressureSlopeList,
            "FirstDownPressureSlope": FirstDownPressureSlopeList,
            "SecondUpPressureMax": SecondUpPressureMaxList,
            "SecondDownPressureMax": SecondDownPressureMaxList,
            "SecondUpPressureSlope": SecondUpPressureSlopeList,
            "SecondDownPressureSlope": SecondDownPressureSlopeList,
            "ThirdUpPressureMax": ThirdUpPressureMaxList,
            "ThirdDownPressureMax": ThirdDownPressureMaxList,
            "ThirdUpPressureSlope": ThirdUpPressureSlopeList,
            "ThirdDownPressureSlope": ThirdDownPressureSlopeList
        }

        metrics = {}
        for k, lst in raw_map.items():
            arr = self._to_clean_array(lst)
            metrics[k] = self._stats(arr, tq)

        result = {
            "device_code": device_code,
            "station_idx": station_idx,
            "start_time": start_time,
            "end_time": end_time,
            "threshold_quantile": tq,
            "metrics": metrics
        }

        return result

