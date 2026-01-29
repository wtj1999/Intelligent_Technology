import numpy as np
import json
import os
import traceback
import logging
import pandas as pd
from datetime import datetime, timedelta
from services.base import BaseService
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import text
from fastapi import HTTPException
from tslearn.clustering import KShape
from tslearn.metrics import dtw, cdist_dtw
from core.logging import setup_logging

setup_logging()
logger = logging.getLogger("rk_anomaly_detector.trainer")


class RuKeTrainService(BaseService):
    def __init__(self, db_client=None):
        self.db_client = db_client
        self._ready = False
        self.table = 'ai_iot_rkdh_process'
        self.n_clusters = 3
        self.target_len = 0
        self.detect_series_keys = ['slope_arr1_rising_1', 'slope_arr2_rising_1']
        self.threshold_quantile = 0.999
        self.chunk_minutes = 60
        self.distance_measure = "dtw"

    async def startup(self) -> None:
        self._ready = True

    async def shutdown(self) -> None:
        self._ready = False

    def info(self) -> Dict[str, Any]:
        return {"name": "RuKeTrainService", "ready": self._ready}

    def resample_series(self, s: np.ndarray, target_len: int) -> np.ndarray:
        """线性插值把 1D 序列 s 重采样到 target_len"""
        L = s.size
        if L == target_len:
            return s.astype(float)
        if L == 0:
            return np.zeros(target_len, dtype=float)
        if L == 1:
            return np.full(target_len, float(s[0]))
        orig_x = np.linspace(0.0, 1.0, num=L)
        target_x = np.linspace(0.0, 1.0, num=target_len)
        return np.interp(target_x, orig_x, s.astype(float))

    def determine_target_len(self, arrays: List[np.ndarray], override: int = 0):
        if override and override > 0:
            return override
        lengths = [a.size for a in arrays if a.size > 0]
        if not lengths:
            return 0
        lengths.sort()
        mid = len(lengths) // 2
        return lengths[mid] if len(lengths) % 2 == 1 else max(1, (lengths[mid - 1] + lengths[mid]) // 2)

    def load_flat(self, path: str) -> Dict[str, Dict[str, Any]]:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def z_normalize(self, s: np.ndarray) -> np.ndarray:
        mu = np.mean(s)
        sigma = np.std(s)
        if sigma == 0 or np.isnan(sigma):
            return s - mu
        return (s - mu) / sigma

    def build_matrix_from_saved(self, saved: Dict[str, Dict[str, Any]], series_key: str, target_len: Optional[int] = None):
        series_list = []
        keys = []
        for gaiban, st in saved.items():
            if not isinstance(st, dict):
                continue
            arr = st.get(series_key)
            if arr is None:
                continue
            try:
                na = np.asarray(arr, dtype=float).ravel()
            except Exception:
                continue
            if na.size == 0:
                continue
            series_list.append(na)
            keys.append(gaiban)
        if not series_list:
            return np.zeros((0, 0)), keys, 0
        if target_len is None or target_len <= 0:
            target_len = self.determine_target_len(series_list, None)
        X = np.vstack([self.resample_series(s, target_len) for s in series_list])
        Xz = np.vstack([self.z_normalize(row) for row in X])
        return Xz, keys, target_len

    def download_train_data(self, payload):
        """
                   Train data

                     /{device_code}/station1/saved_arrays_1.json
                     /{device_code}/station2/saved_arrays_2.json
                     ...

                   JSON format per file:
                   {
                     "<gaiban_val>": {
                       "arr1": [...],
                       "arr2": [...],
                       "position_arr": [...]
                     },
                     ...
                   }

                """
        start_time = payload.get("STARTTIME")
        end_time = payload.get("ENDTIME")

        start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

        sql = text(f"""
                            SELECT *
                            FROM `{self.table}`
                            WHERE devicetime BETWEEN :start_time AND :end_time
                            ORDER BY devicetime ASC
                        """)

        delta = timedelta(minutes=self.chunk_minutes)

        df_parts = []
        cur_start = start_dt
        part_idx = 0
        try:
            while cur_start < end_dt:
                cur_end = min(cur_start + delta, end_dt)
                part_idx += 1
                s_str = cur_start.strftime("%Y-%m-%d %H:%M:%S")
                e_str = cur_end.strftime("%Y-%m-%d %H:%M:%S")
                logger.info("Querying part %d: %s -> %s", part_idx, s_str, e_str)
                try:
                    part_df = self.db_client.read_sql(sql, params={"start_time": s_str, "end_time": e_str})
                except Exception as ex:
                    logger.exception("DB query failed for part %d (%s - %s): %s", part_idx, s_str, e_str, ex)
                    raise HTTPException(status_code=500,
                                        detail=f"数据库查询失败 (part {part_idx} {s_str} ~ {e_str}): {ex}")

                if part_df is not None and not part_df.empty:
                    df_parts.append(part_df)

                cur_start = cur_end

            if not df_parts:
                raise HTTPException(status_code=404, detail="未查询到任何数据 (all chunks empty)")

            df_all = pd.concat(df_parts, ignore_index=True)
            df = df_all.drop_duplicates(subset=['gaiban_code'], keep="first").reset_index(drop=True)

        except HTTPException:
            raise
        except Exception as e:
            tb = traceback.format_exc()
            logger.exception("Failed during chunked DB queries: %s", tb)
            raise HTTPException(status_code=500, detail=f"Chunked DB queries failed: {e}\n{tb}")

        station_saved: Dict[str, Dict[int, Dict[str, Dict[str, Any]]]] = {}
        for name, sub_df in df.groupby(['devicecode', 'station']):
            devicecode, station = name
            if devicecode not in station_saved:
                station_saved[devicecode] = {}
            if station not in station_saved[devicecode]:
                station_saved[devicecode][station] = {}
            for _, row in sub_df.iterrows():
                gaiban_code = row.get('gaiban_code')
                rising_segments = row.get('rising_segments')
                arr1 = json.loads(row.get('pressure1_series'))
                arr2 = json.loads(row.get('pressure2_series'))
                position_arr = json.loads(row.get('position_series'))

                if rising_segments:
                    rising_segments = json.loads(rising_segments)
                    def seg_slope(seg):
                        if seg.size < 2:
                            return 0.0
                        n = seg.size
                        slopes = np.zeros(n, dtype=float)
                        slopes[-1] = (seg[-1] - seg[-2])
                        for i in range(0, n - 1):
                            slopes[i] = (seg[i + 1] - seg[i]) / 1.0
                        return slopes

                    s_a1 = seg_slope(np.array(arr1[:rising_segments[0][1] + 1]))
                    s_a2 = seg_slope(np.array(arr2[:rising_segments[0][1] + 1]))

                    station_saved[devicecode][station][gaiban_code] = {
                        "arr1": list(map(float, arr1)),
                        "arr2": list(map(float, arr2)),
                        "slope_arr1_rising_1": list(map(float, s_a1.tolist())),
                        "slope_arr2_rising_1": list(map(float, s_a2.tolist())),
                        "position_arr": list(map(float, position_arr)),
                        "slope_arr1_rising_1_max": json.loads(row.get('pressures')).get('FirstUpPressureSlope'),
                        "slope_arr2_rising_1_max": json.loads(row.get('pressures')).get('FirstDownPressureSlope'),
                    }

        written = {}
        for device_str, stations_dict in station_saved.items():
            written[device_str] = {}
            base_dir = os.path.join("services/rk_process_train_service/data", device_str)
            for st, st_data in stations_dict.items():
                if not st_data:
                    continue
                station_dir = os.path.join(base_dir, f"station{st}")
                os.makedirs(station_dir, exist_ok=True)
                out_path = os.path.join(station_dir, f"saved_arrays_{st}.json")
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(st_data, f, ensure_ascii=False, indent=2)
                written[device_str][st] = {"path": out_path, "n_samples": len(st_data)}

        return written

    def train_kshape_detector(self, written):
        for device_str, stations_dict in written.items():
            for st, st_data in stations_dict.items():
                path = st_data["path"]
                n_samples = st_data["n_samples"]
                if n_samples < 10:
                    continue

                saved = self.load_flat(path)
                for series_key in self.detect_series_keys:
                    Xz, keys, target_len = self.build_matrix_from_saved(saved, series_key, None)
                    ks = KShape(n_clusters=self.n_clusters)
                    labels = ks.fit_predict(Xz)
                    centers = ks.cluster_centers_.squeeze()  # shape (k, target_len)
                    dists = np.zeros(Xz.shape[0], dtype=float)
                    for i in range(Xz.shape[0]):
                        sample = Xz[i]

                        if self.distance_measure == "euclidean":
                            dist_per_center = np.linalg.norm(centers - sample, axis=1)
                        elif self.distance_measure == "cosine":
                            dist_per_center = 1.0 - np.dot(sample, centers.T) / (
                                        np.linalg.norm(sample) * np.linalg.norm(centers, axis=1))
                        elif self.distance_measure == "sdb":
                            pass
                        elif self.distance_measure == "dtw":
                            dist_per_center = cdist_dtw(centers, sample).ravel()

                        dists[i] = float(np.min(dist_per_center))
                    # threshold by quantile
                    threshold = float(np.quantile(dists, self.threshold_quantile))
                    model = {
                        "series_key": series_key,
                        "n_clusters": self.n_clusters,
                        "target_len": int(target_len),
                        "threshold": threshold,
                        "centers": centers.tolist(),
                        "quantile": self.threshold_quantile,
                    }

                    slope_arr = [v.get(f"{series_key}_max") for v in saved.values() if f"{series_key}_max" in v]

                    model.update(
                        {
                            "slope_arr_mean": np.mean(slope_arr),
                            "slope_arr_std": np.std(slope_arr),
                            "slope_arr_quantile": np.quantile(slope_arr, self.threshold_quantile),
                        }
                    )

                    base_dir = os.path.join("services/rk_process_analysis_service/data", device_str)
                    station_dir = os.path.join(base_dir, f"station{st}")
                    os.makedirs(station_dir, exist_ok=True)
                    model_path = os.path.join(station_dir, f"{series_key}_kshape_model.json")

                    with open(model_path, "w", encoding="utf-8") as f:
                        json.dump(model, f, ensure_ascii=False, indent=2)
                    logger.info(
                        f"Trained KShape model for {series_key}, saved model to {model_path}, threshold={threshold:.6f}")

    def train(self, payload):
        written = self.download_train_data(payload=payload)
        self.train_kshape_detector(written)
