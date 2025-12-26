import numpy as np
import json
from services.base import BaseService
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import text
from fastapi import HTTPException
from tslearn.clustering import KShape


class RuKeClusterService(BaseService):
    def __init__(self, db_client=None):
        self.db_client = db_client
        self._ready = False
        self.table = 'iot_rkdh_process'
        self.n_clusters = 3
        self.target_len = 0

    async def startup(self) -> None:
        self._ready = True

    async def shutdown(self) -> None:
        self._ready = False

    def info(self) -> Dict[str, Any]:
        return {"name": "RuKeClusterService", "ready": self._ready}

    def station_pressure_fields(self, station_idx: int) -> Tuple[str, str, str, str]:

        base = 5 + (station_idx - 1) * 2
        base1 = 14 + (station_idx - 1) * 2
        return f"prkdh{station_idx:03d}", f"prkdh{base:03d}", f"prkdh{base + 1:03d}", f"prkdh{base1:03d}"

    def safe_to_float_array(self, x) -> np.ndarray:

        if x is None:
            return np.array([], dtype=float)
        if isinstance(x, (list, tuple, np.ndarray)):
            try:
                arr = np.asarray(x, dtype=float).ravel()
                if arr.size > 0:
                    arr = arr[~np.isnan(arr)]
                return arr
            except Exception:
                return np.array([], dtype=float)
        if isinstance(x, str):
            try:
                parsed = json.loads(x)
                return self.safe_to_float_array(parsed)
            except Exception:
                return np.array([], dtype=float)
        return np.array([], dtype=float)

    def find_first_both_nonzero(self, arr1: np.ndarray, arr2: np.ndarray, zero_tol: float = 1e-12) -> int:
        n = min(arr1.size, arr2.size)
        if n == 0:
            return 0
        nz1 = ~np.isclose(arr1[:n], 0.0, atol=zero_tol)
        nz2 = ~np.isclose(arr2[:n], 0.0, atol=zero_tol)
        both = nz1 & nz2
        idxs = np.nonzero(both)[0]
        return int(idxs[0]) if idxs.size > 0 else 0

    def resample_list(self, arrays: List[np.ndarray], tgt_len: int):
        if len(arrays) == 0:
            return np.zeros((0, tgt_len), dtype=float)
        out = []
        for a in arrays:
            L = a.size
            if L == tgt_len:
                out.append(a.astype(float))
            elif L == 1:
                out.append(np.full(tgt_len, float(a[0])))
            else:
                orig_x = np.linspace(0.0, 1.0, num=L)
                target_x = np.linspace(0.0, 1.0, num=tgt_len)
                interp = np.interp(target_x, orig_x, a.astype(float))
                out.append(interp)
        return np.vstack(out)

    def determine_target_len(self, arrays: List[np.ndarray], override: int = 0):
        if override and override > 0:
            return override
        lengths = [a.size for a in arrays if a.size > 0]
        if not lengths:
            return 0
        lengths.sort()
        mid = len(lengths) // 2
        return lengths[mid] if len(lengths) % 2 == 1 else max(1, (lengths[mid - 1] + lengths[mid]) // 2)

    def do_kshape_clustering(self, arrays: List[np.ndarray], gaibans: List[str]):
        result = {
            "labels": [],
            "centers": [],
            "label_counts": {}
        }
        if len(arrays) == 0:
            return result

        tgt = self.determine_target_len(arrays, self.target_len)
        if tgt <= 0:
            tgt = max(1, int(np.median([a.size for a in arrays])))

        X = self.resample_list(arrays, tgt)  # shape (n, tgt)
        # z-normalize per series
        Xz = np.vstack([(row - np.mean(row)) / (np.std(row) if np.std(row) > 0 else 1.0) for row in X])
        k = max(1, min(self.n_clusters, Xz.shape[0]))

        ks = KShape(n_clusters=k)
        labels_arr = ks.fit_predict(Xz)
        centers = ks.cluster_centers_.squeeze().tolist()

        labels_out = []
        counts = {}
        for i, g in enumerate(gaibans):
            lbl = int(labels_arr[i])
            labels_out.append({"gaiban": g, "label": lbl, "series": arrays[i].tolist()})
            counts[lbl] = counts.get(lbl, 0) + 1

        result.update({
            "labels": labels_out,
            "centers": centers,
            "label_counts": counts
        })
        return result

    def cluster_analysis(self, payload):
        device_code = payload.get("DEVICECODE")
        start_time = payload.get("STARTTIME")
        end_time = payload.get("ENDTIME")
        station_idx = int(payload.get("STATIONIDX"))

        gaiban_code, p1_key, p2_key, position_key = self.station_pressure_fields(station_idx)

        sql = text(f"""
                    SELECT devicetime, `{gaiban_code}` AS gaiban_code, `{p1_key}` AS p1, `{p2_key}` AS p2, `{position_key}` AS pos
                    FROM `{self.table}`
                    WHERE devicecode = :device_code
                      AND devicetime BETWEEN :start_time AND :end_time
                    ORDER BY devicetime ASC
                """)

        try:
            df = self.db_client.read_sql(sql, params={"device_code": device_code, "start_time": start_time, "end_time": end_time})
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"数据库查询失败: {e}")

        if df.empty:
            raise HTTPException(status_code=404, detail="未查询到任何数据")

        seen = set()
        samples = {}
        for _, row in df.iterrows():
            raw_gaiban = row.get("gaiban_code")
            if raw_gaiban == "":
                continue
            if raw_gaiban in seen:
                continue
            seen.add(raw_gaiban)

            raw_p1 = row.get("p1") if "p1" in row else row.get(p1_key)
            raw_p2 = row.get("p2") if "p2" in row else row.get(p2_key)
            raw_pos = row.get("pos") if "pos" in row else row.get(position_key)

            a1 = self.safe_to_float_array(raw_p1)
            a2 = self.safe_to_float_array(raw_p2)
            ap = self.safe_to_float_array(raw_pos)

            if a1.size > 0:
                a1 = np.array(list(reversed(a1.tolist())), dtype=float)
            if a2.size > 0:
                a2 = np.array(list(reversed(a2.tolist())), dtype=float)
            if ap.size > 0:
                ap = np.array(list(reversed(ap.tolist())), dtype=float)

            samples[raw_gaiban] = {"p1": a1, "p2": a2, "pos": ap}

        if len(samples) == 0:
            raise HTTPException(status_code=404, detail="未查询到任何数据")

        def collect_for_key(key_name: str):
            arrs = []
            gaibans = []
            for g, d in samples.items():
                arr = d.get(key_name)
                if arr is None or arr.size == 0:
                    continue
                arrs.append(arr)
                gaibans.append(g)
            return arrs, gaibans

        arr1_list, gaibans_arr1 = collect_for_key("p1")
        arr2_list, gaibans_arr2 = collect_for_key("p2")
        pos_list, gaibans_pos = collect_for_key("pos")

        clusters = {}
        clusters["arr1"] = self.do_kshape_clustering(arr1_list, gaibans_arr1)
        clusters["arr2"] = self.do_kshape_clustering(arr2_list, gaibans_arr2)
        clusters["position_arr"] = self.do_kshape_clustering(pos_list, gaibans_pos)

        return {
            "device_code": device_code,
            "station_idx": station_idx,
            "clusters": clusters
        }
