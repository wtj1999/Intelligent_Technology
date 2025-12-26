import numpy as np
import json
from services.base import BaseService
from typing import Any, Dict, List, Optional, Tuple


class RuKeService(BaseService):
    def __init__(self, kafka_client=None):
        self.kafka_client = kafka_client
        self._ready = False

    async def startup(self) -> None:
        self._ready = True

    async def shutdown(self) -> None:
        self._ready = False

    def info(self) -> Dict[str, Any]:
        return {"name": "RuKeService", "ready": self._ready}

    def find_active_stations(self, msg_dict: Dict[str, Any]) -> List[int]:

        active = []
        for i in range(1, 5):
            key = f"PRKDH{i:03d}"
            v = msg_dict.get(key, None)
            if v is None:
                continue
            if isinstance(v, str):
                if v.strip() != "":
                    active.append(i)
            elif isinstance(v, (list, tuple)):
                if len(v) > 0 and any((e is not None and (str(e).strip() != "")) for e in v):
                    active.append(i)
            else:
                active.append(i)
        return active

    def station_pressure_fields(self, station_idx: int) -> Tuple[str, str, str]:

        base = 5 + (station_idx - 1) * 2
        base1 = 14 + (station_idx - 1) * 2
        return f"PRKDH{base:03d}", f"PRKDH{base + 1:03d}", f"PRKDH{base1:03d}"

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

    def pointwise_slope(self, y: np.ndarray) -> np.ndarray:
        if y is None:
            return np.array([], dtype=float)
        y = np.asarray(y, dtype=float)
        n = y.size
        if n == 0:
            return np.array([], dtype=float)
        if n == 1:
            return np.array([0.0], dtype=float)
        slopes = np.zeros(n, dtype=float)
        slopes[0] = (y[1] - y[0])
        slopes[-1] = (y[-1] - y[-2])
        if n > 2:
            for i in range(1, n - 1):
                slopes[i] = (y[i + 1] - y[i - 1]) / 2.0
        return slopes

    def find_true_runs(self, mask: np.ndarray) -> List[Tuple[int, int]]:
        runs: List[Tuple[int, int]] = []
        if mask.size == 0:
            return runs
        i = 0
        n = mask.size
        while i < n:
            if mask[i]:
                s = i
                j = i + 1
                while j < n and mask[j]:
                    j += 1
                runs.append((s, j - 1))
                i = j
            else:
                i += 1
        return runs

    def resample_series(self, s: np.ndarray, target_len: int) -> np.ndarray:
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

    def z_normalize(self, s: np.ndarray) -> np.ndarray:
        mu = np.mean(s)
        sigma = np.std(s)
        if sigma == 0 or np.isnan(sigma):
            return s - mu
        return (s - mu) / sigma

    def score_series_with_model(self, model: Dict[str, Any], series: List[float]) -> Dict[str, Any]:
        centers = np.asarray(model["centers"], dtype=float)
        target_len = int(model["target_len"])
        s = np.asarray(series, dtype=float).ravel()
        if s.size == 0:
            return {"score": float("inf"), "is_anomaly": True, "reason": "empty_series"}
        s_rs = self.resample_series(s, target_len)
        s_z = self.z_normalize(s_rs)
        dists = np.linalg.norm(centers - s_z, axis=1)
        score = float(np.min(dists))
        is_anom = score > float(model["threshold"])
        return {"score": score, "threshold": float(model["threshold"]), "is_anomaly": bool(is_anom),
                "dists": dists.tolist()}

    def threshold_detect(self, arr1, arr2, position_arr):

        slope_thresh = 0.1
        min_len = 5
        win1_lo, win1_hi = 1, 80
        win2_lo, win2_hi = 120, 230
        max_thresh = 10.0

        station_res = {
            "status": "NG",
            "rising_segments": None,
            "all_segments": None,
            "pressures": None
        }

        slopes = self.pointwise_slope(position_arr)
        mask = slopes > slope_thresh

        runs = self.find_true_runs(mask)
        rising_segs = [(s, e) for (s, e) in runs if (e - s + 1) >= min_len]

        def count_segments_in_window(seg_list, lo, hi):
            return [seg for seg in seg_list if (seg[0] >= lo and seg[1] <= hi)]

        segs_win1 = count_segments_in_window(rising_segs, win1_lo, win1_hi)
        segs_win2 = count_segments_in_window(rising_segs, win2_lo, win2_hi)

        if len(segs_win1) != 1 or len(segs_win2) != 2:
            return station_res

        first_seg = sorted(segs_win1, key=lambda t: t[0])[0]
        win2_sorted = sorted(segs_win2, key=lambda t: t[0])
        win2_after = [seg for seg in win2_sorted if seg[0] > first_seg[0]]
        if len(win2_after) >= 2:
            second_seg, third_seg = win2_after[0], win2_after[1]
        else:
            second_seg, third_seg = win2_sorted[0], win2_sorted[1]

        three_segs = sorted([first_seg, second_seg, third_seg], key=lambda t: t[0])
        seg1p, seg2p, seg3p = three_segs

        def compute_metrics(a1: np.ndarray, a2: np.ndarray, s: int, e: int):
            npos = position_arr.size
            s_clamped = max(0, min(s, npos - 1))
            e_clamped = max(0, min(e, npos - 1))
            if s_clamped > e_clamped:
                return {"max": None, "slope": None, "arr1": [], "arr2": []}

            def slice_arr(a):
                if a.size == 0:
                    return np.array([], dtype=float)
                L = a.size
                s2 = max(0, min(s_clamped, L - 1))
                e2 = max(0, min(e_clamped, L - 1))
                if s2 > e2:
                    return np.array([], dtype=float)
                return np.abs(a[s2:e2 + 1])

            seg_a1 = slice_arr(a1)
            seg_a2 = slice_arr(a2)
            if seg_a1.size == 0 and seg_a2.size == 0:
                return {"max": None, "slope": None, "arr1": [], "arr2": []}
            m1 = float(np.nanmax(seg_a1)) if seg_a1.size > 0 else -np.inf
            m2 = float(np.nanmax(seg_a2)) if seg_a2.size > 0 else -np.inf
            overall_max = None
            if np.isfinite(m1) or np.isfinite(m2):
                overall_max = float(max(m1 if np.isfinite(m1) else -np.inf, m2 if np.isfinite(m2) else -np.inf))

            def seg_slope(seg):
                if seg.size < 2:
                    return 0.0
                idx_max = int(np.argmax(seg))
                idx_min = int(np.argmin(seg))
                max_val = float(seg[idx_max])
                min_val = float(seg[idx_min])
                idx_diff = idx_max - idx_min
                if idx_diff == 0:
                    return 0.0
                return abs(float((max_val - min_val) / idx_diff))

            s_a1 = seg_slope(seg_a1) if seg_a1.size > 0 else 0.0
            s_a2 = seg_slope(seg_a2) if seg_a2.size > 0 else 0.0
            mean_slope = float((s_a1 + s_a2) / 2.0)
            return {"max": overall_max, "slope": mean_slope, "arr1": seg_a1.tolist(), "arr2": seg_a2.tolist()}

        m1 = compute_metrics(arr1, arr2, seg1p[0], seg1p[1])
        m2 = compute_metrics(arr1, arr2, seg2p[0], seg2p[1])
        m3 = compute_metrics(arr1, arr2, seg3p[0], seg3p[1])

        pressures = {
            "FirstPressureMax": m1["max"],
            "FirstPressureSlope": m1["slope"],
            "SecondPressureMax": m2["max"],
            "SecondPressureSlope": m2["slope"],
            "ThirdPressureMax": m3["max"],
            "ThirdPressureSlope": m3["slope"],
        }

        three_max_vals = [pressures[k] for k in ["FirstPressureMax", "SecondPressureMax", "ThirdPressureMax"]]

        if any(v > max_thresh for v in three_max_vals):
            station_res["status"] = "NG"
        else:
            station_res["status"] = "OK"

        station_res["rising_segments"] = [seg1p, seg2p, seg3p]
        station_res["all_segments"] = {
            "推80%入壳": (1, seg1p[1]),
            "静置": (seg1p[1] + 1, 71),
            "回退": (72, 98),
            "静置2": (99, seg2p[0]),
            "前进推15%": (seg2p[0] + 1, seg2p[1]),
            "静置3": (seg2p[1] + 1, seg3p[0]),
            "夹爪头推5%": (seg3p[0] + 1, seg3p[1]),
            "回退1": (seg3p[1] + 1, len(position_arr))
        }
        station_res["pressures"] = pressures

        return station_res

    def series_detect(self, arr1, arr2):
        with open('services/rk_process_analysis_service/data/saved_arrays.arr1.kshape_model.json', 'r', encoding='utf-8') as file:
            model1 = json.load(file)
        with open('services/rk_process_analysis_service/data/saved_arrays.arr2.kshape_model.json', 'r', encoding='utf-8') as file:
            model2 = json.load(file)

        r1 = self.score_series_with_model(model1, arr1)
        r2 = self.score_series_with_model(model2, arr2)
        combined_score = max(r1["score"], r2["score"])
        combined_threshold = max(model1["threshold"], model2["threshold"])
        is_anom = 'NG' if combined_score > combined_threshold else 'OK'

        return {"status": is_anom}

    def rk_analysis(self, payload):
        device_code = payload.get("DEVICECODE")
        device_time = payload.get("DEVICETIME")
        tenant = payload.get("TENANTID")

        pkg = {
            "device_code": device_code,
            "device_time": device_time,
            "tenant": tenant,
            "stations": {},
        }

        active_stations = self.find_active_stations(payload)

        for st in active_stations:
            p1_key, p2_key, position_key = self.station_pressure_fields(st)
            arr1 = np.array(list(reversed(self.safe_to_float_array(payload.get(p1_key)))), dtype=float)
            arr2 = np.array(list(reversed(self.safe_to_float_array(payload.get(p2_key)))), dtype=float)
            position_arr = np.array(list(reversed(self.safe_to_float_array(payload.get(position_key)))), dtype=float)

            start_idx = self.find_first_both_nonzero(arr1, arr2, zero_tol=1e-12)

            arr1 = arr1[start_idx:] if arr1.size > start_idx else np.array([], dtype=float)
            arr2 = arr2[start_idx:] if arr2.size > start_idx else np.array([], dtype=float)
            position_arr = position_arr[start_idx:] if position_arr.size > start_idx else np.array([], dtype=float)

            threshold_detect_result = self.threshold_detect(arr1, arr2, position_arr)
            series_detect_result = self.series_detect(arr1, arr2)

            status = "NG" if threshold_detect_result.get('status') == "NG" or series_detect_result.get('status') == "NG" else "OK"

            station_res = {
                "gaiban_code": payload.get(f"PRKDH{st:03d}"),
                "status": status,
                "rising_segments": threshold_detect_result.get('rising_segments'),
                "all_segments": threshold_detect_result.get('all_segments'),
                "pressures": threshold_detect_result.get('pressures'),
                "pressure1_series": list(arr1),
                "pressure2_series": list(arr2),
                "position_series": list(position_arr)
            }

            pkg["stations"][f"station_{st}"] = station_res

        return pkg





