from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ClusterRequest(BaseModel):
    DEVICECODE: str = Field(..., description="设备编码，用于查询 DB")
    STARTTIME: str = Field(..., description="查询起始时间，SQL 可接受的时间字符串")
    ENDTIME: str = Field(..., description="查询结束时间，SQL 可接受的时间字符串")
    STATIONIDX: int = Field(..., ge=1, le=4, description="站点索引 1..4")

class ClusterLabelItem(BaseModel):
    gaiban: str = Field(..., description="盖板码（样本键）")
    label: int = Field(..., description="簇标签（整数）")
    series: List[float] = Field(..., description="原始序列（重采样前）")

class ClusterResult(BaseModel):
    labels: List[ClusterLabelItem] = Field(default_factory=list, description="每个样本的 gaiban/label/series")
    centers: List[List[float]] = Field(default_factory=list, description="每个簇的中心序列（已 z-normalized）")
    label_counts: Dict[int, int] = Field(default_factory=dict, description="每个标签的样本计数")

class ClusterResponse(BaseModel):
    device_code: str = Field(..., description="请求中使用的 device_code")
    station_idx: int = Field(..., description="请求中使用的 station index")
    clusters: Dict[str, ClusterResult] = Field(
        default_factory=dict,
        description="按序列类型分组的聚类结果，包含 'arr1','arr2','position_arr' 三个 key（若无数据则对应为空）"
    )