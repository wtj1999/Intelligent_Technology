from typing import Dict, Optional, List
from pydantic import BaseModel, Field


class ParamRecommendRequest(BaseModel):
    DEVICECODE: Optional[str] = Field(None, description="设备编码 (device_code)")
    STARTTIME: Optional[str] = Field(None, description="开始时间，格式示例 'YYYY-MM-DD HH:MM:SS'")
    ENDTIME: Optional[str] = Field(None, description="结束时间，格式示例 'YYYY-MM-DD HH:MM:SS'")
    STATIONIDX: Optional[int] = Field(None, description="工位索引 (1..4)")
    THRESHOLDQUANTILE: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="用于计算分位数的阈值，0..1 之间，默认 0.99"
    )


class StatItem(BaseModel):
    count: int = Field(..., description="有效样本数量 (整型)")
    mean: Optional[float] = Field(None, description="均值（若无数据则为 null）")
    std: Optional[float] = Field(None, description="标准差（若无数据则为 null）")
    quantile: Optional[float] = Field(None, description="按 threshold_quantile 计算的分位数（若无数据则为 null）")
    arr: Optional[List[float]] = Field(None, description="原始数据数组")


class ParamRecommendResponse(BaseModel):
    device_code: Optional[str] = Field(None, description="请求中的 device_code")
    station_idx: Optional[int] = Field(None, description="请求中的 station_idx")
    start_time: Optional[str] = Field(None, description="请求中的 start_time")
    end_time: Optional[str] = Field(None, description="请求中的 end_time")
    threshold_quantile: float = Field(..., ge=0.0, le=1.0, description="用于分位数计算的阈值")
    metrics: Dict[str, StatItem] = Field(
        default_factory=dict,
        description="每个参数名对应的统计量字典，键如 'FirstUpPressureMax' 等，值为 StatItem"
    )
