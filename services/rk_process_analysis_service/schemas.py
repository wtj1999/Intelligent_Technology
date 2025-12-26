from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Field

class RuKeQuery(BaseModel):
    DEVICECODE: str = Field(..., description="设备编码")
    DEVICETIME: str = Field(..., description="设备时间")
    TENANTID: str = Field(..., description="租户ID")
    PRKDH001: str = Field(..., description="工位1盖板码")
    PRKDH002: str = Field(..., description="工位2盖板码")
    PRKDH003: str = Field(..., description="工位3盖板码")
    PRKDH004: str = Field(..., description="工位4盖板码")
    PRKDH005: List[float] = Field(..., description="工位1压力1（单位：N）")
    PRKDH006: List[float] = Field(..., description="工位1压力2（单位：N）")
    PRKDH007: List[float] = Field(..., description="工位2压力1（单位：N）")
    PRKDH008: List[float] = Field(..., description="工位2压力2（单位：N）")
    PRKDH009: List[float] = Field(..., description="工位3压力1（单位：N）")
    PRKDH010: List[float] = Field(..., description="工位3压力2（单位：N）")
    PRKDH011: List[float] = Field(..., description="工位4压力1（单位：N）")
    PRKDH012: List[float] = Field(..., description="工位4压力2（单位：N）")
    PRKDH014: List[float] = Field(..., description="入壳A电芯平移轴当前位置")
    PRKDH016: List[float] = Field(..., description="入壳B电芯平移轴当前位置")
    PRKDH018: List[float] = Field(..., description="入壳C电芯平移轴当前位置")
    PRKDH020: List[float] = Field(..., description="入壳D电芯平移轴当前位置")


class SegmentItem(BaseModel):
    label: str = Field(..., description="阶段名称，如 '推80%入壳'")
    range: Tuple[int, int] = Field(..., description="闭区间的开始和结束索引 (start, end)")


class PressureBlock(BaseModel):
    FirstPressureMax: Optional[float] = None
    FirstPressureSlope: Optional[float] = None
    SecondPressureMax: Optional[float] = None
    SecondPressureSlope: Optional[float] = None
    ThirdPressureMax: Optional[float] = None
    ThirdPressureSlope: Optional[float] = None


class StationResult(BaseModel):
    gaiban_code: Optional[Any] = None
    status: str = Field(..., description='Either "OK" or "NG"')
    rising_segments: Optional[List[Tuple[int, int]]] = None
    all_segments: Optional[List[SegmentItem]] = None
    pressures: Optional[PressureBlock] = None
    pressure1_series: List[float] = Field(default_factory=list)
    pressure2_series: List[float] = Field(default_factory=list)
    position_series: List[float] = Field(default_factory=list)


class RuKeResponse(BaseModel):
    device_code: Optional[str] = None
    device_time: Optional[str] = None
    tenant: Optional[str] = None
    stations: Dict[str, StationResult] = Field(default_factory=dict)