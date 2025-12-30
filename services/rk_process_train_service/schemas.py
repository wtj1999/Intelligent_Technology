from pydantic import BaseModel, Field


class TrainRequest(BaseModel):
    STARTTIME: str = Field(..., description="查询起始时间，SQL 可接受的时间字符串")
    ENDTIME: str = Field(..., description="查询结束时间，SQL 可接受的时间字符串")