from pydantic import BaseModel, field_validator, model_validator, Field, validator
from datetime import datetime


class ProductSales(BaseModel):
    product_id: str = Field(max_length=10)
    quantity: int = Field(gt=0)
    price: float = Field(gt=0.0)
    timestamp: datetime = Field(gt=datetime.now())

    @field_validator("timestamp")
    def validate_timestamp(cls, v):
        if v.year < 2000:
            raise ValueError("year cant be less than 2000")
        return v
