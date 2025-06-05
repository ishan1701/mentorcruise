from pydantic import (
    BaseModel,
    field_validator,
    Field,
    model_serializer,
)
from datetime import datetime
from loguru import logger
from typing import Any


class ProductSales(BaseModel):
    product_id: str = Field(max_length=10)
    quantity: int = Field(gt=0)
    price: float = Field(gt=0.0)
    timestamp: datetime

    @field_validator("timestamp")
    def validate_timestamp(cls, v):
        if v.year < 2000:
            raise ValueError("year cant be less than 2000")
        return v

    @model_serializer
    def serialize(self):
        """This method should be deprecated in future versions.
           model_dump() is a built-in method of Pydantic v2 models and works out of the box.
           You only need to use @model_serializer if you want to customize how your model is serialized
           (for example, to change the output format or add custom logic). For standard serialization to a dictionary, model_dump() is sufficient"""

        logger.info("Serializing ProductSales object")
        return {
            "product_id": self.product_id,
            "quantity": self.quantity,
            "price": self.price,
            "timestamp": str(self.timestamp),
        }

    @classmethod
    def model_schema(cls):
        """
        Returns the schema of the model.
        """
        schema: dict[str, Any] = {
            name: field.annotation.__name__ for name, field in cls.model_fields.items()
        }
        logger.info(f"The model schema is : {schema}")
        return schema


# local testing
if __name__ == "__main__":
    p_sales = ProductSales(
        product_id="23", quantity=5, price=5.0, timestamp=datetime.now()
    )

    print(p_sales.model_dump())
    print(ProductSales.schema())
