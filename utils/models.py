from pydantic import BaseModel


class Pipeline(BaseModel):
    variables: dict[str, str]
    type: str
