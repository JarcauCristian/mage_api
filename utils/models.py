from pydantic import BaseModel


class Pipeline(BaseModel):
    variables: dict[str, str]
    name: str
