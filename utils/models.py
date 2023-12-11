from pydantic import BaseModel


class Pipeline(BaseModel):
    variables: dict[str, str]
    name: str


class Block(BaseModel):
    block_name: str
    pipeline_name: str
    content: str
