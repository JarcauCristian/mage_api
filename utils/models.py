from pydantic import BaseModel


class Pipeline(BaseModel):
    variables: dict[str, str]
    name: str


class Block(BaseModel):
    block_name: str
    pipeline_name: str


class DeleteBlock(BaseModel):
    block_name: str
    block_type: str
    pipeline_name: str
    force: bool
