from typing import Any, Dict

from pydantic import BaseModel


class Pipeline(BaseModel):
    variables: Dict[str, Any]
    run_id: int
    token: str


class Block(BaseModel):
    block_name: str
    pipeline_name: str


class DeleteBlock(BaseModel):
    block_name: str
    block_type: str
    pipeline_name: str
    force: bool
