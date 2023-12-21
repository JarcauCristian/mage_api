from typing import Any, Dict, List

from pydantic import BaseModel


class Pipeline(BaseModel):
    variables: Dict[str, Any]
    run_id: int
    token: str


class Description(BaseModel):
    name: str
    description: str


class Block(BaseModel):
    block_name: str
    pipeline_name: str
    downstream_blocks: List[str]
    upstream_blocks: List[str]


class DeleteBlock(BaseModel):
    block_name: str
    block_type: str
    pipeline_name: str
    force: bool
