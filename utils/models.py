from pydantic import BaseModel


class RunPipeline(BaseModel):
    pipeline_id: int
    trigger_id: str
    variables: dict[str, str]
