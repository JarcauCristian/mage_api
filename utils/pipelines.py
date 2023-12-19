from typing import List, Any, Dict


def parse_pipelines(pipelines: List[Dict[str, Any]], contains: str):
    parsed_pipelines = []
    for pipeline in pipelines:
        if contains in pipeline.get("uuid"):
            blocks = []
            for block in pipeline["blocks"]:
                blocks.append({
                    "name": block["uuid"],
                    "type": block["type"],
                    "language": block["language"],
                    "content": block["content"],
                    "upstream_blocks": block["upstream_blocks"],
                    "downstream_blocks": block["downstream_blocks"],
                    "variables": list(dict(block.get("configuration")).keys())
                })
            parsed_pipelines.append({
                "name": pipeline.get("uuid"),
                "type": pipeline.get("type"),
                "blocks": blocks
            })
    return parsed_pipelines
