from typing import List, Any, Dict


def parse_pipelines(pipelines: List[Dict[str, Any]]):
    parsed_pipelines = []
    for pipeline in pipelines:
        blocks = []
        if pipeline.get("uuid").find("licenta") != -1:
            for block in pipeline["blocks"]:
                blocks.append({
                    "name": block["uuid"],
                    "type": block["type"],
                    "upstream_blocks": block["upstream_blocks"],
                    "downstream_blocks": block["downstream_blocks"],
                    "variables": list(dict(block.get("configuration")).keys())
                })
            parsed_pipelines.append({
                "name": pipeline.get("uuid"),
                "description": pipeline.get("description"),
                "blocks": blocks
            })
    return parsed_pipelines
