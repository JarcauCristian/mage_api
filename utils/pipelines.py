from typing import List, Any, Dict

data_loaders = {
    "csv_loader": {
        "id": 4,
        "run_id": "e966058741f542ddbfa82046e9edce4a"
    }
}

data_transformers = {
    "csv_transformers": {
        "id": 1,
        "run_id": ""
    }
}

data_exporters = {
    "csv_exporters": {
        "id": 1,
        "run_id": ""
    }
}


def parse_pipelines(pipelines: List[Dict[str, Any]], pipeline_type: str = ""):
    parsed_pipelines = []
    for pipeline in pipelines:
        blocks = []
        if pipeline.get("uuid").find("licenta") != -1 and pipeline.get("uuid").find(pipeline_type) != -1:
            for block in pipeline["blocks"]:
                blocks.append({
                    "name": block["uuid"],
                    "type": block["type"],
                    "variables": list(dict(block.get("configuration")).keys())
                })
            parsed_pipelines.append({
                "name": pipeline.get("uuid"),
                "description": pipeline.get("description"),
                "blocks": blocks
            })
    return parsed_pipelines


def find_pipeline(pipeline_type: str) -> dict | bool:
    if data_loaders.get(pipeline_type) is not None:
        return data_loaders[pipeline_type]
    elif data_transformers.get(pipeline_type) is not None:
        return data_transformers[pipeline_type]
    elif data_exporters.get(pipeline_type) is not None:
        return data_exporters[pipeline_type]

    return False
