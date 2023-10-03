data_loaders = {
    "csv_loader": {
        "id": 1,
        "run_id": "7737734d3dd64a0aa02c2ead94316883"
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


def find_pipeline(pipeline_type: str) -> dict | str:
    if data_loaders.get(pipeline_type) is not None:
        return data_loaders[pipeline_type]
    elif data_transformers.get(pipeline_type) is not None:
        return data_transformers[pipeline_type]
    elif data_exporters.get(pipeline_type) is not None:
        return data_exporters[pipeline_type]

    return "Type not found!"
