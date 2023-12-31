import json
from typing import Annotated
from fastapi import FastAPI, Request, UploadFile, Form
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
import requests
from dotenv import load_dotenv
import uvicorn
import os
import base64
from time import sleep
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from redis_cache.cache import get_data_from_redis, is_data_stale, set_data_in_redis, update_timestamp
from utils.pipelines import parse_pipelines
from utils.models import Pipeline, Block, DeleteBlock, Description
from statistics.csv_statistics import CSVLoader

load_dotenv()

app = FastAPI()

origins = [
    "http://localhost:3000",
    "https://localhost:3000",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

token: str = ""

expires: float = 0.0


def get_session_token() -> bool:
    url = os.getenv("BASE_URL") + "/api/sessions"
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": os.getenv("API_KEY")
    }
    data = {
        "session": {
            "email": os.getenv("EMAIL"),
            "password": os.getenv("PASSWORD")
        }
    }

    response = requests.post(url, data=json.dumps(data), headers=headers)
    if response.status_code != 200:
        return False

    body = json.loads(response.content.decode('utf-8'))
    global token, expires
    decode_token = json.loads(base64.b64decode(body['session']['token'].split('.')[1].encode('utf-8') + b'=='))
    token = decode_token['token']
    expires = float(decode_token['expires'])
    return True


def check_token_expired() -> bool:
    provided_time = datetime.fromtimestamp(expires)

    current_time = datetime.now()

    if current_time < provided_time:
        return False

    return True


@app.post('/pipeline/create')
async def pipeline_create(name: str, ptype: str):
    result = True
    if check_token_expired():
        result = get_session_token()
    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    if ptype not in ["python", "streaming"]:
        return JSONResponse(status_code=400, content="Only python and streaming are required for type")

    url = f'{os.getenv("BASE_URL")}/api/pipelines'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'X-API-KEY': os.getenv("API_KEY")
    }

    data = {
        "pipeline": {
            "name": name,
            "type": ptype,
            "description": "not created"
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
        return JSONResponse(status_code=response.status_code, content="Something happened with the server!")

    if response.json().get("error") is not None:
        return JSONResponse(status_code=500, content=response.json().get('message'))

    return JSONResponse(status_code=201, content="Pipeline Created")


@app.get('/pipeline/status_once')
async def pipeline_status_once(pipeline_id: int, block_name: str = ""):
    result = True
    if check_token_expired():
        result = get_session_token()
    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    url = f'{os.getenv("BASE_URL")}/api/pipeline_schedules/{pipeline_id}/pipeline_runs?api_key={os.getenv("API_KEY")}'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return JSONResponse(status_code=response.status_code, content="Something happened with the server!")

    json_response = dict(response.json())

    if json_response.get("error") is not None:
        return JSONResponse(status_code=int(json_response.get('code')), content=json_response.get('message'))

    body = json.loads(response.content.decode('utf-8'))['pipeline_runs']
    if len(body) == 0:
        return JSONResponse(status_code=500, content="No pipelines runs")
    else:
        needed_block = None
        for block in body[0]["block_runs"]:
            if block["block_uuid"] == block_name:
                needed_block = block

    return needed_block["status"]


@app.get('/pipeline/status')
async def pipeline_status(pipeline_id: int, block_name: str = ""):
    if check_token_expired():
        get_session_token()

    url = f'{os.getenv("BASE_URL")}/api/pipeline_schedules/{pipeline_id}/pipeline_runs?api_key={os.getenv("API_KEY")}'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return JSONResponse(status_code=response.status_code, content="Something happened with the server!")

    json_response = dict(response.json())

    if json_response.get("error") is not None:
        return JSONResponse(status_code=int(json_response.get('code')), content=json_response.get('message'))

    body = json.loads(response.content.decode('utf-8'))['pipeline_runs']
    if len(body) == 0:
        return JSONResponse(status_code=500, content="No pipelines runs")
    else:
        needed_block = None
        for block in body[0]["block_runs"]:
            if block["block_uuid"] == block_name:
                needed_block = block
        while needed_block["status"] not in ["completed", "failed", "cancelled", "upstream_failed"]:
            print("Status: ", needed_block["status"])
            print("Name: ", needed_block["block_uuid"])
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                return JSONResponse(status_code=response.status_code, content="Something happened with the server!")

            json_response = dict(response.json())

            if json_response.get("error") is not None:
                return JSONResponse(status_code=int(json_response.get('code')), content=json_response.get('message'))

            body = json.loads(response.content.decode('utf-8'))['pipeline_runs']
            for block in body[0]["block_runs"]:
                if block["block_uuid"] == block_name:
                    needed_block = block

        if needed_block["status"] == "completed":
            return JSONResponse(status_code=200, content="completed")
        elif needed_block["status"] == "failed" or needed_block["status"] == "cancelled" or needed_block["status"] \
                == "upstream_failed":
            return JSONResponse(status_code=500, content="failed")
    return JSONResponse(status_code=200, content="status")


@app.get("/block/model")
async def block_model(pipeline_type: str, block_type: str):
    if pipeline_type not in ["batch", "stream"]:
        return JSONResponse(status_code=400, content="Pipeline Type can only be batch or stream!")

    if block_type not in ["loader", "transformer", "exporter"]:
        return JSONResponse(status_code=400, content="Block Type can only be loader, transformer, exporter!")

    if pipeline_type == "batch":
        if block_type == "loader":
            with open("./block_models/batch/loader.py") as f:
                return JSONResponse(content=f.read(), status_code=200)
        elif block_type == "transformer":
            with open("./block_models/batch/transformer.py") as f:
                return JSONResponse(content=f.read(), status_code=200)
        elif block_type == "exporter":
            with open("./block_models/batch/exporter.py") as f:
                return JSONResponse(content=f.read(), status_code=200)
    elif pipeline_type == "stream":
        if block_type == "loader":
            with open("./block_models/streaming/loader.yaml") as f:
                return JSONResponse(content=f.read(), status_code=200)
        elif block_type == "transformer":
            with open("./block_models/streaming/transformer.py") as f:
                return JSONResponse(content=f.read(), status_code=200)
        elif block_type == "exporter":
            with open("./block_models/streaming/exporter.yaml") as f:
                return JSONResponse(content=f.read(), status_code=200)


@app.get('/pipelines')
async def pipelines():
    pipelines_url = os.getenv('BASE_URL') + f'/api/pipelines?api_key={os.getenv("API_KEY")}'
    if check_token_expired():
        get_session_token()
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.get(pipelines_url, headers=headers)

    if response.status_code != 200:
        return JSONResponse(status_code=response.status_code, content="Something happened with the server!")

    json_response = dict(response.json())

    names = []
    for pipe in json_response["pipelines"]:
        tag = None
        for t in pipe.get("tags"):
            if t in ["train", "data_preprocessing"]:
                tag = t
                break
        names.append({
            "name": pipe.get("name"),
            "type": tag
        })
    return JSONResponse(status_code=200, content=names)


@app.get("/pipelines/specific")
async def specific_pipelines(contains: str):

    cache_key = f"pipelines:{contains}"

    cached_data = get_data_from_redis(cache_key)
    if cached_data and not is_data_stale(cache_key, expire_time_seconds=600):
        return JSONResponse(status_code=200, content=json.loads(cached_data.decode()))
        

    result = True
    if check_token_expired():
        result = get_session_token()
    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    url = f'{os.getenv("BASE_URL")}/api/pipelines?api_key={os.getenv("API_KEY")}'
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    response = requests.request("GET", url, headers=headers)

    if response.status_code != 200:
        return JSONResponse(status_code=response.status_code, content="Something happened with the server!")

    if response.json().get("error") is not None:
        return JSONResponse(status_code=response.status_code, content="Something happened with the server!")

    if len(response.json().get("pipelines")) == 0:
        return JSONResponse(status_code=200, content=[])

    pipes = []
    for pipeline in response.json().get("pipelines"):
        resp = requests.request("GET", f'{os.getenv("BASE_URL")}/api/pipelines/{pipeline.get("uuid")}?'
                                       f'api_key={os.getenv("API_KEY")}', headers=headers)

        if resp.status_code == 200:
            if resp.json().get("error") is None:
                pipes.append(resp.json().get("pipeline"))

    pipes = parse_pipelines(pipes, contains)

    set_data_in_redis(cache_key, json.dumps(pipes), expire_time_seconds=600)

    update_timestamp(cache_key)

    return JSONResponse(status_code=200, content=pipes)


@app.post("/pipeline/run")
async def run_pipeline(pipe: Pipeline):
    result = True
    if check_token_expired():
        result = get_session_token()
    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    url = f"{os.getenv('BASE_URL')}/api/pipeline_schedules/{pipe.run_id}/api_trigger"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {pipe.token}"
    }

    body = {
        "pipeline_run": {
            "variables": {

            }
        }
    }
    for k, v in pipe.variables.items():
        body['pipeline_run']['variables'][k] = v

    response = requests.post(url, headers=headers, data=json.dumps(body, indent=4))

    if response.status_code != 200 or response.json().get("error") is not None:
        return JSONResponse(status_code=500, content="Starting the pipeline didn't work!")

    return JSONResponse(status_code=201, content="Pipeline Started Successfully!")


@app.delete("/pipeline/delete")
async def delete_pipeline(name: str):
    result = True
    if check_token_expired():
        result = get_session_token()
    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "X-API-KEY": os.getenv("API_KEY")
    }

    url = f'{os.getenv("BASE_URL")}/api/pipelines/{name}'

    response = requests.request("DELETE", url, headers=headers)

    if response.status_code != 200:
        return JSONResponse(status_code=500, content="Something happened when deleting the pipeline!")

    if response.json().get("error") is not None:
        return JSONResponse(status_code=500, content="Something happened when deleting the pipeline!")

    return JSONResponse(status_code=200, content="Pipeline deleted successfully!")


@app.get("/pipeline/read")
async def read_pipeline(pipeline_name: str):
    result = True
    if check_token_expired():
        result = get_session_token()
    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    if pipeline_name != "":
        headers = {
            "Authorization": f"Bearer {token}"
        }

        response = requests.get(f'{os.getenv("BASE_URL")}/api/pipelines/{pipeline_name}?api_key='
                                f'{os.getenv("API_KEY")}', headers=headers)

        if response.status_code != 200:
            return JSONResponse(status_code=500, content="Could not get pipeline result!")

        return JSONResponse(status_code=200, content=json.loads(response.content.decode('utf-8')))

    return JSONResponse(status_code=400, content="Pipeline name should not be empty!")


@app.get("/pipeline/run_data")
async def run_tag(pipeline_name: str):
    result = True
    if check_token_expired():
        result = get_session_token()

    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    url = f'{os.getenv("BASE_URL")}/api/pipelines/{pipeline_name}?api_key={os.getenv("API_KEY")}'

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.request("GET", url, headers=headers)

    if response.status_code != 200:
        return JSONResponse(content="Could not get pipeline data!", status_code=500)

    if response.json().get("error") is not None:
        return JSONResponse(content="Error when getting the pipeline!", status_code=500)

    tag = {}

    for t in response.json()["pipeline"]["tags"]:
        if "run_id" in t:
            tag["run_id"] = int(t.split(":")[1].strip())
        elif "token" in t:
            tag["token"] = t.split(":")[1].strip()

    if tag == {}:
        return JSONResponse(content="There are no run tags for this pipeline", status_code=404)

    return JSONResponse(content=tag, status_code=200)


@app.get("/block/read")
async def read_block(block_name: str, pipeline_name: str):
    result = True
    if check_token_expired():
        result = get_session_token()

    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    if block_name != "" and pipeline_name != "":
        headers = {
            "Authorization": f"Bearer {token}"
        }
        response = requests.get(f'{os.getenv("BASE_URL")}/api/pipelines/{pipeline_name}/blocks/{block_name}?api_key='
                                f'{os.getenv("API_KEY")}', headers=headers)
        if response.status_code != 200:
            return JSONResponse(status_code=500, content="Could not get pipeline result!")

        return JSONResponse(content=response.json(), status_code=200)

    return JSONResponse(status_code=400, content="Pipeline name and Block name should not be empty!")


@app.put("/pipeline/description")
async def put_description(desc: Description):
    result = True
    if check_token_expired():
        result = get_session_token()

    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    url = f'{os.getenv("BASE_URL")}/api/pipelines/{desc.name}'

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "X-API-KEY": os.getenv("API_KEY")
    }

    data = {
        "pipeline": {
            "description": desc.description
        }
    }

    payload = json.dumps(data, indent=4)

    response = requests.request("PUT", url, headers=headers, data=payload)

    if response.status_code != 200:
        return JSONResponse(status_code=400, content="Bad Request!")

    if response.json().get("error") is not None:
        return JSONResponse(status_code=500, content="Error from server!")

    return JSONResponse(status_code=200, content="Pipeline updated successfully!")


@app.get("/pipeline/description")
async def description(name: str):
    result = True
    if check_token_expired():
        result = get_session_token()

    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    url = f'{os.getenv("BASE_URL")}/api/pipelines/{name}?api_key={os.getenv("API_KEY")}'

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    response = requests.request("GET", url, headers=headers)

    if response.status_code != 200:
        return JSONResponse(status_code=400, content="Bad Request!")

    if response.json().get("error") is not None:
        return JSONResponse(status_code=500, content="Error from server!")

    return JSONResponse(status_code=200, content=response.json()["pipeline"]["description"])


@app.post("/block/create")
async def block_create(block_name: Annotated[str, Form()], block_type: Annotated[str, Form()],
                       pipeline_name: Annotated[str, Form()],
                       downstream_blocks: Annotated[list[str], Form()], upstream_blocks: Annotated[list[str], Form()],
                       language: Annotated[str, Form()],
                       file: UploadFile):
    result = True
    if check_token_expired():
        result = get_session_token()

    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    file_data = file.file.read().decode("utf-8").replace("\n", "\\n")

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Bearer {token}",
        "X-API-KEY": os.getenv("API_KEY")
    }

    payload = {
        "block": {
            "name": block_name,
            "language": f"{language}",
            "type": f"{block_type}",
            "content": f"{file_data}",
            "downstream_blocks": downstream_blocks,
            "upstream_blocks": upstream_blocks
        },
        "api-key": os.getenv("API_KEY")
    }
    payload = json.dumps(payload).replace("\\\\", "\\")
    response = requests.request("POST", url=f'{os.getenv("base_url")}/api/pipelines/{pipeline_name}/blocks?'
                                            f'api_key={os.getenv("API_KEY")}', headers=headers, data=payload)
    if response.status_code != 200:
        return JSONResponse(status_code=500, content="Could not create block!")

    if response.json().get("error") is not None:
        print(response.json().get("error"))
        return JSONResponse(status_code=500, content="Error occurred when creating the block!")

    return JSONResponse(status_code=200, content="Block Created!")


@app.put("/block/update")
async def update_block(block: Block):
    result = True
    if check_token_expired():
        result = get_session_token()

    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    if block.block_name != "" and block.pipeline_name != "" and block.content != "":
        headers = {
            'Accept': 'application/json, text/plain, */*',
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-API-KEY": os.getenv("API_KEY")
        }
        payload = f'{{"block": {{"downstream_blocks": {block.downstream_blocks}, "upstream_blocks": {block.upstream_blocks}}}}}'
        response = requests.request("PUT", url=f'{os.getenv("BASE_URL")}/api/pipelines/{block.pipeline_name}'
                                               f'/blocks/{block.block_name}?api_key={os.getenv("API_KEY")}',
                                    headers=headers, data=payload)
        if response.status_code != 200:
            return JSONResponse(status_code=500, content="Could not update block!")

        return JSONResponse(status_code=200, content=json.loads(response.content.decode('utf-8')))

    return JSONResponse(status_code=400, content="Body Should not be empty!")


@app.delete("/block/delete")
async def delete_block(block: DeleteBlock):
    result = True
    if check_token_expired():
        result = get_session_token()

    if not result:
        return JSONResponse(status_code=500, content="Could not get the token!")

    if block.block_name == "" and block.pipeline_name == "":
        return JSONResponse(status_code=400, content="Block should not be empty!")

    response = requests.delete(f'{os.getenv("base_url")}/api/pipelines/{block.pipeline_name}/'
                                          f'blocks/{block.block_name}?block_type={block.block_type}&'
                                          f'api_key={os.getenv("API_KEY")}&force={block.force}')

    if response.status_code != 200:
        return JSONResponse(status_code=500, content="Could not delete block!")

    if response.json().get("error") is not None:
        return JSONResponse(status_code=500, content="Error occurred when deleting block!")

    return JSONResponse(status_code=200, content="Block Deleted!")


@app.get('/get_statistics')
async def get_statistics(dataset_path: str, req: Request):
    auth_token = req.headers.get('Authorization')

    if auth_token is None:
        return JSONResponse(status_code=401, content="You don't have access to this component")

    csv_loader = CSVLoader(path=dataset_path)
    csv_loader.execute(auth_token.split(" ")[0])

    return JSONResponse(status_code=200, content=csv_loader.get_statistics())


if __name__ == '__main__':
    get_session_token()

    uvicorn.run(app, host="0.0.0.0", port=7000)

