import json
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, WebSocket
import requests
from dotenv import load_dotenv
import uvicorn
import os
import base64
from time import sleep
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse
from utils.pipelines import find_pipeline, parse_pipelines
from utils.models import Pipeline

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
        "X-API-KEY": os.getenv("API-KEY")
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


@app.get('/pipeline/status')
async def pipeline_status(pipeline_id: int, block_name: str = ""):
    if check_token_expired():
        get_session_token()

    url = f'{os.getenv("BASE_URL")}/api/pipeline_schedules/{pipeline_id}/pipeline_runs?api_key={os.getenv("API-KEY")}'
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
        return JSONResponse(status_code=500, content="No pipelines")
    else:
            needed_block = None
            for block in body[0]["block_runs"]:
                if block["block_uuid"] == block_name:
                    needed_block = block
            if (datetime.now(timezone.utc) - datetime.strptime(needed_block["completed_at"], '%Y-%m-%d %H:%M:%S.%f%z')) > timedelta(minutes=2):
                while (datetime.now(timezone.utc) - datetime.strptime(needed_block["completed_at"], '%Y-%m-%d %H:%M:%S.%f%z')) < timedelta(seconds=30):
                    continue
                return JSONResponse(status_code=200, content="Block Runed Successfully!")
    # while body['status'] != "completed":
    #     if body['status'] == "failed":
    #         break
    #     url = f'{os.getenv("BASE_URL")}/api/pipeline_schedules/{pipeline_id}/pipeline_runs?api_key={os.getenv("API-KEY")}'
    #     headers = {
    #         'Content-Type': 'application/json',
    #         'Authorization': f'Bearer {token}'
    #     }

    #     response = requests.get(url, headers=headers)

    #     if response.status_code != 200:
    #         return JSONResponse(status_code=500, content="Could not get the status for the pipeline!")

    #     body = json.loads(response.content.decode('utf-8'))['pipeline_runs'][0]
    #     sleep(.5)
    return JSONResponse(status_code=200, content="status")


@app.get('/pipelines')
async def pipelines(pipeline_type: str = ""):
    pipelines_url = os.getenv('BASE_URL') + f'/api/pipelines?api_key={os.getenv("API-KEY")}'
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

    if json_response.get("error") is not None:
        return JSONResponse(status_code=int(json_response.get('code')), content=json_response.get('message'))
    pipeline_names = []
    print(json_response['pipelines'])
    for pipeline in json_response['pipelines']:
        pipeline_names.append(pipeline.get("uuid"))
    return JSONResponse(status_code=200, content=pipeline_names)


@app.post("/pipeline/run")
async def run_pipeline(pipeline: Pipeline):

    if pipeline is None:
        return JSONResponse(status_code=400, content="The body of the requests is incorrect!")

    desire_pipeline = find_pipeline(pipeline.name)

    url = (f"{os.getenv('BASE_URL')}/api/pipeline_schedules/{desire_pipeline['id']}/pipeline_runs/"
           f"{desire_pipeline['run_id']}")
    body = {
        "pipeline_run": {
            "variables": {

            }
        }
    }
    for k, v in pipeline.variables.items():
        body['pipeline_run']['variables'][k] = v

    response = requests.post(url, data=json.dumps(body, indent=4))

    if response.status_code != 200:
        return JSONResponse(status_code=500, content="Starting the pipeline didn't work!")

    return JSONResponse(status_code=201, content=desire_pipeline)


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
                                f'{os.getenv("API-KEY")}', headers=headers)

        if response.status_code != 200:
            return JSONResponse(status_code=500, content="Could not get pipeline result!")

        return JSONResponse(status_code=200, content=json.loads(response.content.decode('utf-8')))

    return JSONResponse(status_code=400, content="Pipeline name should not be empty!")


if __name__ == '__main__':
    get_session_token()

    uvicorn.run(app, host="0.0.0.0", port=7000)

