import json
from datetime import datetime
from fastapi import FastAPI
import requests
from dotenv import load_dotenv
import uvicorn
import os
import base64
from time import sleep
from starlette.responses import JSONResponse
from utils.models import RunPipeline

load_dotenv()

app = FastAPI()

token: str = ""

expires: float = 0.0


def get_session_token() -> dict | None:
    url = os.getenv("BASE_URL") + "/api/sessions"
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": os.getenv("X-API-KEY")
    }
    data = {
        "session": {
            "email": os.getenv("EMAIL"),
            "password": os.getenv("PASSWORD")
        }
    }

    response = requests.post(url, data=json.dumps(data), headers=headers)

    if response.status_code != 200:
        return None

    body = json.loads(response.content.decode('utf-8'))
    global token, expires
    decode_token = json.loads(base64.b64decode(body['session']['token'].split('.')[1].encode('utf-8') + b'=='))
    token = decode_token['token']
    expires = float(decode_token['expires'])


def check_token_expired() -> bool:
    provided_time = datetime.fromtimestamp(expires)

    current_time = datetime.now()

    if current_time < provided_time:
        return False

    return True


@app.get('/pipeline_status')
async def pipeline_status(pipeline_id: int):
    if check_token_expired():
        get_session_token()

    url = f'{os.getenv("BASE_URL")}/api/pipeline_schedules/{pipeline_id}/pipeline_runs?api_key={os.getenv("X-API-KEY")}'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        return JSONResponse(status_code=500, content="Could not get the status for the pipeline!")

    body = json.loads(response.content.decode('utf-8'))['pipeline_runs'][0]
    while body['status'] != "completed":
        if body['status'] == "failed":
            break
        url = f'{os.getenv("BASE_URL")}/api/pipeline_schedules/{pipeline_id}/pipeline_runs?api_key={os.getenv("X-API-KEY")}'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            return JSONResponse(status_code=500, content="Could not get the status for the pipeline!")

        body = json.loads(response.content.decode('utf-8'))['pipeline_runs'][0]
        sleep(.5)
    return JSONResponse(status_code=200, content={"status"})


@app.post("/run_pipeline")
async def run_pipeline(run: RunPipeline):
    url = f"{os.getenv('BASE_URL')}/api/pipeline_schedules/{run.pipeline_id}/pipeline_runs/{run.trigger_id}"
    headers = {
        "pipeline_run": {
            "variables": {
                json.dumps(run.variables)
            }
        }
    }
    response = requests.post(url, headers=headers)

    if response.status_code != 200:
        return JSONResponse(status_code=500, content="Starting the pipeline didn't work!")

    return JSONResponse(status_code=201, content="Pipeline started successfully!")


if __name__ == '__main__':
    get_session_token()

    uvicorn.run(app, host="0.0.0.0", port=7000)
