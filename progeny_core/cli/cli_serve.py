import requests
import datetime
import sanic
import json
import ruamel.yaml as yaml
from typing import Dict, List, Any, Optional
from loguru import logger
import sys
import time

from progeny_core.spinner import Progeny

fmt = "[{time}] [{level: <8}] ({name: ^15}) | [{host}]: {request} {message} {status} {byte}"

logger.add(
    sys.stdout,
    format=fmt,
    filter="cli_serve",
    level="DEBUG",
    colorize=True,
    enqueue=True,
)
logger.add("logs.log", level="DEBUG", enqueue=True)


app = sanic.Sanic("middleware_progeny")


def make_response(res, res_type):
    if res_type == "json":
        return sanic.response.json(res.json())
    elif res_type == "html":
        return sanic.response.html(res.text)
    elif res_type == "raw":
        return sanic.response.raw(res.content)
    elif res_type == "text":
        return sanic.response.text(res.text)


def raw_res(res):
    return make_response(res, "raw")


def html_res(res):
    return make_response(res, "html")


def json_res(res):
    return make_response(res, "json")


def text_res(res):
    return make_response(res, "text")


def fetch(
    method, req, pathf, protocol="http", host="0.0.0.0", json=False, **kwargs
):

    port = req.ctx.port
    payload = {}
    if json:
        payload = req.json

    url = f'{protocol}://{host}:{port}/{pathf.format(**kwargs).lstrip("/")}'
    if method == "get":
        return requests.get(url)
    elif method == "post":
        return requests.post(url, json=payload)


@app.middleware("request")
async def set_port_from_cookie(request):
    log_port = "no prodigy port found"
    try:
        request.ctx.port = request.cookies.get("prodigy_port")
        log_port = f"{request.ctx.port}"
    except:
        pass
    request.ctx.log_info = f'{request.headers.get("x-forwarded-for", "unknown client ip")} - {log_port} - {request.path}'
    logger.debug(request.ctx.log_info)


@app.get("/start_session/<port>")
def start_session(request, port):
    res = sanic.response.redirect("/mylayerserver")
    res.cookies["prodigy_port"] = port
    res.cookies["prodigy_port"]["expires"] = datetime.datetime(
        year=9999, month=12, day=31, hour=23, minute=59, second=59
    )
    return res


@app.get("/mylayerserver")
def get_root(request):
    res = html_res(fetch("get", request, "/"))
    logger.info(f"{request.ctx.log_info} | Loaded the Prodigy interface")
    return res


@app.get("/fonts/<font_file>")
def get_fonts(request, font_file):
    return raw_res(fetch("get", request, "/fonts/{font_file}", font_file=font_file))


@app.get("/index.html")
def get_index(request):
    return html_res(fetch("get", request, "/index.html"))


@app.get("/bundle.js")
def get_bundle(request):
    return html_res(fetch("get", request, "/bundle.js"))


@app.get("/favicon.ico")
def get_favicon(request):
    return raw_res(fetch("get", request, "/favicon.ico"))


@app.get("/version")
def get_version(request):
    return json_res(fetch("get", request, "/version"))


@app.get("/get_questions")
def get_questions(request):
    res = json_res(fetch("get", request, "/get_questions"))
    logger.info(
        f"{request.ctx.log_info} | {len(json.loads(res.body)['tasks'])} new tasks fetched"
    )
    return res


@app.get("/project")
def get_project(request):
    return json_res(fetch("get", request, "/project"))


@app.get("/project/<session_id>")
def get_project_session(request, session_id):
    return json_res(
        fetch("get", request, "/project/{session_id}", session_id=session_id)
    )


@app.post("/get_session_questions")
def get_session_questions(request):
    res = json_res(fetch("post", request, "/get_session_questions", json=True))
    logger.info(
        f"{request.ctx.log_info} | {len(json.loads(res.body)['tasks'])} new tasks fetched"
    )
    return res


@app.post("/set_session_aliases")
def set_session_aliases(request):
    return json_res(fetch("post", request, "/set_session_aliases", json=True))


@app.post("/end_session")
def end_session(request):
    return json_res(fetch("post", request, "/end_session", json=True))


@app.post("/validate_answer")
def validate_answer(request):
    return json_res(fetch("post", request, "/validate_answer", json=True))


@app.post("/give_answers")
def give_answers(request):
    n = len(json.loads(request.body)["answers"])
    res = json_res(fetch("post", request, "/give_answers", json=True))
    logger.info(f"{request.ctx.log_info} | {n} answers to be sent to DB")
    return res


def parse_config(config_filepath: str) -> Dict[str, Any]:
    with open(config_filepath, "r") as f:
        config = yaml.YAML(typ="safe").load(f)

    assert {"app", "progeny", "instances"}.issubset(set(config.keys()))
    assert {"host", "port"}.issubset(set(config["app"].keys()))

    try:
        assert config["progeny"].get("scheduled_cleaning_interval") is None
        assert config["progeny"].get("scheduled_cleaning_timeout") is None
    except AssertionError:
        raise NotImplementedError(
            "Middleware doesn't yet support on-demand instances."
        )

    return config


def run(args):

    config = parse_config(args["<config>"])

    progenitor = Progeny(**config["progeny"])

    app.ctx.progenitor = progenitor
    app.ctx.instances = {}

    for identifier, params in config["instances"].items():
        port, session_name = progenitor.spin(identifier=identifier, **params)
        app.ctx.instances["identifier"] = {
            "port": port,
            "session_name": session_name,
        }
        time.sleep(1)

    app.run(**config["app"])
