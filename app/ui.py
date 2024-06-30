from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from utils.db import db

app = FastAPI()
db = db()

templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

## HOMESCREEN
# Strategy list with names and descriptions. Able to select a strategy.
@app.get("/")
async def read_root(request: Request):
    return templates.TemplateResponse("page/index.html", {"request": request, "title": "My FastAPI App"})

@app.get("/strategies")
async def read_strategies(request: Request):
    strategies = db.get_strategies()
    return templates.TemplateResponse("partial/strategy_selector.html", {"request": request, "strategies": strategies})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
