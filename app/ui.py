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
    strategies = db.get_strategies()
    return templates.TemplateResponse("page/index.html", {"request": request, "title": "My FastAPI App","strategies": strategies})

@app.get("/strategies")
async def read_strategies(request: Request):
    strategies = db.get_strategies()
    return templates.TemplateResponse("partial/strategy_selector.html", {"request": request, "strategies": strategies})

## STRATEGY VIEWER
# Show strategy information
# Show baskets on right side
# Show instruments on bottom
@app.get("/strategies/{strategy_id}")
async def read_strategy(request: Request, strategy_id: int):
    strategy = db.get_strategy_by_id(strategy_id)
    return templates.TemplateResponse("page/strategy.html", {"request": request, "strategy": strategy})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
