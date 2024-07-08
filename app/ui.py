from fastapi import FastAPI, Request, Query
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
    instruments = db.get_instrument_batch(0, 10)
    return templates.TemplateResponse("page/index.html", {"request": request, "title": "My FastAPI App","strategies": strategies,"instruments": instruments, "offset":0})

# View strategy types
@app.get("type/strategies")
async def read_strategy_types(request: Request):
    strategies = db.get_strategy_types()
    return templates.TemplateResponse("partial/strategy_type_selector.html", {"request": request, "strategies": strategies})

#Add button for creating a new strategy
@app.get("/strategies")
async def read_strategies(request: Request):
    strategies = db.get_strategies()
    return templates.TemplateResponse("partial/strategy_selector.html", {"request": request, "strategies": strategies})

## STRATEGY VIEWER
# Show strategy information
# Show baskets on right side
# Show instruments on bottom
@app.get("/strategy/{strategy_id}")
async def read_strategy(request: Request, strategy_id: int):
    strategy = db.get_strategy_by_id(strategy_id)
    return templates.TemplateResponse("page/strategy.html", {"request": request, "strategy": strategy})

# View strategy identifiers
@app.get("/strategy/{strategy_id}/identifiers")
async def read_strategy_identifiers(request: Request, strategy_id: int):
    identifiers = db.get_strategy_identifiers_by_strategy_id(strategy_id)
    return templates.TemplateResponse("partial/identifier_selector.html", {"request": request, "identifiers": identifiers})

# Return all methodologies for strategy
@app.get("/strategy/{strategy_id}/methodologies")
async def read_strategy_methodologies(request: Request, strategy_id: int):
    methodologies = db.get_methodologies_by_strategy_id(strategy_id)
    return templates.TemplateResponse("partial/methodology_selector.html", {"request": request, "methodologies": methodologies})

# Return all instruments for strategy
@app.get("/strategy/{strategy_id}/instruments")
async def read_strategy_instruments(request: Request, strategy_id: int):
    instruments = db.get_instruments_by_strategy_id(strategy_id)
    return templates.TemplateResponse("partial/instrument_selector.html", {"request": request, "instruments": instruments})

# Return all instrument classifications for strategy
@app.get("/strategy/{strategy_id}/instruments/classifications")
async def read_strategy_instrument_classifications(request: Request, strategy_id: int):
    classifications = db.get_classifications_by_strategy_id(strategy_id)
    return templates.TemplateResponse("partial/classification_selector.html", {"request": request, "classifications": classifications})

# Return all methodologies
@app.get("/methodologies")
async def read_methodologies(request: Request):
    methodologies = db.get_all_methodologies()
    return templates.TemplateResponse("partial/methodology_selector.html", {"request": request, "methodologies": methodologies})

# @app.get("/more-instruments")
# async def read_more_instruments(request: Request, offset: int = Query(0), limit: int = Query(10)):
#     instruments = db.get_instrument_batch(offset, limit)
#     return templates.TemplateResponse("partial/instrument_selector.html", {"request": request, "instruments": instruments})

# Return all instruments paginated
@app.get("/instruments")
async def read_instruments(request: Request, offset: int = Query(0), limit: int = Query(10)):
    instruments = db.get_instrument_batch(offset, limit)
    return templates.TemplateResponse("partial/instrument_selector.html", {"request": request, "instruments": instruments, "offset":offset, "limit":limit})

# Return information for a specific instrument
@app.get("/instrument/{instrument_id}")
async def read_instrument(request: Request, instrument_id: int):
    instrument = db.get_instrument_by_id(instrument_id)
    return templates.TemplateResponse("page/instrument.html", {"request": request, "instrument": instrument})

# Return classifications for a specific instrument
@app.get("/instrument/{instrument_id}/classifications")
async def read_instrument_classifications(request: Request, instrument_id: int):
    classifications = db.get_classifications_by_instrument_id(instrument_id)
    return templates.TemplateResponse("partial/classification_selector.html", {"request": request, "classifications": classifications})

# Return instrument identifiers for a specific instrument 
@app.get("/instrument/{instrument_id}/identifiers")
async def read_instrument_identifiers(request: Request, instrument_id: int):
    identifiers = db.get_instrument_identifiers_by_instrument_id(instrument_id)
    return templates.TemplateResponse("partial/identifier_selector.html", {"request": request, "identifiers": identifiers})

# Return all classifications
@app.get("/classifications")
async def read_classifications(request: Request):
    classifications = db.get_all_classifications()
    return templates.TemplateResponse("partial/classification_selector.html", {"request": request, "classifications": classifications})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
