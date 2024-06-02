from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Dict, List, Optional
from strategy.Strategy import Strategy

from utils.db import db, get_db

# --------------------------------------------------------------------------------
# Router
# --------------------------------------------------------------------------------

router = APIRouter(
  prefix="/api",
  tags=["API"]
)

# STRATEGIES ---------------------------------------------------------------------
@router.get(
  path="/strategies",
  summary="Get all strategies",
  response_model=List[Strategy]
)
async def get_strategies(
  storage: db = Depends(get_db)
) -> list[Strategy]:
  """Gets the list of all reminder lists owned by the user."""

  return storage.get_strategies()

#-----------------------------------

@router.post(
  path="/strategy",
  summary="Create a new strategy",
  response_model=Strategy
)
async def post_reminders(
  new_strategy: dict, # Or Strategy?
  storage: db = Depends(get_db)
) -> Strategy:
  """Creates a new strategy for the user."""

  strategy_id = storage.create_strategy(new_strategy)
  return storage.get_strategy_by_id(strategy_id)

#-----------------------------------

@router.get(
  path="/reminders/{list_id}",
  summary="Get a reminder list by ID",
  response_model=ReminderList
)
async def get_list_id(
  list_id: int,
  storage: ReminderStorage = Depends(get_storage_for_api)
) -> ReminderList:
  """Gets a reminder list by ID."""

  return storage.get_list(list_id)

#-----------------------------------

@router.patch(
  path="/reminders/{list_id}",
  summary="Updates a reminder list's name",
  response_model=ReminderList
)
async def patch_list_id(
  list_id: int,
  reminder_list: NewReminderListName,
  storage: ReminderStorage = Depends(get_storage_for_api)
) -> ReminderList:
  """Updates a reminder list's name."""
  
  storage.update_list_name(list_id, reminder_list.name)
  return storage.get_list(list_id)

#-----------------------------------

@router.delete(
  path="/reminders/{list_id}",
  summary="Deletes a reminder list",
  response_model=dict
)
async def delete_list_id(
  list_id: int,
  storage: ReminderStorage = Depends(get_storage_for_api)
) -> Dict:
  """Deletes a reminder list by ID."""

  storage.delete_list(list_id)
  return dict()