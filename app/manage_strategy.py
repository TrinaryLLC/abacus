from typing import List
import inquirer
from inquirer.themes import GreenPassion
#from objects import Strategy

from utils.db import db
db=db()

# List all strategies to select from
def select_strategy_from_list():
    q = []
    # Get list of db.get_strategies() and populate inquirer choices
    q.append(inquirer.List('strategy', message='Select a strategy', choices=db.get_strategy_names()))
    answers = inquirer.prompt(q, theme=GreenPassion())
    print(answers)

# def create():
#     print('**** New Strategy ****')
#     q = []
#     for field in Strategy.model_fields.items():
#         if field[1].annotation == str:
#             if str(field[1].default) != 'PydanticUndefined':
#                 q.append(inquirer.Text(field[0], message=field[0], default=field[1].default))
#             else:
#                 q.append(inquirer.Text(field[0], message=field[0]))        
#         elif field[1].annotation == bool:
#             q.append(inquirer.Checkbox(field[0], message=field[0], choices=["yes", "no"], default=field[1].default))
#         elif field[1].annotation == CalcMethodology:
#             q.append(inquirer.List(field[0], message=field[0], choices=list(CalcMethodology)))
#         elif field[1].annotation == RebalMethodology:
#             q.append(inquirer.List(field[0], message=field[0], choices=list(RebalMethodology)))
#         elif field[1].annotation == List[ClassificationMethodology]:
#             q.append(inquirer.Checkbox(field[0], message=field[0], choices=list(ClassificationMethodology)))

#     answers = inquirer.prompt(q, theme=GreenPassion())
#     print(answers)
#     strategy = Strategy(**answers)
#     id = db.create_strategy(answers)
#     print(strategy)

if __name__ == '__main__':
    select_strategy_from_list()
    # create()
    