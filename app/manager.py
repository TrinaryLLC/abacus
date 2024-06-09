from typing import List, Tuple
import inquirer
from inquirer.themes import GreenPassion
from objects import Strategy, MethodologyType, Methodology

from utils.db import db
db=db()

def create_methodology_type():
    q = []
    q.append(inquirer.Text('name', message=f"Please enter a name for the new methodology type"))
    answers = inquirer.prompt(q, theme=GreenPassion())
    return db.create_methodology_type(answers['name'])

def select_methodology_type(strategy_id=None):
    q = []
    choices = db.get_methodology_types()
    if strategy_id:
        ... # TODO: Filter exsiting methodologies
    choices.append('**New**')
    q.append(inquirer.List('methodology_type', message='Select a methodology type', choices=choices))    
    answers = inquirer.prompt(q, theme=GreenPassion())
    if answers['methodology_type'] == '**New**':
        return create_methodology_type()
    else:
        return db.get_methodology_type_id_by_type_name(answers['methodology_type'])

def create_methodology(type_id=None):
    q = []                
    if not type_id:
        type_id = select_methodology_type()
    for field in Methodology.model_fields.items():
        if field[1].annotation == str:
            if str(field[1].default) != 'PydanticUndefined':
                q.append(inquirer.Text(field[0], message=f"Methodology {field[0]}", default=field[1].default))
            else:
                q.append(inquirer.Text(field[0], message=f"Methodology {field[0]}"))
        elif field[1].annotation == bool:
            q.append(inquirer.Checkbox(field[0], message=f"Methodology {field[0]}", choices=["yes", "no"], default=field[1].default))            
    answers = inquirer.prompt(q, theme=GreenPassion())
    return db.create_methodology(type_id, answers)

def select_methodology(strategy_id=None):
    q = []                
    meth_type = select_methodology_type(strategy_id)            
    choices = db.get_methodology_by_type_id(meth_type)
    choices.append('**New**')
    q.append(inquirer.List('methodology_name', message='Select a methodology', choices=choices))
    answers = inquirer.prompt(q, theme=GreenPassion())
    if answers['methodology'] == '**New**':
        create_methodology(meth_type)
    else:
        db.get_methodology_id_by_name(answers['methodology'])
    # TODO : Add methodology to strategy
    print(answers, meth_type)

def create_strategy():
    q = []
    for field in Strategy.model_fields.items():
        if field[1].annotation == str:
            if str(field[1].default) != 'PydanticUndefined':
                q.append(inquirer.Text(field[0], message=f"Strategy {field[0]}", default=field[1].default))
            else:
                q.append(inquirer.Text(field[0], message=f"Strategy {field[0]}"))
        elif field[1].annotation == bool:
            q.append(inquirer.Checkbox(field[0], message=f"Strategy {field[0]}", choices=["yes", "no"], default=field[1].default))        
    answers = inquirer.prompt(q, theme=GreenPassion())
    strategy_id = db.create_strategy(answers)
    q=[]
    q.append(inquirer.List('add_methodology', message='Add Methodology Now?', choices=["yes", "no"]))        
    answers = inquirer.prompt(q, theme=GreenPassion())
    while answers['add_methodology'] == 'yes':
        q = []
        select_methodology(strategy_id=strategy_id)
        q.append(inquirer.List('add_methodology', message='Add another Methodology?', choices=["yes", "no"]))        
        answers = inquirer.prompt(q, theme=GreenPassion())

# List all strategies to select from
def select_strategy_from_list():
    q = []
    choices = db.get_strategy_names()
    choices.append('**New**')
    q.append(inquirer.List('strategy_name', message='Select a strategy', choices=choices))
    answers = inquirer.prompt(q, theme=GreenPassion())
    if answers['strategy_name'] == '**New**':
        create_strategy()
    else:
        db.get_strategy_by_name(answers['strategy_name'])

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
    q = []
    init_options = ['Manage Strategies', 'Manage Methodologies', 'Manage Instruments']
    q.append(inquirer.List('SELECT_OBJECT', message='What would you like to do?', choices=init_options))        
    answers = inquirer.prompt(q, theme=GreenPassion())
    if answers['SELECT_OBJECT'] == 'Manage Strategies':
        select_strategy_from_list()
    # create()
    