from typing import List, Optional, Dict
import inquirer
from inquirer.themes import GreenPassion
from objects import Strategy, MethodologyType, Methodology
from utils.db import db

db = db()

def create_methodology_type() -> int:
    questions = [inquirer.Text('name', message="Please enter a name for the new methodology type")]
    answers = inquirer.prompt(questions, theme=GreenPassion())
    return db.create_methodology_type(answers['name'])

def select_methodology_type(strategy_id: Optional[int] = None) -> int:
    choices = db.get_methodology_types()
    if strategy_id:
        existing_meths = db.get_methodology_types_by_strategy_id(strategy_id)
        choices = [meth for meth in choices if meth not in existing_meths]
        pass
    choices.append('**New**')
    
    questions = [inquirer.List('methodology_type', message='Select a methodology type', choices=choices)]
    answers = inquirer.prompt(questions, theme=GreenPassion())
    
    if answers['methodology_type'] == '**New**':
        return create_methodology_type()
    else:
        return db.get_methodology_type_id_by_type_name(answers['methodology_type'])

def create_methodology(type_id: Optional[int] = None) -> int:
    if not type_id:
        type_id = select_methodology_type()
    
    questions = []
    for field_name, field in Methodology.model_fields.items():
        if field.annotation == str:
            if str(field.default) != 'PydanticUndefined':
                questions.append(inquirer.Text(field_name, message=f"Methodology {field_name}", default=field.default))
            else:
                questions.append(inquirer.Text(field_name, message=f"Methodology {field_name}"))
        elif field.annotation == bool:
            questions.append(inquirer.Checkbox(field_name, message=f"Methodology {field_name}", choices=["yes", "no"], default=field.default))
    
    answers = inquirer.prompt(questions, theme=GreenPassion())
    return db.create_methodology(type_id, answers)

def select_methodology(strategy_id: Optional[int] = None) -> None:
    meth_type = select_methodology_type(strategy_id)
    choices = db.get_methodology_by_type_id(meth_type)
    choices.append('**New**')
    
    questions = [inquirer.List('methodology_name', message='Select a methodology', choices=choices)]
    answers = inquirer.prompt(questions, theme=GreenPassion())
    
    if answers['methodology_name'] == '**New**':
        create_methodology(meth_type)
    else:
        db.get_methodology_id_by_name(answers['methodology_name'])
    
    # TODO: Add methodology to strategy
    print(answers, meth_type)

def create_strategy() -> int:
    questions = []
    for field_name, field in Strategy.model_fields.items():
        if field.annotation == str:
            if str(field.default) != 'PydanticUndefined':
                questions.append(inquirer.Text(field_name, message=f"Strategy {field_name}", default=field.default))
            else:
                questions.append(inquirer.Text(field_name, message=f"Strategy {field_name}"))
        elif field.annotation == bool:
            questions.append(inquirer.Checkbox(field_name, message=f"Strategy {field_name}", choices=["yes", "no"], default=field.default))
    
    answers = inquirer.prompt(questions, theme=GreenPassion())
    strategy_id = db.create_strategy(answers)
    
    questions = [inquirer.List('add_methodology', message='Add Methodology Now?', choices=["yes", "no"])]
    answers = inquirer.prompt(questions, theme=GreenPassion())
    
    while answers['add_methodology'] == 'yes':
        select_methodology(strategy_id=strategy_id)
        questions = [inquirer.List('add_methodology', message='Add another Methodology?', choices=["yes", "no"])]
        answers = inquirer.prompt(questions, theme=GreenPassion())
    
    return strategy_id

def select_strategy_from_list() -> None:
    choices = db.get_strategy_names()
    choices.append('**New**')
    
    questions = [inquirer.List('strategy_name', message='Select a strategy', choices=choices)]
    answers = inquirer.prompt(questions, theme=GreenPassion())
    
    if answers['strategy_name'] == '**New**':
        strategy_id = create_strategy()
        strategy_actions(strategy_id=strategy_id)
    else:
        strategy_name = answers['strategy_name']
        strategy_actions(strategy_name=strategy_name)
    
    

def strategy_actions(strategy_id: Optional[int] = None, strategy_name: Optional[str] = None) -> None:
    actions = ['View','Run', 'Clone', 'Update', 'Delete', 'Return']
    if not strategy_id and not strategy_name:
        strategy_id = select_strategy_from_list()
    if strategy_name:    
        questions = [inquirer.List('action', message=f'{strategy_name}', choices=actions)]
    else:
        questions = [inquirer.List('action', message=f'{db.get_strategy_name_by_id(strategy_id)}', choices=actions)]
    answers = inquirer.prompt(questions, theme=GreenPassion())
    
    action = answers['action']
    if action == 'View':
        print(f"Viewing strategy {db.get_strategy_name_by_id(strategy_id)}")
    elif action == 'Run':
        print(f"Running strategy {db.get_strategy_name_by_id(strategy_id)}")
    elif action == 'Clone':
        print(f"Cloning strategy {db.get_strategy_name_by_id(strategy_id)}")
    elif action == 'Update':
        print(f"Updating strategy {db.get_strategy_name_by_id(strategy_id)}")
    elif action == 'Delete':
        print(f"Deleting strategy {db.get_strategy_name_by_id(strategy_id)}")
    elif action == 'Return':
        print(f"Returning to main menu")
        return
    else:
        print(f"Unknown action {action}")

def methodology_actions(methodology_id: Optional[int] = None) -> None:
    actions = ['View', 'Run', 'Update', 'Delete', 'Return']
    if not methodology_id:
        methodology_id = select_methodology()
    
    questions = [inquirer.List('action', message=f'{db.get_methodology_name_by_id(methodology_id)}', choices=actions)]
    answers = inquirer.prompt(questions, theme=GreenPassion())
    
    action = answers['action']
    if action == 'View':
        print(f"Viewing methodology {db.get_methodology_name_by_id(methodology_id)}")
    elif action == 'Run':
        print(f"Running methodology {db.get_methodology_name_by_id(methodology_id)}")
    elif action == 'Update':
        print(f"Updating methodology {db.get_methodology_name_by_id(methodology_id)}")
    elif action == 'Delete':
        print(f"Deleting methodology {db.get_methodology_name_by_id(methodology_id)}")
    elif action == 'Return':
        print(f"Returning to main menu")
        return
    else:
        print(f"Unknown action {action}")

def main() -> None:
    init_options = ['Manage Strategy', 'Manage Methodology', 'Manage Instrument', 'Quit']
    questions = [inquirer.List('SELECT_OBJECT', message='What would you like to do?', choices=init_options)]
    answers = inquirer.prompt(questions, theme=GreenPassion())
    
    selection = answers['SELECT_OBJECT']
    if selection == 'Manage Strategy':
        select_strategy_from_list()
    elif selection == 'Manage Methodology':
        select_methodology()
    elif selection == 'Manage Instrument':
        # Implement this if needed
        pass
    elif selection == 'Quit':
        print("Quitting")
        exit(0)

if __name__ == '__main__':
    while True:
        main()
