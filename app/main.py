import os
from dotenv import load_dotenv
from utils.db import db
from manager import select_strategy_from_list

load_dotenv()
db = db()

def main():
    # Read in the csv files from source directory
    data = db.read_dir(os.getenv('file_loc'), 'csv')
    # Select strategy id from list or hard-code
    strategy_id = 500000000
    # Initialization. Add instrument to list and create list and instrument if not exists
    # {strategy.add_to_basket(x) for x in data}
    instruments = [{db.add_instrument_to_list(strategy_id, list_name = x['DATE'], instrument_identifier = x['TICKER'], instrument_identifier_type='TICKER', init_mode=True) for x in row} for row in data]
    print(instruments)

if __name__ == '__main__':
    main()
