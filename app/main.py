import os
from dotenv import load_dotenv
from utils.db import db
from manager import select_strategy_from_list

load_dotenv()
db = db()

def main():
    # Read in the csv files from source directory. 
    
    #TODO: ADD FILE TRACKING AS FILES DO NOT NEED TO BE PROCESSED TWICE.
         # ONLY READ IN NEW FILES.
    data = db.load_dir(os.getenv('file_loc'), 'csv')
    # Select strategy id from list or hard-code
    strategy_id = 500000000    
    # Initialization. Add instrument to list and create list and instrument if not exists
    # Need to speed up this process
    instruments = [{db.add_instrument_to_list(strategy_id, list_name = x['DATE'], instrument_identifier = x['TICKER'], instrument_type = 'EQUITY', instrument_identifier_type='TICKER', init_mode=True) for x in row} for row in data]
    print(instruments[0])

if __name__ == '__main__':
    main()
