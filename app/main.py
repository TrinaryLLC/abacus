import os
from dotenv import load_dotenv
from utils.db import db

load_dotenv()
db = db()

def main():    
    # Select strategy id from list or hard-code
    strategy_id = 500000000    
    # Read in the csv files from source directory. 
    data = db.load_dir(os.getenv('file_loc'), 'csv')
    [{db.add_instrument_to_list(strategy_id, list_name = x['DATE'], instrument_identifier = x['TICKER'], instrument_type = 'EQUITY', instrument_identifier_type='TICKER', init_mode=True) for x in row} for row in data]
    # TODO: Generate buys and sells based on instrument lists    

if __name__ == '__main__':
    main()

