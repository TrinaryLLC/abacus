from datetime import datetime, date
from typing import Optional, List, Dict, Generator
from uuid import uuid4
import duckdb as ddb
from objects import Strategy, Methodology, MethodologyType

def get_db(db_loc: Optional[str] = None): 
    return db(db_loc) if db_loc else db()

class db:
    def __init__(self, db_loc: str = './database.duckdb'):
        self.db_loc = db_loc

    def get(self, query: str, params: Optional[List] = None) -> List[Dict]:
        with ddb.connect(self.db_loc) as con:
            if params:
                rows = con.execute(query, params).fetchall()
            else:
                rows = con.execute(query).fetchall()
            column_names = [desc[0] for desc in con.description]
        return [dict(zip(column_names, row)) for row in rows] if rows else []

    # TODO: DO NOT COMMIT IF INIT MODE
    def post(self, query: str, params: Optional[List] = None, init_mode: bool = False):
        with ddb.connect(self.db_loc) as con:
            con.execute(query, params)
            result = con.fetchone()
        return result[0] if result else None

    def put(self, query: str, params: Optional[List] = None, init_mode: bool = False) -> None:
        with ddb.connect(self.db_loc) as con:
            con.execute(query, params)
            con.commit()

    def delete(self, query: str, params: Optional[List] = None, init_mode: bool = False) -> None:
        with ddb.connect(self.db_loc) as con:
            con.execute(query, params)
            con.commit()

    def add_file_to_log(self, directory: str, file_name: str, file_size: int, file_type: str, file_status: str = 'NEW') -> None:
        self.put(
            "INSERT OR IGNORE INTO file_log (DIRECTORY, NAME, SIZE, TYPE, STATUS) VALUES (?, ?, ?, ?, ?)",
            (directory, file_name, file_size, file_type, file_status)
        )

    def get_files_ending_in_from_dir(self, directory: str, file_type: str) -> list[str]:
        loc = f"{directory}*.{file_type}"
        data = self.get("SELECT * FROM read_text( ? );", [loc])
        #[self.add_file_to_log(directory, file['filename'], file['size'], 'csv') for file in data]
        return [file['filename'] for file in data]
    
    def get_file_log(self, directory: str, file_type: str, file_status: str = None) -> list[str]:
        return self.get("SELECT * FROM file_log WHERE DIRECTORY = ? AND TYPE = ? AND STATUS = ?", [directory, file_type, file_status])

    def add_files_to_log_by_name(self, directory: str, file_names: list[str]) -> None:
        data = self.get("SELECT * FROM read_text( ? );", [directory])
        [self.add_file_to_log(directory, file['filename'], file['size'], 'csv') for file in data if file['filename'] in file_names]
    def update_file_log(self, directory:str, file_name:str, file_status:str, content:str = None, size:int = None, last_modified:datetime = None) -> None:
        self.put(
            "UPDATE file_log SET STATUS = ?, CONTENT = ?, SIZE = ?, LAST_MODIFIED = ? WHERE DIRECTORY = ? AND NAME = ?",
            [file_status, content, size, last_modified, directory, file_name]
        )

    def load_dir(self, directory: str, file_type: str) -> Generator[Dict, None, None]:
        # Compare files in directory to files in database        
        all_files = self.get_files_ending_in_from_dir(directory, file_type)
        processed_files = self.get_file_log(directory, file_type, 'PROCESSED')
        # Remove processed files from files list
        new_files = [file for file in all_files if file not in [x['NAME'] for x in processed_files]]     
        self.add_files_to_log_by_name(directory, new_files)
        ## TODO: Check for files that have been updated
        if file_type == 'csv':
            for file in new_files:
                query = ddb.execute(f"SELECT * FROM read_csv_auto(?)", [file])
                record_batch_reader = query.fetch_record_batch()
                self.update_file_log(directory, file, 'PROCESSED')
                try:
                    while True:
                        chunk = record_batch_reader.read_next_batch()
                        if len(chunk) == 0:
                            break
                        yield chunk.to_pylist()
                except StopIteration:
                    pass
        else:
            raise ValueError(f"File type {file_type} not supported")

    def convert_date(self, d: datetime) -> str:
        return d.strftime("%Y-%m-%d %H:%M:%S")

    def create_object(self, obj: object) -> str:
        self.put(
            "INSERT INTO object (id, type, start, thru, modified_by) VALUES (?, ?, ?, ?, ?)",
            (obj.id, obj.__class__.__name__, self.convert_date(obj.start), self.convert_date(obj.stop), 'Aaron')
        )
        for key, value in obj.dict().items():
            self.put(
                "INSERT INTO value (id, objid, key, value, start, thru, modified_by) VALUES (?, ?, ?, ?, ?, ?)",
                (uuid4().hex, obj.id, key, value, self.convert_date(obj.start), self.convert_date(obj.stop), 'Aaron')
            )
        return obj.id

    def create_relationship(self, objid1: str, objid2: str, rel_type: str) -> str:
        id = uuid4().hex
        self.put(
            "INSERT INTO relation (id, type, key1, key2, modified_by) VALUES (?, ?, ?, ?, ?)",
            (id, rel_type, objid1, objid2, 'Aaron')
        )
        return id

    # Strategy Methods
    def get_strategy_types(self) -> List[str]:
        return [x['NAME'] for x in self.get("SELECT DISTINCT NAME FROM strategy_types WHERE VALID_FROM <= current_date() AND VALID_TO >= current_date();")]

    def get_strategies(self) -> List[Dict]:
        strategies = self.get("SELECT * FROM view_strategy_identifiers WHERE NAME='name'")
        return strategies
    
    def get_strategy_names(self) -> List[str]:        
        strategies = self.get("SELECT * FROM view_strategy_identifiers WHERE NAME='name'")
        return [x['VALUE'] for x in strategies]

    def get_strategy_by_id(self, strategy_id: int) -> Dict:
        return self.get("SELECT * FROM strategy WHERE ID = ?", [strategy_id])

    def get_strategy_by_name(self, strategy_name: str, start: Optional[date] = None, stop: Optional[date] = None) -> Strategy:
        return self.get("SELECT * FROM strategy WHERE strategy_type = ?", [strategy_name])

    def get_strategy_identifier_type(self, name: str, create_if_not_exists: bool = True) -> Optional[int]:
        id_type = self.get("SELECT ID FROM strategy_identifier_types WHERE NAME = ?", [name.lower()])
        if id_type:
            return id_type[0]['ID']
        elif create_if_not_exists:
            return self.create_strategy_identifier_type(name)
        return None

    def create_strategy_identifier_type(self, name: str) -> int:
        return self.post("INSERT INTO strategy_identifier_types (NAME) VALUES (?) RETURNING ID", [name])

    # INSTRUMENT METHODS
    def get_instrument_type_id_by_name(self, name: str, valid_from: Optional[date] = None, valid_to: Optional[date] = None, init_mode: bool = False) -> int:
        id = self.get("SELECT ID FROM instrument_types WHERE NAME = ?", [name.lower()])
        if id:
            return id[0]['ID']
        elif init_mode:
            return self.create_instrument_type(name)
        return None

    def get_instruments_by_strategy_id(self, strategy_id: int) -> dict:
        return self.get("SELECT * FROM instruments WHERE TYPE_ID IN (SELECT ID FROM instrument_types WHERE ID IN (SELECT TYPE_ID FROM instrument_list_relationship WHERE LIST_ID IN (SELECT ID FROM instrument_list WHERE NAME IN (SELECT VALUE FROM view_strategy_identifiers WHERE ID = ?))))", [strategy_id])

    def create_instrument_type(self, name: str) -> int:
        type_id = self.post("INSERT INTO instrument_types (NAME) VALUES (?) RETURNING ID", [name.lower()])
        return type_id
    
    def create_instrument(self, instrument_type_id: int) -> int:
        instrument_id = self.post("INSERT INTO instruments (TYPE_ID) VALUES (?) RETURNING ID", [instrument_type_id])
        return instrument_id

    def get_all_classifications(self) -> dict:
        return self.get("SELECT * FROM instrument_classifications")
    
    def get_classifications_by_strategy_id(self, strategy_id: int) -> dict:
        return self.get("SELECT * FROM instrument_classifications WHERE INSTRUMENT_ID IN (SELECT ID FROM instruments WHERE TYPE_ID IN (SELECT ID FROM instrument_types WHERE ID IN (SELECT TYPE_ID FROM instrument_list_relationship WHERE LIST_ID IN (SELECT ID FROM instrument_list WHERE NAME IN (SELECT VALUE FROM view_strategy_identifiers WHERE ID = ?)))))", [strategy_id])

    def get_classifications_by_instrument_id(self, instrument_id: int) -> dict:
        return self.get("SELECT * FROM instrument_classifications WHERE INSTRUMENT_ID = ?", [instrument_id])
    
    def get_instrument_batch(self, offset: int = 0, limit: int = 100) -> dict:
        return self.get("SELECT * FROM view_instruments LIMIT ? OFFSET ?", [limit, offset])
    
    def get_all_instruments(self) -> dict:
        return self.get("SELECT * FROM view_instruments")

    def get_instrument_identifiers_by_instrument_id(self, instrument_id: int) -> dict:
        return self.get("SELECT * FROM instrument_identifier WHERE INSTRUMENT_ID = ?", [instrument_id])

    def get_instrument_id_by_identifier_id(self, identifier_id: int, instrument_type_id:int = None, valid_from: Optional[date] = None, valid_to: Optional[date] = None, init_mode: bool = False) -> int:
        id = self.get("SELECT INSTRUMENT_ID FROM instrument_identifier_relationship WHERE INSTRUMENT_IDENTIFIER_ID = ?", [identifier_id])
        if id:
            return id[0]['INSTRUMENT_ID']
        elif init_mode and instrument_type_id:
            instrument_id = self.create_instrument(instrument_type_id)
            self.create_instrument_identifier_relationship(instrument_id, identifier_id)
            return instrument_id
        return None
    
    # TODO: UPDATE QUERY
    def create_instrument_identifier_type(self, name: str, desc: str = None, valid_from: Optional[date] = None, valid_to: Optional[date] = None) -> int:
        return self.post(
                    f"""INSERT INTO instrument_identifier_types 
                    (NAME, DESCRIPTION, VALID_FROM, VALID_TO) 
                    VALUES (?, ?, ?, ?) RETURNING ID""", 
                    [name.lower(), desc, valid_from, valid_to]
                )
    
    #TODO: INCORPORATE VALID_FROM AND VALID_TO
    def get_instrument_identifier_type_id_by_name(self, type_name:str,  valid_from: Optional[date] = None, valid_to: Optional[date] = None, init_mode: bool = False) -> int:
        id = self.get("SELECT ID FROM instrument_identifier_types WHERE NAME = ?", [type_name.lower()])
        if id:
            return id[0]['ID']
        elif init_mode:
            return self.create_instrument_identifier_type(type_name, valid_from, valid_to)
        return None
    
    def instrument_identifier_type_exists(self, name: str) -> bool:
        return True if self.get("SELECT 1 FROM instrument_identifier_types WHERE NAME = ?", [name.lower()]) else False

    def get_instrument_by_id(self, instrument_id: int) -> dict:
        return self.get("SELECT * FROM instruments WHERE ID = ?", [instrument_id])

    #TODO: INCORPORATE VALID_FROM AND VALID_TO
    def get_instrument_identifier_id_by_value(self, value: str, type_id: int,  valid_from: Optional[date] = None, valid_to: Optional[date] = None, init_mode: bool = False) -> int:
        id = self.get("SELECT ID FROM instrument_identifier WHERE VALUE = ? AND TYPE_ID = ?", [value, type_id])
        if id:
            return id[0]['ID']
        elif init_mode:
            return self.create_instrument_identifier(type_id, value)
        return None
    
    # TODO: UPDATE QUERY
    def create_instrument_identifier(self, type_id: int, value: str, valid_from: Optional[date] = None, valid_to: Optional[date] = None) -> int:
        return self.post(
                    f"""INSERT INTO instrument_identifier 
                    (TYPE_ID, VALUE, VALID_FROM, VALID_TO) 
                    VALUES (?, ?, ?, ?) RETURNING ID""", 
                    [type_id, value, valid_from, valid_to]
                )
    
    def instrument_identifier_exists(self, value:str, type_id:int) -> bool:
        return True if self.get("SELECT 1 FROM instrument_identifier WHERE VALUE = ? AND TYPE_ID = ?", [value, type_id]) else False
    
    #TODO: INCORPORATE VALID_FROM AND VALID_TO
    def get_instrument_list_id_by_name(self, strategy_id: int, list_name: str, valid_from: Optional[date] = None, valid_to: Optional[date] = None, init_mode: bool = False) -> int:
        id = self.get("SELECT ID FROM instrument_list WHERE NAME = ?", [list_name])
        if id:
            return id[0]['ID']
        elif init_mode:
            return self.create_instrument_list(strategy_id, list_name)
        return None

    # TODO: UPDATE QUERY
    def create_instrument_list(self, strategy_id: int, list_name: str, desc: str = None, valid_from: Optional[date] = None, valid_to: Optional[date] = None) -> int:
        list_id = self.post(
                    f"""INSERT INTO instrument_list 
                    (NAME, DESCRIPTION,VALID_FROM, VALID_TO) 
                    VALUES (?, ?, ?, ?) RETURNING ID""", 
                    [list_name, desc, valid_from, valid_to]
                )
        return self.post(
                    f"""INSERT INTO instrument_list_strategy_relationship
                    (STRATEGY_ID, LIST_ID) 
                    VALUES (?, ?) RETURNING LIST_ID""", 
                    [strategy_id, list_id]
                )        

    # TODO: UPDATE QUERY
    def add_instrument_to_instrument_list(self, instrument_id:int, instrument_list_id:int, valid_from: Optional[date] = None, valid_to: Optional[date] = None) -> int:
        return self.post(
                    f"""INSERT INTO instrument_list_relationship 
                    (INSTRUMENT_ID, LIST_ID, VALID_FROM, VALID_TO) 
                    VALUES (?, ?, ?, ?) RETURNING INSTRUMENT_ID, LIST_ID""", 
                    [instrument_id, instrument_list_id, valid_from, valid_to]                    
                )
    
    def create_instrument_identifier_relationship(self, instrument_id:int, instrument_identifier_id:int, valid_from: Optional[date] = None, valid_to: Optional[date] = None) -> int:
        return self.post(
                    f"""INSERT INTO instrument_identifier_relationship 
                    (INSTRUMENT_ID, INSTRUMENT_IDENTIFIER_ID, VALID_FROM, VALID_TO) 
                    VALUES (?, ?, ?, ?) RETURNING INSTRUMENT_ID, INSTRUMENT_IDENTIFIER_ID""", 
                    [instrument_id, instrument_identifier_id, valid_from, valid_to]                    
                )
    
    # STRATEGY METHODS

    def create_strategy_identifier(self, type: str, value: str) -> int:
        id_type = self.get_strategy_identifier_type(type)        
        return self.post("INSERT INTO strategy_identifier (TYPE_ID, VALUE) VALUES (?, ?) RETURNING ID", [id_type, value])
            
    def create_strategy(self, strat: Optional[Dict] = None, start: Optional[date] = None, stop: Optional[date] = None) -> int:
        strategy_id = self.post("INSERT INTO strategy (VALID_FROM) VALUES(current_date()) RETURNING ID")
        if strat:
            id_keys = [self.create_strategy_identifier(key, value) for key, value in strat.items()]
            for id_key in id_keys:
                self.post("INSERT INTO strategy_identifier_relationship (STRATEGY_ID, STRATEGY_IDENTIFIER_ID, TYPE) VALUES (?, ?, 'REFERENCES')", [strategy_id, id_key])
        return strategy_id

    def get_strategy_identifiers_by_strategy_id(self, strategy_id: int) -> dict:
        return self.get("SELECT VALUE FROM view_strategy_identifiers WHERE ID = ?", [strategy_id])
        #return [identifier['VALUE'] for identifier in identifiers]

    def get_strategy_name_by_id(self, strategy_id: int) -> str:
        name = self.get("SELECT VALUE FROM view_strategy_identifiers WHERE NAME='name' and ID = ?", [strategy_id])
        return name[0]['VALUE'] if name else None
    
    def get_strategy_id_by_name(self, strategy_name: str, start: Optional[date] = None, stop: Optional[date] = None):
        return self.get("SELECT STRATEGY_ID FROM strategy_identifier_relationship WHERE STRATEGY_IDENTIFIER_ID = (SELECT ID FROM strategy_identifier WHERE VALUE = ? and TYPE_ID = 1000000)", [strategy_name])

    # Methodology Methods
    def create_methodology(self, type_id: int, meth: Dict) -> int:
        return self.post(
            "INSERT INTO methodology (TYPE_ID, FUNCTION_REPO, FUNCTION_BRANCH, FUNCTION_FILE, FUNCTION_NAME, FUNCTION_DESCRIPTION) VALUES (?, ?, ?, ?, ?, ?) RETURNING ID",
            [type_id, meth['function_repo'], meth['function_branch'], meth['function_file'], meth['function_name'], meth['function_description']]
        )

    def get_all_methodologies(self) -> dict:
        return self.get("SELECT * FROM methodology")
    
    def get_methodology_id_by_name(self, name: str) -> Optional[int]:
        id_type = self.get("SELECT ID FROM methodology WHERE FUNCTION_NAME = ?", [name.lower()])
        return id_type[0]['ID'] if id_type else None
    
    def get_methodology_types(self) -> List[str]:
        m_types = self.get("SELECT DISTINCT NAME FROM methodology_types WHERE VALID_FROM <= current_date() AND VALID_TO >= current_date();")
        return [type['NAME'] for type in m_types]
        
    def get_methodology_by_type_id(self, type_id: int) -> List[str]:
        meths = self.get("SELECT FUNCTION_NAME FROM methodology WHERE TYPE_ID = ?", [type_id])
        return [meth['FUNCTION_NAME'] for meth in meths]
    
    # TODO: UPDATE FOR INIT MODE
    def get_methodology_by_type_name(self, type: str, init_mode: bool = False) -> List[str]:
        meths = self.get("SELECT FUNCTION_NAME FROM methodology WHERE TYPE_ID = (SELECT ID FROM methodology_types WHERE NAME = ?)", [type.lower()])
        return [meth['FUNCTION_NAME'] for meth in meths]
    
    def get_methodology_type_id_by_type_name(self, name: str) -> Optional[int]:
        id_type = self.get("SELECT ID FROM methodology_types WHERE NAME = ?", [name.lower()])
        return id_type[0]['ID'] if id_type else None

    def create_methodology_type(self, name: str) -> int:
        type_id = self.post("INSERT INTO methodology_types (NAME) VALUES (?) RETURNING ID", [name.lower()])
        return type_id[0]['ID']
    
    def get_methodologies_by_strategy_id(self, strategy_id: int) -> dict:
        return self.get("SELECT * FROM methodology WHERE TYPE_ID IN (SELECT ID FROM methodology_types WHERE ID IN (SELECT TYPE_ID FROM strategy_methodology_relationship WHERE STRATEGY_ID = ?))", [strategy_id])
    
    def get_methodology_types_by_strategy_id(self, strategy_id: int) -> List[str]:
        meths = self.get("SELECT DISTINCT NAME FROM methodology_types WHERE VALID_FROM <= current_date() AND VALID_TO >= current_date() AND ID IN (SELECT TYPE_ID FROM strategy_methodology_relationship WHERE STRATEGY_ID = ?)", [strategy_id])
        return [type['NAME'] for type in meths]
    
    def get_table_schema(db_loc, table_name):
        with ddb.connect(db_loc) as con:
            # Query the table schema
            schema_info = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()

            if not schema_info:
                raise ValueError(f"Table '{table_name}' does not exist.")

            # Start building the CREATE TABLE statement
            create_table_stmt = f"CREATE TABLE {table_name} (\n"

            # Loop through the schema information to build column definitions
            columns = []
            for column in schema_info:
                column_name = column[1]
                column_type = column[2]
                not_null = " NOT NULL" if column[3] else ""
                default_value = f" DEFAULT {column[4]}" if column[4] else ""
                primary_key = " PRIMARY KEY" if column[5] else ""

                columns.append(f"    {column_name} {column_type}{not_null}{default_value}{primary_key}")

            # Join all column definitions and close the statement
            create_table_stmt += ",\n".join(columns) + "\n);"

        return create_table_stmt
    
    #TODO: INCORPORATE VALID_FROM AND VALID_TO
    # INSTRUMENT METHODS
    def add_instrument_to_list(self, strategy_id: int, list_name: str, instrument_identifier: str, instrument_identifier_type: str, instrument_type:str='EQUITY', valid_from: Optional[date] = None, valid_to: Optional[date] = None, init_mode: bool = False) -> None:
        #Get id of instrument identifier type
        instrument_identifier_type_id = self.get_instrument_identifier_type_id_by_name(instrument_identifier_type, valid_from=valid_from, valid_to=valid_to, init_mode=init_mode)
        #Get id of instrument identifier
        instrument_identifier_id = self.get_instrument_identifier_id_by_value(instrument_identifier, instrument_identifier_type_id, valid_from=valid_from, valid_to=valid_to, init_mode=init_mode)                                
        #Get id of instrument type
        instrument_type_id = self.get_instrument_type_id_by_name(instrument_type, valid_from=valid_from, valid_to=valid_to, init_mode=init_mode)        
        
        #Get id of instrument using instrument identifier and type. Set if not exists        
        instrument_id = self.get_instrument_id_by_identifier_id(instrument_identifier_id,instrument_type_id, valid_from=valid_from, valid_to=valid_to, init_mode=init_mode)
        
        #Get id of instrument list
        instrument_list_id = self.get_instrument_list_id_by_name(strategy_id,list_name, valid_from, valid_to, init_mode=init_mode)        
        return self.add_instrument_to_instrument_list(instrument_id, instrument_list_id, valid_from, valid_to)