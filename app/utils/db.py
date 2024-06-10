from datetime import datetime, date
from typing import Optional, List
from uuid import uuid4
import duckdb as ddb
from objects import Strategy, Methodology, MethodologyType

def get_db(db_loc=None): 
    if db_loc:
        return db(db_loc)
    else:
        return db()
    
class db:
    def __init__(self, db_loc='./database.duckdb'):        
        self.db_loc = db_loc
        #self.table_names = self.get('SHOW ALL TABLES;')['name']
        # self.put("SET timezone='UTC';") # Set TZ        
        # if "object" not in self.table_names:
        #     self.put("CREATE TABLE object (id VARCHAR(255) PRIMARY KEY, type VARCHAR(255), start TIMESTAMP DEFAULT CURRENT_TIMESTAMP, thru TIMESTAMP DEFAULT TIMESTAMP '3000-12-31 12:12:12', genesis TIMESTAMP DEFAULT CURRENT_TIMESTAMP, modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP, modified_by VARCHAR(255));") #start TIMESTAMP DEFAULT CURRENT_TIMESTAMP, thru TIMESTAMP DEFAULT TIMESTAMP '3000-12-31 12:12:12', genesis TIMESTAMP DEFAULT CURRENT_TIMESTAMP, modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        # if "relation" not in self.table_names:
        #     self.put("CREATE TABLE relation (id VARCHAR(255) PRIMARY KEY, type VARCHAR(255), key1 VARCHAR(255), key2 VARCHAR(255), start TIMESTAMP DEFAULT CURRENT_TIMESTAMP, thru TIMESTAMP DEFAULT TIMESTAMP '3000-12-31 12:12:12', genesis TIMESTAMP DEFAULT CURRENT_TIMESTAMP, modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP, modified_by VARCHAR(255));")
        # if "value" not in self.table_names:
        #     self.put("CREATE TABLE value (id VARCHAR(255) PRIMARY KEY, objid VARCHAR(255) REFERENCES object(id), key VARCHAR(255), value VARCHAR(255), start TIMESTAMP DEFAULT CURRENT_TIMESTAMP, thru TIMESTAMP DEFAULT TIMESTAMP '3000-12-31 12:12:12', genesis TIMESTAMP DEFAULT CURRENT_TIMESTAMP, modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP, modified_by VARCHAR(255));")
        # if "log" not in self.table_names:
        #     self.put("CREATE TABLE log (user VARCHAR(255), action VARCHAR(255), genesis TIMESTAMP DEFAULT CURRENT_TIMESTAMP, modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP, modified_by VARCHAR(255));")
        # self.put("DELETE FROM object WHERE id not in (SELECT objid FROM value);") # Delete out orphaned objects
        # self.put("DELETE FROM value WHERE objid not in (SELECT id FROM object);") # Delete out orphaned values
    """
        Creates a SQL query from a dictionary of conditions.
        :param table_name: Name of the table to query.
        :param conditions: A dictionary where keys are column names and values are the conditions.
        :return: A SQL query string.
    """
    # def get(self, query:str):
    #     with ddb.connect(self.db_loc) as con:
    #         result = con.execute(query).fetchall()
    #     return None if all(len(arr) == 0 for arr in result.values()) else result                
    
    # def post(self, query:str):
    #     with ddb.connect(self.db_loc) as con:
    #         result = con.execute(query)
    #         result = con.commit().fetchone()
    #     return None if all(len(arr) == 0 for arr in result.values()) else result
    
    # def put(self, sql:str) -> None:
    #     with ddb.connect(self.db_loc) as con:
    #         con.execute(sql)
    #         con.commit()

    # def delete(self, query):
    #     ...
    
    # STRATEGY ####################################################################################################
    # Get strategy types
    def get_strategy_types(self) -> list[str]:
        return self.get("SELECT DISTINCT NAME FROM strategy_types WHERE VALID_FROM<=current_date() AND VALID_TO>=current_date();")

    # Get all strategies
    def get_strategy_names(self) -> list[str]:        
        strategies = self.get("SELECT * FROM view_strategy_identifiers WHERE NAME='name'")        
        return [x['VALUE'] for x in strategies]
        #return [Strategy(**row) for row in strategies]
    
    def get_strategy_by_name(self, strategy_name:str, tart:date=None, stop:date=None) -> Strategy:
        return self.execute("SELECT * FROM strategy WHERE strategy_type = ?;", (strategy_name,))
    
    def get_strategy_identifier_type(self, name:str, create_if_not_exists:bool=True) -> int:
        id_type = self.get("SELECT ID FROM strategy_identifier_types WHERE NAME=?", (name.lower(),))
        if id_type:
            return id_type[0]['ID'] if id_type else None
        elif create_if_not_exists:
            return self.create_strategy_identifier_type(name)
        else:
            return None
    
    def create_strategy_identifier_type(self, name:str) -> int:
        id_type = self.post("INSERT INTO strategy_identifier_types (NAME) VALUES (?) RETURNING ID", (name,))
        return id_type['ID'][0]
    
    def create_strategy_identifier(self, type:str, value:str) -> int:
        id_type = self.get_strategy_identifier_type(type)        
        return self.post("INSERT INTO strategy_identifier (TYPE_ID, VALUE) VALUES (?, ?) RETURNING ID", (id_type, value))
            
    def create_strategy(self, strat:dict=None, start:date=None, stop:date=None) -> int:
        strategy_id = self.post("INSERT INTO strategies (VALID_FROM) VALUES(current_date()) RETURNING ID")                
        if strat:
            id_keys = [self.create_strategy_identifier(key, value) for key, value in strat.items()]
            [self.post("INSERT INTO strategy_identifier_relationship (STRATEGY_ID, STRATEGY_IDENTIFIER_ID, TYPE) VALUES (?, ?, 'REFERENCES')", (strategy_id, id_key)) for id_key in id_keys]
        return strategy_id

    def get_strategy_name_by_id(self, strategy_id):
        name = self.get("SELECT VALUE FROM view_strategy_identifiers WHERE NAME='name' and ID=?", (strategy_id,))
        return name[0]['VALUE']
    
    def get_strategy_id_by_name(self, strategy_name:str, tart:date=None, stop:date=None):
        return self.get(f"SELECT STRATEGY_ID FROM strategy_identifier_relationship WHERE STRATEGY_IDENTIFIER_ID = (SELECT ID FROM strategy_identifier WHERE VALUE='{strategy_name}' and TYPE_ID=1000000)")
    # METHODLOGY ####################################################################################################
    # Get methodology types
    def create_methodology(self, type_id:int, meth:dict):
        id = self.post("INSERT INTO methodologies (TYPE_ID, FUNCTION_REPO, FUNCTION_BRANCH, FUNCTION_FILE, FUNCTION_NAME, FUNCTION_DESCRIPTION) VALUES (?, ?, ?, ?, ?, ?) RETURNING ID", (type_id, meth['function_repo'], meth['function_branch'], meth['function_file'], meth['function_name'], meth['function_description']))
        return id
    
    def get_methodology_id_by_name(self, name:str) -> int:
        id_type = self.get("SELECT ID FROM methodologies WHERE FUNCTION_NAME=?", (name.lower(),))
        return id_type[0]['ID'] if id_type else None
    
    def get_methodology_types(self) -> list[str]:
        m_types = self.get("SELECT DISTINCT NAME FROM methodology_types WHERE VALID_FROM<=current_date() AND VALID_TO>=current_date();")
        return [type['NAME'] for type in m_types]
    
    def get_methodology_by_type_id(self, type_id:int) -> list[dict]:
        meths = self.get("SELECT FUNCTION_NAME FROM methodologies WHERE TYPE_ID = ? ", (type_id,))
        return [meth['FUNCTION_NAME'] for meth in meths]
    
    def get_methodology_by_type_name(self, type:str) -> list[dict]:
        meths = self.get("SELECT FUNCTION_NAME FROM methodologies WHERE TYPE_ID = (SELECT ID FROM methodology_types WHERE NAME=?)", (type.lower(),))
        return [meth['FUNCTION_NAME'] for meth in meths]
    
    def get_methodology_type_id_by_type_name(self, name:str) -> int:
        id_type = self.get("SELECT ID FROM methodology_types WHERE NAME=?", (name.lower(),))
        return id_type[0]['ID'] if id_type else None

    def create_methodology_type(self, name:str) -> int:
        id_type = self.post("INSERT INTO methodology_types (NAME) VALUES (?) RETURNING ID", (name.lower(),))
        return id_type
    ##################################################################################################################    
    def dict_to_sql(self, table_name, conditions:dict=None) -> str:        
        if not conditions:
            return "SELECT * FROM ?", (table_name,)

        where_clause = " AND ".join([f"{key}='{value}'" if isinstance(value, str) 
                                    else f"{key}={value}" for key, value in conditions.items()])
        return "SELECT * FROM ? WHERE ?", (table_name, where_clause)

    def get(self, query:str, params: Optional[List] = None) -> list[dict]:
        with ddb.connect(self.db_loc) as con:
            if params:
                rows = con.execute(query, params).fetchall()
            else:
                rows = con.execute(query).fetchall()
            column_names = [desc[0] for desc in con.description]
        return None if len(rows) == 0 else [dict(zip(column_names, row)) for row in rows]                
    
    def post(self, query:str, params: Optional[List] = None):
        with ddb.connect(self.db_loc) as con:
            con.execute(query, params)
            result = con.fetchone()
        return result[0] if result else None
    
    def put(self, sql:str) -> None:
        with ddb.connect(self.db_loc) as con:
            con.execute(sql)
            con.commit()

    def delete(self, query):
        ...

    def read_dir(self, directory:str, file_type:str):
        query = ddb.execute("SELECT * FROM '?*.?';", (directory, file_type))
        record_batch_reader = query.fetch_record_batch()
        try:
            while True:
                chunk = record_batch_reader.read_next_batch()
                if len(chunk) == 0:
                    break
                yield chunk.to_pylist()
        except StopIteration:
            pass
    
    def convert_date(self, d:datetime) -> str:
            return d.strftime("%Y-%m-%d %H:%M:%S")
    
    def create_object(self, obj:object) -> str:
        self.put(f"INSERT INTO object (id, type, start, thru, modified_by) VALUES('{obj.id}', '{obj.__class__.__name__}','{self.convert_date(obj.start)}','{self.convert_date(obj.stop)}','Aaron')")
        for key, value in obj.dict().items():
            self.put(f"INSERT INTO value (id, objid, key, value, start,thru, modified_by) VALUES('{uuid4().hex}', '{obj.id}', '{key}', '{value}','{self.convert_date(obj.start)}','{self.convert_date(obj.stop)}','Aaron')")
        return obj.id
    
    def create_relationship(self, objid1:str, objeid2:str, type:str):
        id = uuid4().hex
        self.put(f"INSERT INTO relation (id, type, key1, key2, modified_by) VALUES('{id}', '{type}', '{objid1}', '{objeid2}', 'Aaron')")
        return id