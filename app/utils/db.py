from datetime import datetime, date
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
    def get(self, query:str):
        with ddb.connect(self.db_loc) as con:
            result = con.execute(query).fetchall()
        return None if all(len(arr) == 0 for arr in result.values()) else result                
    
    def post(self, query):
        with ddb.connect(self.db_loc) as con:
            pass
    
    def put(self, sql:str) -> None:
        with ddb.connect(self.db_loc) as con:
            con.execute(sql)
            con.commit()

    def delete(self, query):
        ...
    
    # STRATEGY ####################################################################################################
    # Get strategy types
    def get_strategy_types(self) -> list[str]:
        return self.get("SELECT DISTINCT NAME FROM strategy_types WHERE VALID_FROM<=current_date() AND VALID_TO>=current_date();")

    # Get all strategies
    def get_strategy_names(self) -> list[str]:        
        strategies = self.get(f"SELECT * FROM view_strategy_identifiers WHERE NAME='name'")        
        return list(strategies.get('VALUE', []))
        #return [Strategy(**row) for row in strategies]
    
    def get_strategy_by_name(self, strategy_name:str, tart:date=None, stop:date=None) -> Strategy:
        return self.execute("SELECT * FROM strategy WHERE strategy_type = ?;", (strategy_name,))
    
    def create_strategy(self, strat:dict, tart:date=None, stop:date=None):
        return self.execute("SELECT * FROM strategy WHERE strategy_type = ?;", (strat))

    # Get strategy by id
    def get_strategy_by_id(self, strategy_id):
        return self.execute("SELECT * FROM strategy WHERE id = ?;", (strategy_id,))
    
    def get_strategy_by_name(self, strategy_name:str, tart:date=None, stop:date=None):
        return self.get(f"SELECT STRATEGY_ID FROM strategy_identifier_relationship WHERE STRATEGY_IDENTIFIER_ID = (SELECT ID FROM strategy_identifier WHERE VALUE='{strategy_name}' and TYPE_ID=1000000)")
    # METHODLOGY ####################################################################################################
    # Get methodology types
    def get_methodology_types(self) -> list[str]:
        m_types = self.get("SELECT DISTINCT NAME FROM methodology_types WHERE VALID_FROM<=current_date() AND VALID_TO>=current_date();")
        return list(m_types.get('VALUE', []))
    
    ##################################################################################################################    
    def dict_to_sql(self, table_name, conditions:dict=None) -> str:        
        if not conditions:
            return f"SELECT * FROM {table_name}"

        where_clause = " AND ".join([f"{key}='{value}'" if isinstance(value, str) 
                                    else f"{key}={value}" for key, value in conditions.items()])
        return f"SELECT * FROM {table_name} WHERE {where_clause}"

    def get(self, query:str):
        with ddb.connect(self.db_loc) as con:
            result = con.execute(query).fetchnumpy()
        return None if all(len(arr) == 0 for arr in result.values()) else result                
    
    def post(self, query):
        with ddb.connect(self.db_loc) as con:
            pass
    
    def put(self, sql:str) -> None:
        with ddb.connect(self.db_loc) as con:
            con.execute(sql)
            con.commit()

    def delete(self, query):
        ...

    def read_dir(self, directory:str, file_type:str):
        query = ddb.execute(f"SELECT * FROM '{directory}*.{file_type}';")
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