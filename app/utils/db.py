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

    def post(self, query: str, params: Optional[List] = None):
        with ddb.connect(self.db_loc) as con:
            con.execute(query, params)
            result = con.fetchone()
        return result[0] if result else None

    def put(self, query: str, params: Optional[List] = None) -> None:
        with ddb.connect(self.db_loc) as con:
            con.execute(query, params)
            con.commit()

    def delete(self, query: str, params: Optional[List] = None) -> None:
        with ddb.connect(self.db_loc) as con:
            con.execute(query, params)
            con.commit()

    def dict_to_sql(self, table_name: str, conditions: Dict = None) -> str:
        if not conditions:
            return f"SELECT * FROM {table_name}"
        where_clause = " AND ".join([f"{key}='{value}'" if isinstance(value, str) else f"{key}={value}" for key, value in conditions.items()])
        return f"SELECT * FROM {table_name} WHERE {where_clause}"

    def read_dir(self, directory: str, file_type: str) -> Generator[Dict, None, None]:
        query = ddb.execute(f"SELECT * FROM '{directory}*.{file_type}'")
        record_batch_reader = query.fetch_record_batch()
        try:
            while True:
                chunk = record_batch_reader.read_next_batch()
                if len(chunk) == 0:
                    break
                yield chunk.to_pylist()
        except StopIteration:
            pass

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

    def get_strategy_names(self) -> List[str]:        
        strategies = self.get("SELECT * FROM view_strategy_identifiers WHERE NAME='name'")
        return [x['VALUE'] for x in strategies]

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

    def get_methodology_id_by_name(self, name: str) -> Optional[int]:
        id_type = self.get("SELECT ID FROM methodology WHERE FUNCTION_NAME = ?", [name.lower()])
        return id_type[0]['ID'] if id_type else None
    
    def get_methodology_types(self) -> List[str]:
        m_types = self.get("SELECT DISTINCT NAME FROM methodology_types WHERE VALID_FROM <= current_date() AND VALID_TO >= current_date();")
        return [type['NAME'] for type in m_types]
    
    def get_methodology_by_type_id(self, type_id: int) -> List[str]:
        meths = self.get("SELECT FUNCTION_NAME FROM methodology WHERE TYPE_ID = ?", [type_id])
        return [meth['FUNCTION_NAME'] for meth in meths]
    
    def get_methodology_by_type_name(self, type: str) -> List[str]:
        meths = self.get("SELECT FUNCTION_NAME FROM methodology WHERE TYPE_ID = (SELECT ID FROM methodology_types WHERE NAME = ?)", [type.lower()])
        return [meth['FUNCTION_NAME'] for meth in meths]
    
    def get_methodology_type_id_by_type_name(self, name: str) -> Optional[int]:
        id_type = self.get("SELECT ID FROM methodology_types WHERE NAME = ?", [name.lower()])
        return id_type[0]['ID'] if id_type else None

    def create_methodology_type(self, name: str) -> int:
        return self.post("INSERT INTO methodology_types (NAME) VALUES (?) RETURNING ID", [name.lower()])
    
    def get_methodology_types_by_strategy_id(self, strategy_id: int) -> List[str]:
        meths = self.get("SELECT DISTINCT NAME FROM methodology_types WHERE VALID_FROM <= current_date() AND VALID_TO >= current_date() AND ID IN (SELECT TYPE_ID FROM strategy_methodology_relationship WHERE STRATEGY_ID = ?)", [strategy_id])
        return [type['NAME'] for type in meths]
