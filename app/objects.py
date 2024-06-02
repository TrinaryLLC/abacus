from typing import List, Tuple
from pydantic import BaseModel

class MethodologyType(BaseModel):
    name: str
    description: str

class Methodology(BaseModel):
    name: str
    type: MethodologyType
    function_repo: str
    function_branch: str
    function_file: str
    function_name: str
    function_description: str

class Strategy(BaseModel):
    name: str
    description: str
    methodology: List[Tuple[MethodologyType, Methodology]]
    # def __str__(self):
    #     return self.name

class InstrumentType(BaseModel):
    name: str
    description: str

class Instrument(BaseModel):
    name: str
    type: InstrumentType
    
class InstrumentIdentifierType(BaseModel):
    name: str
    description: str

class InstrumentIdentifier(BaseModel):    
    name: str    
    type: InstrumentIdentifierType
    value: str