from typing import List
from pydantic import BaseModel

class ProspectsFiles(BaseModel):
    """A processed CSV file of Prospect data"""
    id: int
    data: List[List[str]]

class ProspectsFilesResponse(BaseModel):
    """A preview of prospects file data"""
    id: int
    preview: List[List[str]]

class ProspectsFilesProgress(BaseModel):
    """Prospects processed from total prospects in a given file"""
    id: int
    file_id: int
    total: int
    done: int

class ProspectFilesProgressResponse(BaseModel):
    total: int
    done: int