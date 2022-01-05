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


class ProspectFilesProgressResponse(BaseModel):
    total_rows: int
    done: int
