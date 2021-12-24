from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger, Integer

from api.database import Base

class ProspectsFilesProgress(Base):
    """Progress of Prospects file being processed"""

    __tablename__ = "prospects_files_progress"

    id = Column(BigInteger, autoincrement=True, unique=True)
    file_id = Column(BigInteger, primary_key=True, unique=True)
    
    done = Column(Integer)
    total = Column(Integer)

    def __repr__(self):
        return f"{self.id} | {len(self.data)} files"
