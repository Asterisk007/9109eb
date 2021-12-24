from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger, String, ARRAY

from api.database import Base

class ProspectsFiles(Base):
    """Prospects Files Table"""

    __tablename__ = "prospects_files"

    id = Column(BigInteger, primary_key=True, autoincrement=True, unique=True)
    data = Column(ARRAY(String, dimensions=2), nullable=False)
    
    def __repr__(self):
        return f"{self.id} | {len(self.data)} files"
