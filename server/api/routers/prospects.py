from logging import Logger
from os import stat
from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.datastructures import UploadFile
from fastapi.params import File
from sqlalchemy.orm.session import Session
from api import schemas
from api.dependencies.auth import get_current_user
from api.core.constants import DEFAULT_PAGE, DEFAULT_PAGE_SIZE
from api.crud import ProspectCrud
from api.dependencies.db import get_db
from api.models.prospectsfiles import ProspectsFiles
from api.schemas.prospectsfiles import ProspectFilesProgressResponse
from csv import reader

router = APIRouter(prefix="/api", tags=["prospects"])


@router.get("/prospects", response_model=schemas.ProspectResponse)
def get_prospects_page(
    current_user: schemas.User = Depends(get_current_user),
    page: int = DEFAULT_PAGE,
    page_size: int = DEFAULT_PAGE_SIZE,
    db: Session = Depends(get_db),
):
    """Get a single page of prospects"""
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Please log in"
        )
    prospects = ProspectCrud.get_users_prospects(db, current_user.id, page, page_size)
    total = ProspectCrud.get_user_prospects_total(db, current_user.id)
    return {"prospects": prospects, "size": len(prospects), "total": total}

@router.post("/prospects_files", response_model=schemas.ProspectsFilesResponse)
async def post_prospects_file(
    current_user: schemas.User = Depends(get_current_user),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """
        POST file to be processed.
        This route will read the first few lines and respond with
        a ProspectFile schema object.
    """
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Please log in"
        )
    
    fb = file.file.read()

    # Check that file is under size limit
    if len(fb) > (200 * (2**1024)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="File size limit is 200 MB"
        )
    
    await file.seek(0)

    file_data = file.file.read().decode("utf-8")

    csv_data = []
    for lines in file_data.splitlines():
        if lines != "":
            line = []
            for x in lines.split(","):
                line.append(x)
            csv_data.append(line)

    # Save file to database
    prospectsfiles_obj = ProspectCrud.create_prospects_file(db=db, data=csv_data)
    
    return { "id": prospectsfiles_obj.id, "preview": prospectsfiles_obj.data[0:3] }


@router.post("/prospects_files/{id:str}/prospects", response_model=schemas.ProspectsFiles)
async def set_columns(
    current_user: schemas.User = Depends(get_current_user),
    id: int = 0,
    email_index: int = 0,
    first_name_index: int = 1,
    last_name_index: int = 2,
    force: bool = False,
    has_headers: bool = False,
    db: Session = Depends(get_db)
) -> ProspectsFiles:
    # Route for starting the process of adding prospects to the database"""
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Please log in"
        )
    
    try:
        response_obj = await ProspectCrud.set_columns(db=db, user_id=current_user.id, id=id, options={
            "email_index": email_index,
            "first_name_index": first_name_index,
            "last_name_index": last_name_index,
            "force": force,
            "has_headers": has_headers
        })
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=e.args[0]
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=e.args[0]
        )
    return { "id": response_obj.id, "data": response_obj.data }

@router.get("/prospect_files/{id:str}/progress", response_model=schemas.ProspectFilesProgressResponse)
def get_csv_progress(
    db: Session = Depends(get_db),
    current_user: schemas.User = Depends(get_current_user),
    id: int = 0
) -> ProspectFilesProgressResponse:
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Please log in"
        )
    
    try:
        progress = ProspectCrud.get_prospects_file_progress(db, current_user.id, id)
        return { "total": progress.total, "done": progress.done }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=e.args[0]
        )