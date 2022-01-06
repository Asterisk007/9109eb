from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.datastructures import UploadFile
from fastapi.params import File
from sqlalchemy.orm.session import Session
from api import schemas
from api.dependencies.auth import get_current_user
from api.core.constants import (
    DEFAULT_PAGE,
    DEFAULT_PAGE_SIZE,
    FILE_SIZE_LIMIT,
    FILE_LINE_LIMIT,
)
from api.crud import ProspectCrud
from api.dependencies.db import get_db
from api.models.prospectsfiles import ProspectsFiles
from api.schemas.prospectsfiles import ProspectFilesProgressResponse

from csv import reader
import codecs

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

    file_bytes = file.file.read()
    line_count = len(file_bytes.decode("utf-8").split("\n"))

    # Check that file is under size limit
    # File must be under 200 MB or contain fewer than 1,000,000 rows
    if len(file_bytes) > FILE_SIZE_LIMIT or line_count > FILE_LINE_LIMIT:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File size limit is 200 MB or 1 million rows",
        )

    await file.seek(0)

    csv_file = reader(codecs.iterdecode(file.file, "utf-8"))
    csv_data = []
    for row in csv_file:
        if len(row) == 0:
            continue
        csv_data.append(row)

    # Save file to database
    prospectsfiles_obj = ProspectCrud.create_prospects_file(
        db=db, data=csv_data, user_id=current_user.id
    )

    return {"id": prospectsfiles_obj.id, "preview": prospectsfiles_obj.data[0:3]}


@router.post(
    "/prospects_files/{id:int}/prospects", response_model=schemas.ProspectsFiles
)
async def set_columns(
    id: int,
    email_index: int,
    first_name_index: int,
    last_name_index: int,
    force: bool,
    has_headers: bool,
    db: Session = Depends(get_db),
    current_user: schemas.User = Depends(get_current_user),
) -> ProspectsFiles:
    """Route for starting the process of adding prospects to the database"""
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Please log in"
        )

    try:
        response_obj = await ProspectCrud.set_csv_column_indices(
            db=db,
            user_id=current_user.id,
            id=id,
            options={
                "email_index": email_index,
                "first_name_index": first_name_index,
                "last_name_index": last_name_index,
                "force": force,
                "has_headers": has_headers,
            },
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.args[0])
    except PermissionError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=e.args[0])
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=e.args[0])
    return {"id": response_obj.id, "data": response_obj.data}


@router.get(
    "/prospect_files/{id:int}/progress",
    response_model=schemas.ProspectFilesProgressResponse,
)
def get_csv_progress(
    id: int,
    db: Session = Depends(get_db),
    current_user: schemas.User = Depends(get_current_user),
) -> ProspectFilesProgressResponse:
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Please log in"
        )

    try:
        response = ProspectCrud.get_prospects_file_progress(db, current_user.id, id)
        return response
    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.args[0])
    except PermissionError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=e.args[0])
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=e.args[0])
