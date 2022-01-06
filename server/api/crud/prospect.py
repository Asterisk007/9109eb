import asyncio
from typing import Dict, List, Set, Union
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.sqltypes import String
from api import schemas
from api.models import Prospect, ProspectsFiles
from api.schemas import ProspectFilesProgressResponse
from api.core.constants import DEFAULT_PAGE_SIZE, DEFAULT_PAGE, MIN_PAGE, MAX_PAGE_SIZE

from asyncio import create_task


class ProspectCrud:
    async def create_prospect_from_csv(
        self, db: Session, user_id: int, file_id: int, data: List[String], options: Dict
    ):
        overwrite_existing = options["force"] == True
        # Check if a prospect with this email exists
        existing_prospect = (
            db.query(Prospect)
            .filter(
                Prospect.user_id == user_id,
                Prospect.email == data[options["email_index"]],
            )
            .all()
        )

        # If this prospect exists but the user wants to overwrite,
        # overwrite. Else, skip.
        if len(existing_prospect) > 0 and overwrite_existing:
            existing_prospect = existing_prospect[0]
            existing_prospect.first_name = data[options["first_name_index"]]
            existing_prospect.last_name = data[options["last_name_index"]]
            existing_prospect.email = data[options["email_index"]]
            existing_prospect.file_id = file_id
            db.commit()
            return
        elif len(existing_prospect) == 0:
            self.create_prospect(
                db=db,
                user_id=user_id,
                data={
                    "email": data[options["email_index"]],
                    "first_name": data[options["first_name_index"]],
                    "last_name": data[options["last_name_index"]],
                    "file_id": file_id,
                },
            )
            return
        else:
            return

    @classmethod
    def get_users_prospects(
        cls,
        db: Session,
        user_id: int,
        page: int = DEFAULT_PAGE,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Union[List[Prospect], None]:
        """Get user's prospects"""
        if page < MIN_PAGE:
            page = MIN_PAGE
        if page_size > MAX_PAGE_SIZE:
            page_size = MAX_PAGE_SIZE
        return (
            db.query(Prospect)
            .filter(Prospect.user_id == user_id)
            .offset(page * page_size)
            .limit(page_size)
            .all()
        )

    @classmethod
    def get_user_prospects_total(cls, db: Session, user_id: int) -> int:
        return db.query(Prospect).filter(Prospect.user_id == user_id).count()

    @classmethod
    def create_prospect(
        cls, db: Session, user_id: int, data: schemas.ProspectCreate
    ) -> Prospect:
        """Create a prospect"""
        prospect = Prospect(**data, user_id=user_id)
        db.add(prospect)
        db.commit()
        db.refresh(prospect)
        return prospect

    @classmethod
    def validate_prospect_ids(
        cls, db: Session, user_id: int, unvalidated_prospect_ids: Set[int]
    ) -> Set[int]:
        res = (
            db.query(Prospect.id)
            .filter(
                Prospect.user_id == user_id, Prospect.id.in_(unvalidated_prospect_ids)
            )
            .all()
        )
        return {row.id for row in res}

    @classmethod
    def create_prospects_file(
        cls, db: Session, user_id: int, data: List[List[String]]
    ) -> ProspectsFiles:
        prospect_file = ProspectsFiles(data=data, user_id=user_id)
        db.add(prospect_file)
        db.commit()
        db.refresh(prospect_file)
        return prospect_file

    @classmethod
    async def set_csv_column_indices(
        self, db: Session, user_id: int, id: int, options: Dict
    ) -> ProspectsFiles:
        prospects_file = db.query(ProspectsFiles).get(id)

        if prospects_file is None:
            raise FileNotFoundError("No such file is in the database")
        elif prospects_file.user_id != user_id:
            raise PermissionError("You do not have access to this file")

        # Skip first line if CSV file has headers
        if options["has_headers"] == True:
            starting_index = 1
        else:
            starting_index = 0

        tasks = []
        for row in prospects_file.data[starting_index:]:
            """
            Create an asyncio task which will add this prospect to the database.

            CSV will always be formatted correctly, but
            data will not always exist in a column (i.e. any columns could
            be empty for a given row). Since the Prospect model requires
            an email for a primary key, if no email exists then skip that row.
            """
            if "@" in row[options["email_index"]]:
                tasks.append(
                    create_task(
                        self.create_prospect_from_csv(
                            self,
                            db=db,
                            user_id=user_id,
                            file_id=id,
                            data=row,
                            options=options,
                        )
                    )
                )

        await asyncio.gather(*tasks)
        return prospects_file

    @classmethod
    def get_prospects_file_progress(
        self, db: Session, user_id: int, id: int
    ) -> ProspectFilesProgressResponse:
        queried_file = db.query(ProspectsFiles).get(id)
        if queried_file is None:
            raise FileNotFoundError("No such file is in the database")
        elif queried_file.user_id != user_id:
            raise PermissionError("You do not have access to this file")

        total = len(queried_file.data)
        done = (
            db.query(Prospect)
            .filter(Prospect.user_id == user_id, Prospect.file_id == id)
            .count()
        )

        return {"total_rows": total, "done": done}
