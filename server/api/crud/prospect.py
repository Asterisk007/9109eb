import asyncio
from contextvars import ContextVar
from logging import Logger
import multiprocessing
from multiprocessing import process
from typing import Dict, List, Set, Union
from fastapi.param_functions import Depends
from sqlalchemy.orm import session
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.functions import user
from sqlalchemy.sql.sqltypes import String
from api import schemas
from api.dependencies.db import get_db
from api.models import Prospect, ProspectsFiles, ProspectsFilesProgress
from api.core.constants import DEFAULT_PAGE_SIZE, DEFAULT_PAGE, MIN_PAGE, MAX_PAGE_SIZE

from asyncio import create_task
from multiprocessing import Process
from functools import partial


class ProspectCrud:
    async def create_prospect_from_csv(
        self, db: Session, user_id: int, file_id: int, data: List[String], options: Dict
    ):
        overwrite_existing = options["force"] == True
        # Check if a prospect with this email exists
        existing_prospect = (
            db.query(Prospect)
            .filter(Prospect.email == data[options["email_index"]])
            .all()
        )

        # If this prospect exists but the user wants to overwrite,
        # overwrite. Else, skip.
        if len(existing_prospect) > 0 and overwrite_existing:
            existing_prospect = existing_prospect[0]
            existing_prospect.first_name = data[options["first_name_index"]]
            existing_prospect.last_name = data[options["last_name_index"]]
            existing_prospect.email = data[options["email_index"]]
            db.commit()
            self.update_csv_progress(self, db=db, file_id=file_id)
            return
        elif len(existing_prospect) == 0:
            self.create_prospect(
                db=db,
                user_id=user_id,
                data={
                    "email": data[options["email_index"]],
                    "first_name": data[options["first_name_index"]],
                    "last_name": data[options["last_name_index"]],
                },
            )
            self.update_csv_progress(self, db=db, file_id=file_id)
            return
        else:
            return

    def update_csv_progress(self, db: Session, file_id: int):
        """Update the total number of prospect entries processed from a CSV file"""
        progress = db.query(ProspectsFilesProgress).get({"file_id": file_id})
        progress.done += 1
        db.commit()

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
        cls, db: Session, data: List[List[String]]
    ) -> ProspectsFiles:
        prospect_file = ProspectsFiles(data=data)
        db.add(prospect_file)
        db.commit()
        db.refresh(prospect_file)
        return prospect_file

    @classmethod
    async def set_columns(
        self, db: Session, user_id: int, id: int, options: Dict
    ) -> ProspectsFiles:
        prospects_file = db.query(ProspectsFiles).get(id)

        if prospects_file is None:
            Logger.error("No prospects file was found with id of " + str(id))
            raise FileNotFoundError("No such file in the database")

        # Skip first line if CSV file has headers
        if options["has_headers"] == True:
            starting_index = 1
        else:
            starting_index = 0

        # Check for existing session to insert new prospects
        existing_progress = db.query(ProspectsFilesProgress).get({"file_id": id})
        if existing_progress is not None:
            # print("Restarting previous session")
            prospects_file_progress = existing_progress
            prospects_file_progress.done = 0
        else:
            # print("Creating new session")
            prospects_file_progress = ProspectsFilesProgress(
                file_id=id, done=0, total=len(prospects_file.data)
            )
            db.add(prospects_file_progress)
        db.commit()
        db.refresh(prospects_file_progress)

        tasks = []
        """
        pool = multiprocessing.Pool(processes=4)
        run_func = partial(self.create_prospect_from_csv, self, db, user_id, id, options)
        """
        for row in prospects_file.data[starting_index:]:
            # Create an asyncio task which will add this prospect to the database.
            """
            CSV will always be formatted correctly, but
            data will not always exist in a column (i.e. any columns could
            be empty for a given row). Since the Prospect model requires
            an email for a primary key, if no email exists then skip that row.
            """
            if "@" in row[options["email_index"]]:
                """
                tasks.append(row)
                """
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
        """
        pool.map(run_func, tasks)
        """
        await asyncio.gather(*tasks)
        return prospects_file

    @classmethod
    def get_prospects_file_progress(
        self, db: Session, user_id: int, id: int
    ) -> ProspectsFilesProgress:
        progress = db.query(ProspectsFilesProgress).get({"file_id": id})
        if progress is None:
            raise Exception("No such file is being processed.")

        return progress
