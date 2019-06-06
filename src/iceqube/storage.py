import logging
import uuid
from contextlib import contextmanager
from copy import copy

from sqlalchemy import Column, DateTime, Index, Integer, PickleType, String, event, func, or_
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from iceqube.classes import State
from iceqube.exceptions import JobNotFound

Base = declarative_base()


class ORMJob(Base):
    """
    The DB representation of a common.classes.Job object,
    storing the relevant details needed by the job storage
    backend.
    """
    __tablename__ = "jobs"

    # The hex UUID given to the job upon first creation
    id = Column(String, primary_key=True, autoincrement=False)

    # The job's state. Inflated here for easier querying to the job's state.
    state = Column(String, index=True)

    # The job's order in the entire global queue of jobs.
    queue_order = Column(Integer, autoincrement=True)

    # The app name passed to the client when the job is scheduled.
    app = Column(String, index=True)

    # The app name passed to the client when the job is scheduled.
    namespace = Column(String, index=True)

    # The original Job object, pickled here for so we can easily access it.
    obj = Column(PickleType)

    time_created = Column(DateTime(timezone=True), server_default=func.now())
    time_updated = Column(DateTime(timezone=True), server_onupdate=func.now())

    __table_args__ = (Index('app_namespace_index', 'app', 'namespace'),)


class Storage(object):

    def __init__(self, app, namespace, connection, *args, **kwargs):
        self.app = app
        self.namespace = namespace

        self.engine = connection
        if self.engine.name == "sqlite":
            self.set_sqlite_pragmas()
        Base.metadata.create_all(self.engine)
        self.sessionmaker = sessionmaker(bind=self.engine)

    @contextmanager
    def session_scope(self):
        session = self.sessionmaker()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def set_sqlite_pragmas(self):
        """
        Sets the connection PRAGMAs for the sqlalchemy engine stored in self.engine.

        It currently sets:
        - journal_mode to WAL

        :return: None
        """
        try:
            self.engine.execute("PRAGMA journal_mode = WAL;")
        except OperationalError:
            pass

    def enqueue_job(self, j):
        """
        Add the job given by j to the job queue.

        Note: Does not actually run the job.
        """
        job_id = uuid.uuid4().hex
        j.job_id = job_id

        with self.session_scope() as session:
            orm_job = ORMJob(
                id=job_id,
                state=j.state,
                app=self.app,
                namespace=self.namespace,
                obj=j)
            session.add(orm_job)
            try:
                session.commit()
            except Exception as e:
                logging.error(
                    "Got an error running session.commit(): {}".format(e))

            return job_id

    def mark_job_as_canceled(self, job_id):
        """

        Mark the job as canceled. Does not actually try to cancel a running job.

        """
        self._update_job(job_id, State.CANCELED)

    def mark_job_as_canceling(self, job_id):
        """
        Mark the job as requested for canceling. Does not actually try to cancel a running job.

        :param job_id: the job to be marked as canceling.
        :return: None
        """
        self._update_job(job_id, State.CANCELING)

    def get_next_queued_job(self):
        with self.session_scope() as s:
            orm_job = self._ns_query(s).filter_by(
                state=State.QUEUED).order_by(ORMJob.queue_order).first()
            if orm_job:
                job = orm_job.obj
            else:
                job = None
            return job

    def get_canceling_jobs(self):
        with self.session_scope() as s:
            jobs = self._ns_query(s).filter_by(state=State.CANCELING).order_by(ORMJob.queue_order)
            return [job.obj for job in jobs]

    def get_all_jobs(self):
        with self.session_scope() as s:
            orm_jobs = self._ns_query(s).all()
            return [o.obj for o in orm_jobs]

    def count_all_jobs(self):
        with self.session_scope() as s:
            return self._ns_query(s).count()

    def get_job(self, job_id):
        with self.session_scope() as session:
            job, _ = self._get_job_and_orm_job(job_id, session)
            return job

    def clear(self, job_id=None, force=False):
        """
        Clear the queue and the job data.
        If force is True, clear all jobs, otherwise only delete jobs that are in a finished state,
        COMPLETED, FAILED, or CANCELED.
        :type job_id: NoneType or str
        :param job_id: the job_id to clear. If None, clear all jobs.
        :type force: bool
        :param force: If True, clear the job (or jobs), even if it hasn't completed, failed or been cancelled.
        """
        with self.session_scope() as s:
            q = self._ns_query(s)
            if job_id:
                q = q.filter_by(id=job_id)

            # filter only by the finished jobs, if we are not specified to force
            if not force:
                q = q.filter(
                    or_(ORMJob.state == State.COMPLETED, ORMJob.state == State.FAILED, ORMJob.state == State.CANCELED))

            q.delete(synchronize_session=False)

    def update_job_progress(self, job_id, progress, total_progress):
        """
        Update the job given by job_id's progress info.
        :type total_progress: int
        :type progress: int
        :type job_id: str
        :param job_id: The id of the job to update
        :param progress: The current progress achieved by the job
        :param total_progress: The total progress achievable by the job.
        :return: None
        """
        self._update_job(job_id, progress=progress, total_progress=total_progress)

    def mark_job_as_failed(self, job_id, exception, traceback):
        """
        Mark the job as failed, and record the traceback and exception.
        Args:
            job_id: The job_id of the job that failed.
            exception: The exception object thrown by the job.
            traceback: The traceback, if any. Note (aron): Not implemented yet. We need to find a way
            for the conncurrent.futures workers to throw back the error to us.

        Returns: None

        """
        self._update_job(job_id, State.FAILED, exception=exception, traceback=traceback)

    def mark_job_as_running(self, job_id):
        self._update_job(job_id, State.RUNNING)

    def complete_job(self, job_id):
        self._update_job(job_id, State.COMPLETED)

    def _update_job(self, job_id, state=None, **kwargs):
        with self.session_scope() as session:

            job, orm_job = self._get_job_and_orm_job(job_id, session)

            # Note (aron): looks like SQLAlchemy doesn't automatically
            # save any pickletype fields even if we re-set (orm_job.obj = job) that
            # field. My hunch is that it's tracking the id of the object,
            # and if that doesn't change, then SQLAlchemy doesn't repickle the object
            # and save to the DB.
            # Our hack here is to just copy the job object, and then set thespecific
            # field we want to edit, in this case the job.state. That forces
            # SQLAlchemy to re-pickle the object, thus setting it to the correct state.
            job = copy(job)
            if state is not None:
                orm_job.state = job.state = state
            for kwarg in kwargs:
                setattr(job, kwarg, kwargs[kwarg])
            orm_job.obj = job
            session.add(orm_job)
            return job, orm_job

    def _get_job_and_orm_job(self, job_id, session):
        orm_job = self._ns_query(session).filter_by(id=job_id).one_or_none()
        if orm_job is None:
            raise JobNotFound()
        job = orm_job.obj
        return job, orm_job

    def _ns_query(self, session):
        """
        Return a SQLAlchemy query that is already namespaced by the app and namespace given to this backend
        during initialization.
        Returns: a SQLAlchemy query object

        """
        return session.query(ORMJob).filter(ORMJob.app == self.app,
                                            ORMJob.namespace == self.namespace)
