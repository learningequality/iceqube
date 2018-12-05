import logging
import uuid
from collections import defaultdict, deque
from copy import copy

from sqlalchemy import Column, DateTime, Index, Integer, PickleType, String, create_engine, event, func, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import QueuePool, StaticPool

from iceqube.common.classes import State
from iceqube.storage.backends.default import BaseBackend

INMEM_STORAGE = {}
INMEM_QUEUE = defaultdict(lambda: deque())

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


class StorageBackend(BaseBackend):
    # constant for specifying an in-memory DB
    MEMORY = 1

    def __init__(self, app, namespace, storage_path, *args, **kwargs):
        self.app = app
        self.namespace = namespace

        if storage_path == self.MEMORY:
            storage_path = "sqlite:///:memory:"
            connection_class = StaticPool
        else:
            storage_path = "sqlite:///{path}".format(path=storage_path)
            connection_class = QueuePool

        self.engine = create_engine(
            storage_path,
            connect_args={'check_same_thread': False},
            poolclass=connection_class)
        self.set_sqlite_pragmas()
        Base.metadata.create_all(self.engine)
        self.sessionmaker = scoped_session(sessionmaker(bind=self.engine))

        # create the tables if they don't exist yet
        super(StorageBackend, self).__init__(*args, **kwargs)

    def set_sqlite_pragmas(self):
        """
        Sets the connection PRAGMAs for the sqlalchemy engine stored in self.engine.

        It currently sets:
        - journal_mode to WAL

        :return: None
        """

        def _pragmas_on_connect(dbapi_con, con_record):
            dbapi_con.execute("PRAGMA journal_mode = WAL;")

        event.listen(self.engine, "connect", _pragmas_on_connect)

    def schedule_job(self, j):
        """
        Add the job given by j to the job queue.

        Note: Does not actually run the job.
        """
        job_id = uuid.uuid4().hex
        j.job_id = job_id

        session = self.sessionmaker()
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
        job, _ = self._update_job_state(job_id, State.CANCELED)
        return job

    def mark_job_as_canceling(self, job_id):
        """
        Mark the job as requested for canceling. Does not actually try to cancel a running job.

        :param job_id: the job to be marked as canceling.
        :return: the job object
        """

        job, _ = self._update_job_state(job_id, State.CANCELING)
        return job

    def get_next_scheduled_job(self):
        s = self.sessionmaker()
        orm_job = self._ns_query(s).filter_by(
            state=State.SCHEDULED).order_by(ORMJob.queue_order).first()
        s.close()
        if orm_job:
            job = orm_job.obj
        else:
            job = None
        return job

    def get_scheduled_jobs(self):
        s = self.sessionmaker()
        jobs = self._ns_query(s).filter_by(state=State.SCHEDULED).order_by(ORMJob.queue_order)
        return [job.obj for job in jobs]

    def get_all_jobs(self):
        s = self.sessionmaker()
        orm_jobs = self._ns_query(s).all()
        s.close()
        return [o.obj for o in orm_jobs]

    def get_job(self, job_id):
        s = self.sessionmaker()
        job, _ = self._get_job_and_orm_job(job_id, s)
        s.close()
        return job

    def clear(self, job_id=None, force=False):
        """
        Clear the queue and the job data. If job_id is not given, clear out all
        jobs marked COMPLETED. If job_id is given, clear out the given job's
        data. This function won't do anything if the job's state is not COMPLETED or FAILED.
        :type job_id: NoneType or str
        :param job_id: the job_id to clear. If None, clear all jobs.
        :type force: bool
        :param force: If True, clear the job (or jobs), even if it hasn't completed or failed.
        """
        s = self.sessionmaker()
        q = self._ns_query(s)
        if job_id:
            q = q.filter_by(id=job_id)

        # filter only by the finished jobs, if we are not specified to force
        if not force:
            q = q.filter(
                or_(ORMJob.state == State.COMPLETED, ORMJob.state == State.FAILED))

        q.delete(synchronize_session=False)
        s.commit()
        s.close()

    def update_job_progress(self, job_id, progress, total_progress):
        """
        Update the job given by job_id's progress info.
        :type total_progress: int
        :type progress: int
        :type job_id: str
        :param job_id: The id of the job to update
        :param progress: The current progress achieved by the job
        :param total_progress: The total progress achievable by the job.
        :return: the job_id
        """

        session = self.sessionmaker()
        job, orm_job = self._update_job_state(
            job_id, state=State.RUNNING, session=session)

        # Note (aron): looks like SQLAlchemy doesn't automatically
        # save any pickletype fields even if we re-set (orm_job.obj = job) that
        # field. My hunch is that it's tracking the id of the object,
        # and if that doesn't change, then SQLAlchemy doesn't repickle the object
        # and save to the DB.
        # Our hack here is to just copy the job object, and then set thespecific
        # field we want to edit, in this case the job.state. That forces
        # SQLAlchemy to re-pickle the object, thus setting it to the correct state.

        job = copy(job)

        job.progress = progress
        job.total_progress = total_progress
        orm_job.obj = job

        session.add(orm_job)
        session.commit()
        session.close()
        return job_id

    def mark_job_as_queued(self, job_id):
        """
        Change the job given by job_id to QUEUED.

        :param job_id: the job to be marked as QUEUED.
        :return: None
        """
        self._update_job_state(job_id, State.QUEUED)

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
        session = self.sessionmaker()
        job, orm_job = self._update_job_state(
            job_id, State.FAILED, session=session)

        # Note (aron): looks like SQLAlchemy doesn't automatically
        # save any pickletype fields even if we re-set (orm_job.obj = job) that
        # field. My hunch is that it's tracking the id of the object,
        # and if that doesn't change, then SQLAlchemy doesn't repickle the object
        # and save to the DB.
        # Our hack here is to just copy the job object, and then set thespecific
        # field we want to edit, in this case the job.state. That forces
        # SQLAlchemy to re-pickle the object, thus setting it to the correct state.
        job = copy(job)

        job.exception = exception
        job.traceback = traceback
        orm_job.obj = job

        session.add(orm_job)
        session.commit()
        session.close()

    def mark_job_as_running(self, job_id):
        self._update_job_state(job_id, State.RUNNING)

    def complete_job(self, job_id):
        self._update_job_state(job_id, State.COMPLETED)

    def _update_job_state(self, job_id, state, session=None):
        scoped_session = session if session else self.sessionmaker()

        job, orm_job = self._get_job_and_orm_job(job_id, scoped_session)

        # Note (aron): looks like SQLAlchemy doesn't automatically
        # save any pickletype fields even if we re-set (orm_job.obj = job) that
        # field. My hunch is that it's tracking the id of the object,
        # and if that doesn't change, then SQLAlchemy doesn't repickle the object
        # and save to the DB.
        # Our hack here is to just copy the job object, and then set thespecific
        # field we want to edit, in this case the job.state. That forces
        # SQLAlchemy to re-pickle the object, thus setting it to the correct state.
        job = copy(job)

        orm_job.state = job.state = state
        orm_job.obj = job
        scoped_session.add(orm_job)
        if not session:  # session was created by our hand. Close it now.
            scoped_session.commit()
            scoped_session.close()
        self.notify_of_job_update(job_id)
        return job, orm_job

    def _get_job_and_orm_job(self, job_id, session):
        orm_job = self._ns_query(session).filter_by(id=job_id).one()
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
