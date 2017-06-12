import uuid
from collections import defaultdict, deque
from copy import copy
from threading import Event

import logging
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, PickleType, Boolean, DateTime, func, create_engine, Index
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool, AssertionPool, StaticPool

from barbequeue.common.classes import State
from barbequeue.storage.backends.default import BaseBackend

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
    def __init__(self, app, namespace, *args, **kwargs):
        self.app = app
        self.namespace = namespace
        self.namespace_id = uuid.uuid5(uuid.NAMESPACE_DNS, app + namespace).hex

        self.engine = create_engine('sqlite:///:memory:', connect_args={'check_same_thread': False},
                                    poolclass=StaticPool)
        Base.metadata.create_all(self.engine)
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.hack_hack_hack_now_has_scheduled_job = Event()

        # create the tables if they don't exist yet
        super(StorageBackend, self).__init__(*args, **kwargs)

    def schedule_job(self, j):
        """
        Add the job given by j to the job queue.

        Note: Does not actually run the job.
        """
        job_id = uuid.uuid4().hex
        j.job_id = job_id

        session = self.sessionmaker()
        orm_job = ORMJob(id=job_id, state=j.state, app=self.app, namespace=self.namespace, obj=j)
        session.add(orm_job)
        try:
            session.commit()
        except Exception as e:
            logging.error("Got an error running session.commit(): {}".format(e))
        self.hack_hack_hack_now_has_scheduled_job.set()

        return job_id

    def cancel_job(self, job_id):
        """

        Mark the job as canceled. Does not actually try to cancel a running job.

        """
        raise NotImplementedError
        # job = self._get_job_nocopy(job_id)
        #
        # # Mark the job as canceled.
        # job.state = State.CANCELED
        #
        # # Remove it from the queue.
        # self.queue.remove(job_id)
        #
        # return self.get_job(job)

    def get_next_scheduled_job(self):
        s = self.sessionmaker()
        orm_job = self._ns_query(s).filter_by(state=State.SCHEDULED).order_by(ORMJob.queue_order).first()
        s.close()
        if orm_job:
            job = orm_job.obj
        else:
            job = None
        return job

    def get_scheduled_jobs(self):
        return [self.get_job(jid) for jid in INMEM_QUEUE[self.namespace_id]]

    def get_job(self, job_id):
        s = self.sessionmaker()
        job, _ = self._get_job_and_orm_job(job_id, s)
        s.close()
        return job

    def clear(self, job_id=None):
        """
        Clear the queue and the job data. If job_id is not given, clear out all
        jobs marked COMPLETED. If job_id is given, clear out the given job's
        data. This function won't do anything if the job's state is not COMPLETED.
        """
        s = self.sessionmaker()
        q = self._ns_query(s)
        if job_id:
            q = q.filter_by(id=job_id)

        q = q.filter_by(state=State.COMPLETED)
        s.flush()

        q.delete()

    def update_job_progress(self, job_id, progress, total_progress):
        session = self.sessionmaker()
        job, orm_job = self._update_job_state(job_id, state=State.RUNNING, session=session)
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
        self._update_job_state(job_id, State.FAILED)

    def mark_job_as_running(self, job_id):
        self._update_job_state(job_id, State.RUNNING)

    def complete_job(self, job_id):
        self._update_job_state(job_id, State.COMPLETED)

    def _update_job_state(self, job_id, state, session=None):
        scoped_session = session if session else self.sessionmaker()

        job, orm_job = self._get_job_and_orm_job(job_id, scoped_session)
        orm_job.state = job.state = state
        orm_job.obj = job
        session.add(orm_job)
        session.commit()
        if not session:  # session was created by our hand. Close it now.
            scoped_session.commit()
            scoped_session.close()
        self.notify_of_job_update(job_id)
        return job, orm_job

    def _get_job_and_orm_job(self, job_id, session):
        orm_job = session.query(ORMJob).filter_by(id=job_id).one()
        job = orm_job.obj
        return job, orm_job

    def _ns_query(self, session):
        """
        Return a SQLAlchemy query that is already namespaced by the app and namespace given to this backend
        during initialization.
        Returns: a SQLAlchemy query object

        """
        return session.query(ORMJob).filter(ORMJob.app == self.app, ORMJob.namespace == self.namespace)
