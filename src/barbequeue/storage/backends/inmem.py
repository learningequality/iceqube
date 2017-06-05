import uuid
from collections import defaultdict, deque
from copy import copy

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, PickleType, Boolean, DateTime, func, create_engine, Index
from sqlalchemy.orm import sessionmaker

from barbequeue.common.classes import Job, State
from barbequeue.storage.backends.default import BaseBackend

INMEM_STORAGE = {}
INMEM_QUEUE = defaultdict(lambda: deque())

Base = declarative_base()


class ORMJob(Base):
    __tablename__ = "jobs"

    # The hex UUID given to the job upon first creation
    id = Column(String, primary_key=True, autoincrement=False)

    # The job's state. Inflated here for easier querying to the job's state.
    state = Column(Integer, index=True)

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


class Backend(BaseBackend):
    def __init__(self, app, namespace, *args, **kwargs):
        self.app = app
        self.namespace = namespace
        self.namespace_id = uuid.uuid5(uuid.NAMESPACE_DNS, app + namespace).hex
        self.queue = INMEM_QUEUE[self.namespace_id]

        self.engine = create_engine('sqlite:///:memory:')
        self.session = sessionmaker(bind=self.engine)()

        # create the tables if they don't exist yet
        Base.metadata.create_all(self.engine)
        super(Backend, self).__init__(*args, **kwargs)

    def schedule_job(self, j):
        """
        Add the job given by j to the job queue.

        Note: Does not actually run the job.
        """
        job_id = uuid.uuid4().hex
        j.job_id = job_id

        orm_job = ORMJob(id=job_id, state=j.state, app=self.app, namespace=self.namespace, obj=j)
        self.session.add(orm_job)
        self.session.commit()

        return job_id

    def cancel_job(self, job_id):
        """

        Mark the job as canceled. Does not actually try to cancel a running job.

        """
        job = self._get_job_nocopy(job_id)

        # Mark the job as canceled.
        job.state = State.CANCELED

        # Remove it from the queue.
        self.queue.remove(job_id)

        return self.get_job(job)

    def _get_job_and_orm_job(self, job_id):
        orm_job = self.session.query(ORMJob).filter_by(id=job_id).one()
        job = orm_job.obj
        return job, orm_job

    def get_next_scheduled_job(self):
        try:
            job = self.get_job(self.queue[0])
        except IndexError:
            job = None

        # logging.warning("getting job with job id {}".format(job.job_id))
        return job

    def get_scheduled_jobs(self):
        return [self.get_job(jid) for jid in INMEM_QUEUE[self.namespace_id]]

    def get_job(self, job_id):
        job, _ = self._get_job_and_orm_job(job_id)
        return job

    def clear(self, job_id=None):
        """
        Clear the queue and the job data. If job_id is not given, clear out all
        jobs marked COMPLETED. If job_id is given, clear out the given job's
        data. This function won't do anything if the job's state is not COMPLETED.
        """
        q = self._ns_query()
        if job_id:
            q = q.filter_by(id=job_id)

        q = q.filter_by(state=State.COMPLETED)

        q.delete()

    def update_job_progress(self, job_id, progress, total_progress):
        job, orm_job = self._update_job_state(job_id, state=State.RUNNING, commit=False)
        job.progress = progress
        job.total_progress = total_progress
        orm_job.obj = job

        self.session.commit()
        self.notify_of_job_update(job_id)
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

    def _update_job_state(self, job_id, state, commit=True):
        job, orm_job = self._get_job_and_orm_job(job_id)
        orm_job.state = job.state = state
        orm_job.obj = job
        if commit:
            self.session.commit()
            self.notify_of_job_update(job_id)
        return job, orm_job

    def _ns_query(self):
        """
        Return a SQLAlchemy query that is already namespaced by the app and namespace given to this backend
        during initialization.
        Returns: a SQLAlchemy query object

        """
        return self.session.query(ORMJob).filter(ORMJob.app == self.app, ORMJob.namespace == self.namespace)
