import copy
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import simpy

import logging

from job import Job, Queue
from processes.execution import execution_process
from resource_manager import ResourceManager, u_pred
from util import simtime_to_datetime, datetime_to_simtime, round_down_dt

logging.basicConfig(format="")
logger = logging.getLogger(__name__)


def admission_control_process(env: simpy.Environment,
                              policy: str,
                              incoming_jobs: List[Job],
                              queue: Queue,
                              resource_manager: ResourceManager,
                              u_df: pd.DataFrame):
    """Simpy process that simulates the arrival of job requests to the admission control.

    Args:
        env: Simpy environment for managing the process execution
        policy: Forecast mode used in the execution process
        incoming_jobs: List of jobs that get send to the admission control during the experiment
        queue: Runtime queue
        resource_manager: Runtime resource manager
        u_df: free/freep capacity forecasts that the admission control bases its decisions on
    """
    for job in incoming_jobs:
        yield env.timeout(datetime_to_simtime(job.arrive_time) - env.now)  # wait until arrive_time of next job
        now = simtime_to_datetime(env.now)

        if policy == "Naive":
            u_freep, _ = resource_manager.u_now(now)
            job.accepted = len(queue.items) == 0 and u_freep > 0
        else:
            job.accepted = _admission_control(
                new_job=copy.deepcopy(job),  # admission_control should not have access to the real job
                queued_jobs=copy.deepcopy(queue.items),  # admission_control should not have access to the real queue
                active_job=copy.deepcopy(queue.active_job(now)),  # admission_control should not have access to the real queue
                initial_u_used=resource_manager.u_used(now),
                now=now,
                policy=policy,
                u_df=u_df
            )
        if job.accepted:
            yield queue.put(job)  # yield is instant as the queue is unlimited


def _admission_control(new_job: Job, queued_jobs: List[Job], active_job: Optional[Job], now: datetime,
                       initial_u_used: float, policy: str, u_df: pd.DataFrame):
    """Admission control policy that simulates the future execution of the queue.

    Args:
        new_job: Job request that the admission control will accept or reject
        queued_jobs: Currently queued jobs
        active_job: Currently active job
        now: Current time
        initial_u_used: mch used during current time step
        policy: Forecast mode used in the execution process
        u_df: free/freep capacity forecasts that the admission control bases its decisions on

    Returns:
        True if <new_job> should get accepted, False otherwise.
    """
    queued_jobs.append(new_job)
    queued_jobs.sort(key=lambda j: j.deadline)

    if active_job is None:
        # Previous job finished before time step
        initial_time = now
        initial_u_used = 0
    else:
        # if there is an active job add it at highest priority (non-preemtive scheduling)
        queued_jobs.insert(0, active_job)
        initial_time = active_job.last_processed

    _new_queue_str = "open jobs: " + ", ".join([f"{j.id}" for j in queued_jobs]) if len(queued_jobs) < 10 else f"{len(queued_jobs)} open jobs"
    logger.debug(f"{now}: [ADMISSION] Checking admission for job {new_job.id} ({_new_queue_str})")

    forecasts = u_pred(u_df, policy, start_date=round_down_dt(initial_time), initial_u_used=initial_u_used)
    fake_resource_manager = ResourceManager(forecasts)

    # Speedup: If the remaining_queue_size is bigger than the u_freep
    # we can directly reject without checking individual deadlines
    remaining_queue_size = sum(j.size_remaining for j in queued_jobs)
    u_freep, _ = fake_resource_manager.u_in_daterange(start=initial_time, end=queued_jobs[-1].deadline)
    u_freep = u_freep - initial_u_used
    if remaining_queue_size > u_freep:
        logger.info(f"{now}: [ADMISSION] Rejected job {new_job.id} (queue would reserve {remaining_queue_size:.2f}/{u_freep:.2f} available mch)")
        return False
    elif queued_jobs[0].deadline == queued_jobs[-1].deadline:
        logger.info(f"{now}: [ADMISSION] All queued jobs have same deadline and use only {remaining_queue_size:.2f}/{u_freep:.2f} available mch")
        logger.info(f"{now}: [ADMISSION] Accepted job {new_job.id} (open jobs: {_new_queue_str})")
        return True
    else:
        # Simulate execution
        logger.debug(f"{now}: [ADMISSION] Simulating from {initial_time} ({initial_u_used:.2f} mch already used)")
        # Run fake scenario to simulate execution according to forecast_mode
        fake_env = simpy.Environment(initial_time=datetime_to_simtime(initial_time))
        fake_queue = simpy.Store(fake_env)  # FIFO
        for job in queued_jobs:
            fake_queue.put(job)
        fake_env.process(execution_process(fake_env, fake_queue, resource_manager=fake_resource_manager, simulated=True))
        fake_env.run(until=datetime_to_simtime(round_down_dt(initial_time) + timedelta(days=1)))  # Simulate one day max

        # Check for deadline misses
        for job in queued_jobs:
            if not job.finished:
                logger.debug(f"- Job {job.id} will not finish within 24 hours")
                logger.info(f"{now}: [ADMISSION] Rejected job {new_job.id} (queued job {job.id} would miss its deadline)")
                return False
            if not job.finished_before_deadline:
                logger.debug(f"- Job {job.id} will finish at {job.finish_time} and miss its deadline {job.deadline}")
                logger.info(f"{now}: [ADMISSION] Rejected job {new_job.id} (queued job {job.id} would miss its deadline)")
                return False

        logger.info(f"{now}: [ADMISSION] Accepted job {new_job.id}")
        return True
