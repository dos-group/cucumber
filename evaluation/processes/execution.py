import logging
from datetime import datetime

import simpy

from job import Job
from resource_manager import ResourceManager
from util import simtime_to_datetime, round_up_dt, timedelta_to_simtimedelta

logging.basicConfig(format="")
logger = logging.getLogger(__name__)


def execution_process(env: simpy.Environment, queue: simpy.Store, resource_manager: ResourceManager,
                      simulated: bool = False):
    """Simpy process that simulates the processing of queued jobs.

    Args:
        env: Simpy environment for managing the process execution
        queue: Queue of jobs that have to be executed
        resource_manager: Resource manager that determines how many resources can be used for executing jobs
        simulated: If True, the ResourceManager is used within the Cucumber simulation. Besides logging, this (1) omits
            the check for running the execution on full capacity in case of deadline violations and (2) returns once
            the queue is empty.
    """
    while True:
        job = yield queue.get()
        queue.last_returned_job = job

        _queue_str = "queue now empty" if len(queue.items) == 0 else "queue now: " + ", ".join([str(job.id) for job in queue.items])
        if simulated:
            logger.debug(f"- {simtime_to_datetime(env.now)}: Starting job {job.id} ({_queue_str}) ...")
        else:
            logger.info(f"{simtime_to_datetime(env.now)}: [EXECUTION] Starting job {job.id} ({_queue_str}) ...")

        while not job.finished:
            now = simtime_to_datetime(env.now)
            u_freep, u_free = resource_manager.u_now(now)
            if not simulated and _expected_deadline_violation(now, job, resource_manager):
                u_freep = u_free

            if u_freep > 0:
                end_time = round_up_dt(now)  # after this new mch capacity is available
                if simulated:
                    logger.debug(f"-* {simtime_to_datetime(env.now)}: Running job {job.id} ({job.size_remaining:.2f} "
                                 f"mch left) for {u_freep:.2f} mch until {end_time}")
                else:
                    logger.debug(f"{simtime_to_datetime(env.now)}: [EXECUTION] Running job {job.id} "
                                 f"({job.size_remaining:.2f} mch left) for {u_freep:.2f} mch until {end_time}")
                size_excess, duration = job.run_for(u_freep, start_time=now, end_time=end_time)

                u_used = u_freep - size_excess
                resource_manager.notify_usage(now, u_used)

                # progress until the end of the job
                yield env.timeout(timedelta_to_simtimedelta(duration))
            else:
                next_step_datetime = round_up_dt(now)
                if not simulated:
                    job.last_processed = now
                    logger.debug(f"{simtime_to_datetime(env.now)}: [EXECUTION] Idling job {job.id} "
                                 f"({job.size_remaining:.2f} mch left) until {next_step_datetime}")

                # There are no more mch available for this timestep so we progress until the next timestep
                yield env.timeout(timedelta_to_simtimedelta(next_step_datetime - now))

        s = " successfully" if job.finished_before_deadline else " - DEADLINE MISS!"
        if simulated:
            logger.debug(f"- {simtime_to_datetime(env.now)}: Finished job {job.id}{s} (deadline: {job.deadline})")
        else:
            logger.info(f"{simtime_to_datetime(env.now)}: [EXECUTION] Finished job {job.id}{s} (deadline: {job.deadline})")

        # If we run a fake scenario we exit once the queue is empty
        if simulated and len(queue.items) == 0:
            return


def _expected_deadline_violation(now: datetime, job: Job, resource_manager: ResourceManager) -> bool:
    """Returns True if job is expected to violate its deadline using freep capacity only, False otherwise"""
    if job.deadline < now:
        logger.debug(f"{now}: [EXECUTION] [!] Using grid power for job {job.id}! Deadline {job.deadline} is in the past.")
        return True
    u_freep, _ = resource_manager.u_in_daterange(start=now, end=job.deadline)
    if u_freep < job.size_remaining:
        logger.debug(f"{now}: [EXECUTION] [!] Using grid power for job {job.id}! {job.size_remaining:.2f} mch "
                     f"remaining but only {u_freep:.2f} mch available until {job.deadline}.")
        return True
    return False
