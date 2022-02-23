from datetime import datetime
from typing import Union

import pandas as pd
import simpy

from util import round_down_dt


class Job:
    def __init__(self, id_, arrive_time: Union[datetime, str], size: float, deadline: Union[datetime, str]):
        """A delay-tolerant workload.

        Args:
            id_: ID of the workload
            arrive_time: Datetime the job was issued
            size: Size of the workload in millicore-hours
            deadline: Deadline of workload
        """
        self.id = id_
        self.arrive_time = pd.to_datetime(arrive_time)
        self.size = size
        self.size_remaining = size
        self.deadline = pd.to_datetime(deadline)
        self.accepted = None
        self.last_processed = None
        assert self.arrive_time < self.deadline

    def __repr__(self):
        finish_time_str = f", finish_time={self.finish_time}" if self.finished else ""
        return f"Job({self.id} [{self.status}], arrive_time={self.arrive_time}, " \
               f"size={self.size_remaining:.2f}/{self.size}, deadline={self.deadline}{finish_time_str})"

    def __lt__(self, other):  # Needs to be comparable for priority queue
        return self.deadline < other.deadline

    @property
    def finished(self):
        return self.size_remaining == 0

    @property
    def finished_before_deadline(self):
        if not self.finished:
            raise ValueError(f"Job {self.id} not yet finished ({self.size_remaining} mch remaining)")
        return self.finish_time <= self.deadline

    @property
    def finish_time(self):
        if self.finished:
            return self.last_processed
        else:
            return None

    @property
    def status(self):
        if self.accepted is False:
            return "REJECTED"
        elif not self.finished:
            return "RUNNING"
        elif self.finished_before_deadline:
            return "SUCCESS"
        else:
            return "MISS"

    def run_for(self, size: float, start_time: datetime, end_time: datetime):
        """Executes the job for <mch> millicore-hours or until <end_time> is reached.

        Returns the remaining millicore-hours (0 if job is finished) and the duration the job was running.
        """
        assert size > 0  # sanity check
        size_remaining = self.size_remaining - size
        if size_remaining > 0:
            size_excess = 0
            duration = end_time - start_time
            self.size_remaining = size_remaining
        else:
            size_excess = -size_remaining
            used_capacity_fraction = self.size_remaining / size
            duration = (end_time - start_time) * used_capacity_fraction
            duration = duration.round('s')
            self.size_remaining = 0
        self.last_processed = start_time + duration
        return size_excess, duration

    def to_record(self):
        finish_time = self.finish_time if self.finished else "-"
        return self.id, self.status, self.size, self.arrive_time, finish_time, self.deadline


class Queue(simpy.PriorityStore):

    def __init__(self, env: simpy.Environment):
        super().__init__(env)
        self.last_returned_job = None

    def active_job(self, now):
        """Returns the last returned job if its not already finished."""
        if self.last_returned_job is None:
            # Previous job finished before time step
            return None
        if self.last_returned_job.finished and self.last_returned_job.last_processed < round_down_dt(now):
            # Previous job finished before current time step
            return None
        return self.last_returned_job
