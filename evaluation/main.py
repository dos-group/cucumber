import logging

import pandas as pd
import simpy

from job import Job, Queue
from processes.execution import execution_process
from processes.admission_control import admission_control_process
from resource_manager import ResourceManager, u_real
from util import datetime_to_simtime
from config import START_DATE, FINAL_DATE, END_DATE, LOG_LEVEL


logging.basicConfig(format="")
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)


def _load_jobs(dataset: str):
    with open(f"../data/requests_{dataset}.csv", "r") as csvfile:
        df = pd.read_csv(csvfile, parse_dates=True, index_col=0)
    jobs = []
    for i, (arrive_time, row) in enumerate(df.iterrows()):
        size = row["size"]
        deadline = pd.to_datetime(row["deadline"])
        if START_DATE < arrive_time < END_DATE and deadline < FINAL_DATE:
            jobs.append(Job(i, arrive_time=arrive_time, size=size, deadline=deadline))
    return jobs


def load_u_df(scenario, solar_site):
    """Loads the capacity forecast DataFrame U.

    Each line represents a forecast result and is indexed by a multi-index where level 1 is the datetime of the
    forecast request and level 2 the datetime of the forecast result.

    For each forecast result the following information is available:
    - u_free: Actual free capacity
    - u_free_pred: Forecasted free capacity
    - u_reep: Actual capacity which can be powered by renewable excess energy
    - u_reep_pred_expected: Forecasted capacity which can be powered by renewable excess energy (expected case)
    - u_reep_pred_conservative: Forecasted capacity which can be powered by renewable excess energy (conservative case)
    - u_reep_pred_optimistic: Forecasted capacity which can be powered by renewable excess energy (optimistic case)
    """
    df = pd.read_csv(f"../data/u_{scenario}_{solar_site}.csv", parse_dates=True, index_col=[0, 1])
    df = df.loc[START_DATE:FINAL_DATE]
    return df


def run_experiment(scenario, solar_site, policy):
    """Execute an experiment by simulating a scenario at a solar site using a specific policy.

    Returns two DataFrames:
    - The first containing statistics over each job (arrive time, finish time, deadline violation, ...)
    - The second containing statistics over the computational capacity usage (excess/grid energy usage)
    """
    # Load data_old
    u_df = load_u_df(scenario=scenario, solar_site=solar_site)
    incoming_jobs = _load_jobs(scenario)

    # Initialize and run simulation
    forecasts = u_real(u_df, policy=policy, start_date=START_DATE)
    resource_manager = ResourceManager(forecasts)

    env = simpy.Environment()
    queue = Queue(env)
    env.process(execution_process(env=env, queue=queue, resource_manager=resource_manager))
    env.process(admission_control_process(env=env, policy=policy, incoming_jobs=incoming_jobs, queue=queue,
                                          resource_manager=resource_manager, u_df=u_df))
    env.run(until=datetime_to_simtime(END_DATE))

    # Save job statistics
    jobs = pd.DataFrame([j.to_record() for j in incoming_jobs],
                        columns=["id", "status", "mch", "arrive time", "finish time", "deadline"])
    jobs = jobs.set_index("id")
    jobs.to_csv(f"../results/jobs_{scenario}_{solar_site}_{policy}.csv")

    # Save capacity dataframe
    resource_manager.u.to_csv(f"../results/u_{scenario}_{solar_site}_{policy}.csv")

    return jobs, resource_manager.u


def main():
    scenarios = [
        "alibaba",
        "nyctaxi",
    ]
    solar_sites = [
        "berlin",
        "cdmx",
        "capet",
    ]
    policies = [
        "Baseline 1",  # Perfect forecasts for free capacity
        "Baseline 2",  # Perfect forecasts for freep capacity
        "Naive",  # No forecasts
        "Expected",  # Real forecasts for freep capacity
        "Conservative",  # Real forecasts for freep capacity (conservative)
        "Optimistic",  # Real forecasts for freep capacity (optimistic)
    ]
    for scenario in scenarios:
        print(f"\n### SCENARIO: {scenario}")
        for solar_site in solar_sites:
            print(f"\n### SOLAR SITE: {solar_site}")
            for policy in policies:
                logger.info(f"\n### POLICY: {policy}")
                jobs, u = run_experiment(scenario, solar_site, policy)

                # Report summary
                diff = u["u_reep"] - u["u_used"]
                grid_mch = -diff[diff < 0]
                print(f"{policy}: Success/Rejected/Miss: {jobs[jobs['status'] == 'SUCCESS'].shape[0]}/"
                      f"{jobs[jobs['status'] == 'REJECTED'].shape[0]}/{jobs[jobs['status'] == 'MISS'].shape[0]} at "
                      f"{grid_mch.sum() / 6:.2f} mch powered by grid energy")


if __name__ == "__main__":
    main()
