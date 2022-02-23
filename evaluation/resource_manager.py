from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd

from util import round_up_dt, round_down_dt
from config import TIME_STEP


class ResourceManager:

    def __init__(self, u: pd.DataFrame):
        """Manages the resource utilization of the node.

        U is a DataFrame where each row (time step) has the following information:
        - u_freep: Free capacity which can be powered by renewable excess energy
        - u_free: Free capacity
        - u_reep: Capacity which can be powered by renewable excess energy
        - u_used: Actual capacity usage. Initially all 0, will be filled during the simulation
        """
        self.u = u

    def u_now(self, now: datetime) -> Tuple[float, float]:
        """Returns the freep (u_freep) and free capacity (u_free) available at the provided time step."""
        u_freep, u_free, _, u_used = self.u.loc[round_up_dt(now)]
        return u_freep - u_used, u_free - u_used

    def u_in_daterange(self, start: datetime, end: datetime) -> Tuple[float, float]:
        """Returns the freep (u_freep) and free capacity (u_free) available during the provided time range."""
        df = self.u.loc[round_up_dt(start):round_up_dt(end)]
        df = df[["u_freep", "u_free"]].subtract(df["u_used"], axis="index")
        last_fraction = (end - round_down_dt(end)) / timedelta(minutes=TIME_STEP)
        if len(df) > 1:
            df.iloc[-1] *= last_fraction
            return df.sum(axis=0)
        else:
            return 0, 0

    def u_used(self, now: datetime):
        """Returns the already used capacity at the provided time step."""
        _, _, _, u_used = self.u.loc[round_up_dt(now)]
        return u_used

    def notify_usage(self, now: datetime, u_used: float):
        """Adds the provided capacity usage to the `u_used` column at the provided time step."""
        self.u.loc[round_up_dt(now), "u_used"] += u_used
        # sanity check
        assert self.u_now(now)[1] >= 0, f"Cannot use additional {u_used} mch at {round_up_dt(now)}!"


def u_real(u_df: pd.DataFrame, policy: str, start_date: datetime):
    """Converts a load DataFrame into the U DataFrame as described in the ResourceManager."""
    mch_list = []
    for date, df in u_df.groupby(level=0):
        # unnecessary but correct
        if date < start_date:
            continue
        u_free = df.iloc[0]["u_free"]
        u_reep = df.iloc[0]["u_reep"]
        # If the admission optimizes for max capacity we make all mch available
        if policy == "Baseline 1":
            u_freep = u_free
        else:
            u_freep = min(u_free, u_reep)
        mch_list.append((date + timedelta(minutes=TIME_STEP), u_freep, u_free, u_reep, 0))
    forecasts = pd.DataFrame(mch_list, columns=["datetime", "u_freep", "u_free", "u_reep", "u_used"])
    forecasts = forecasts.set_index("datetime")
    return forecasts


def u_pred(u_df: pd.DataFrame, policy: str, start_date: datetime, initial_u_used: float):
    """Converts a load DataFrame into the U DataFrame as described in the ResourceManager.

    The resulting values represent forecasts of future freep/free/reep capacity.
    This function is used within cucumber admission control, i.e. the internal simulations of future system states.

    If "initial_u_used" is not zero, we are starting the simulation in the middle of two time steps and have
    already consumed some of the available capacity.
    """
    if policy in ["Baseline 1", "Baseline 2"]:  # baselines use perfect forecasts
        u_free = "u_free"
        u_reep = "u_reep"
    else:
        u_free = "u_free_pred"
        if policy == "Expected":
            u_reep = "u_reep_pred_expected"
        elif policy == "Conservative":
            u_reep = "u_reep_pred_conservative"
        elif policy == "Optimistic":
            u_reep = "u_reep_pred_optimistic"
        else:
            raise ValueError("Unknown policy")

    # select "u_free" and "u_reep" slices
    forecasts = u_df.loc[start_date, (u_free, u_reep)]
    forecasts.columns = ["u_free", "u_reep"]

    # If the admission optimizes for max capacity we make all free capacity available
    if policy == "Baseline 1":
        forecasts["u_freep"] = forecasts["u_free"]
    else:
        forecasts["u_freep"] = forecasts[["u_free", "u_reep"]].min(axis=1)

    forecasts["u_used"] = 0
    forecasts.loc[forecasts.index[0], "u_used"] = initial_u_used
    return forecasts[["u_freep", "u_free", "u_reep", "u_used"]]
