from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from cron_descriptor import get_description

from calendar import monthrange

import pendulum
from pendulum import DateTime
from airflow.timetables.base import Timetable, DagRunInfo, DataInterval, TimeRestriction


LOGGER = logging.getLogger(__name__)

@dataclass(frozen=True)
class QuartzTimetable(Timetable):
    
    cron_expression: str
    tz: str = "Asia/Kolkata"

    # ------------------------------------------------------------------
    # Airflow Timetable interface methods
    # ------------------------------------------------------------------
    @property
    def summary(self) -> str:
        return self.cron_expression
    
    @property
    def description(self) -> str:
        return get_description(self.cron_expression)
    
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        
        anchor = run_after.in_timezone(self.tz)
        # Look for the next valid start after one second before the trigger.
        # This allows a manual trigger that lands exactly on a scheduled run to
        # include the current moment.
        try:
            search_time = anchor.subtract(seconds=1)
        except Exception:
            search_time = anchor - pendulum.duration(seconds=1)
        next_start = self._next_valid_start(search_time)
        # If there is no next start, or it is None, fall back to the trigger time.
        start = next_start or anchor
        return DataInterval(start=start, end=start.add(minutes=1))

    def next_dagrun_info(self, *, last_automated_data_interval: Optional[DataInterval], restriction: TimeRestriction) -> Optional[DagRunInfo]:
        tz = self.tz
        now = pendulum.now(tz)
        
        # If last_automated_data_interval exists, continue from there; otherwise, use current time
        anchor = last_automated_data_interval.end.in_timezone(tz) if last_automated_data_interval else now

        # If catchup is False, we won't backfill or run the missed jobs.
        if not restriction.catchup and anchor < now:
            anchor = now  # Adjust anchor to the current time if catchup is off

        next_start = self._next_valid_start(anchor)
        if next_start is None:
            return None

        # Ensure next start does not exceed the latest allowed time
        if restriction.latest and next_start > restriction.latest.in_timezone(tz):
            return None

        first_end = next_start.add(seconds=1)
        return DagRunInfo.interval(start=next_start, end=first_end)

    def serialize(self) -> Dict[str, Any]:
        """Serialize the timetable's configuration for persistence."""
        return {
            "cron_expression": self.cron_expression,
            "tz": self.tz,
        }

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> "QuartzTimetable":
        tz = data.get("tz", "Asia/Kolkata")
        return cls(
            cron_expression=data["cron_expression"],
            tz=tz,
        )

    # ------------------------------------------------------------------
    # Internal helper: Compute next run time after a given datetime
    # ------------------------------------------------------------------
    def _next_valid_start(self, dt: DateTime) -> Optional[DateTime]:
        """
        Compute the next valid start time strictly after ``dt`` using the cron
        expression. Supports Quartz syntax, including extended day-of-week and
        day-of-month semantics. Returns ``None`` if no next trigger exists.
        """
        # Break the expression into exactly 7 parts (sec min hour dom mon dow year)
        parts = self._ensure_quartz_fields(self.cron_expression.split())
        if not parts:
            return None

        # ------------------------------------------------------------------
        # Field parsing helpers
        # ------------------------------------------------------------------
        def parse_field(expr: str, min_val: int, max_val: int, names_map: Optional[Dict[str, int]] = None) -> Optional[List[int]]:
            """Parse a cron field into a sorted list of integers or ``None`` for wildcard.

            Supports comma-separated values, ranges, and step expressions. Named
            values are translated using ``names_map`` when provided.
            """
            expr = expr.strip().upper()
            if not expr or expr in {"*", "?"}:
                return None
            values: Set[int] = set()
            for segment in expr.split(','):
                segment = segment.strip()
                if not segment:
                    continue
                # Step value (e.g. "*/5", "0/10", "5-20/5")
                if '/' in segment:
                    base, step_str = segment.split('/', 1)
                    try:
                        step = int(step_str)
                    except Exception:
                        continue
                    # Determine start and end for the stepping range
                    if base in ('*', ''):
                        start = min_val
                        end = max_val
                    elif '-' in base:
                        try:
                            start_str, end_str = base.split('-', 1)
                            start = int(names_map.get(start_str, start_str)) if names_map else int(start_str)
                            end = int(names_map.get(end_str, end_str)) if names_map else int(end_str)
                        except Exception:
                            continue
                    else:
                        try:
                            start = int(names_map.get(base, base)) if names_map else int(base)
                        except Exception:
                            continue
                        end = max_val
                    # Clamp start and end to valid bounds
                    start = max(min_val, start)
                    end = min(max_val, end)
                    # Generate stepped values within bounds
                    for v in range(start, end + 1, step):
                        if min_val <= v <= max_val:
                            values.add(v)
                    continue
                # Range expression (e.g. "9-17", "MON-FRI")
                if '-' in segment:
                    a_str, b_str = segment.split('-', 1)
                    a_str = a_str.strip()
                    b_str = b_str.strip()
                    try:
                        a_val = int(names_map.get(a_str, a_str)) if names_map else int(a_str)
                        b_val = int(names_map.get(b_str, b_str)) if names_map else int(b_str)
                    except Exception:
                        continue
                    # If the range wraps around, include the wrap-around portion
                    if a_val <= b_val:
                        seq = range(a_val, b_val + 1)
                    else:
                        seq = list(range(a_val, max_val + 1)) + list(range(min_val, b_val + 1))
                    for v in seq:
                        if min_val <= v <= max_val:
                            values.add(v)
                    continue
                # Single numeric or named value
                try:
                    val_str = names_map.get(segment, segment) if names_map else segment
                    v = int(val_str)
                except Exception:
                    continue
                if min_val <= v <= max_val:
                    values.add(v)
            return sorted(values)

        # Month names mapping (JAN=1 … DEC=12)
        month_names = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
        }
        # Weekday names mapping to Quartz numeric values (Sun=1 … Sat=7)
        dow_names = {
            'SUN': 1, 'MON': 2, 'TUE': 3, 'WED': 4, 'THU': 5, 'FRI': 6, 'SAT': 7,
        }

        # Parse simple time fields
        sec_spec = parse_field(parts[0], 0, 59)
        min_spec = parse_field(parts[1], 0, 59)
        hr_spec = parse_field(parts[2], 0, 23)
        # Day-of-month parsing: handle L, LW, step, lists and ranges
        dom_field = parts[3].strip().upper()
        dom_spec: Dict[str, Union[bool, Tuple[int, int], List[int]]] = {}
        if dom_field in ('*', '?', ''):
            dom_spec['wildcard'] = True
        elif dom_field == 'L':
            dom_spec['last_day'] = True
        elif dom_field == 'LW':
            dom_spec['last_weekday'] = True
        elif '/' in dom_field:
            try:
                start_str, step_str = dom_field.split('/', 1)
                start = int(start_str)
                step = int(step_str)
                # Clamp the start to 1 for day-of-month stepping, since day 0 does not exist
                if start < 1:
                    start = 1
                dom_spec['step'] = (start, step)
            except Exception:
                dom_spec['wildcard'] = True
        else:
            days: Set[int] = set()
            for seg in dom_field.split(','):
                seg = seg.strip()
                if not seg:
                    continue
                if '-' in seg:
                    try:
                        a_str, b_str = seg.split('-', 1)
                        a = int(a_str)
                        b = int(b_str)
                        for d in range(a, b + 1):
                            days.add(d)
                    except Exception:
                        continue
                else:
                    try:
                        d = int(seg)
                        days.add(d)
                    except Exception:
                        continue
            if days:
                dom_spec['days'] = sorted(days)
            else:
                dom_spec['wildcard'] = True
        # Month parsing
        mon_spec = parse_field(parts[4], 1, 12, month_names)
        # Day-of-week parsing: supports nth (#), last (L) and simple lists/ranges
        dow_field = parts[5].strip().upper()
        dow_spec: Dict[str, Union[bool, Dict[int, List[int]], Set[int]]] = {}
        if dow_field in ('*', '?', ''):
            dow_spec['any'] = True
        else:
            for seg in dow_field.split(','):
                seg = seg.strip()
                if not seg:
                    continue
                # Nth weekday (e.g. MON#2 or 6#3)
                if '#' in seg:
                    prefix, nth_str = seg.split('#', 1)
                    try:
                        nth = int(nth_str)
                    except Exception:
                        continue
                    # Determine Quartz numeric value
                    try:
                        qval = int(prefix)
                    except Exception:
                        qval = dow_names.get(prefix, None)
                    if qval is None:
                        continue
                    # Convert to Python weekday (Mon=0 … Sun=6)
                    py = (qval + 5) % 7
                    dow_spec.setdefault('nth', {}).setdefault(py, []).append(nth)
                    continue
                    
                # Last weekday (e.g. 5L or THUL)
                if seg.endswith('L') and seg != 'L':
                    prefix = seg[:-1]
                    try:
                        qval = int(prefix)
                    except Exception:
                        qval = dow_names.get(prefix, None)
                    if qval is None:
                        continue
                    py = (qval + 5) % 7
                    dow_spec.setdefault('last', set()).add(py)
                    continue
                # Range (e.g. MON-FRI or 2-4)
                if '-' in seg:
                    a_str, b_str = seg.split('-', 1)
                    try:
                        q_a = int(a_str)
                    except Exception:
                        q_a = dow_names.get(a_str, None)
                    try:
                        q_b = int(b_str)
                    except Exception:
                        q_b = dow_names.get(b_str, None)
                    if q_a is None or q_b is None:
                        continue
                    py_a = (q_a + 5) % 7
                    py_b = (q_b + 5) % 7
                    if py_a <= py_b:
                        seq = range(py_a, py_b + 1)
                    else:
                        seq = list(range(py_a, 7)) + list(range(0, py_b + 1))
                    for d in seq:
                        dow_spec.setdefault('weekdays', set()).add(d)
                    continue
                # Single day (name or number)
                try:
                    q = int(seg)
                except Exception:
                    q = dow_names.get(seg, None)
                if q is None:
                    continue
                py = (q + 5) % 7
                dow_spec.setdefault('weekdays', set()).add(py)
        # Year parsing
        year_field = parts[6].strip()
        year_spec: Optional[List[int]] = None
        if year_field and year_field not in ('*', '?'):
            try:
                year_spec = parse_field(year_field, 1970, 2199)
            except Exception:
                year_spec = None
        # Expand None specs into full ranges
        sec_list = sec_spec if sec_spec is not None else list(range(0, 60))
        min_list = min_spec if min_spec is not None else list(range(0, 60))
        hr_list = hr_spec if hr_spec is not None else list(range(0, 24))
        # Ensure sorted and unique
        sec_list = sorted(set(sec_list))
        min_list = sorted(set(min_list))
        hr_list = sorted(set(hr_list))

        # Helper: compute candidate days for a given month based on DOM and DOW specs
        def compute_candidate_days(year: int, month: int) -> List[int]:
            days_in_month = monthrange(year, month)[1]
            # Build day‑of‑month candidate set
            dom_candidates: Optional[Set[int]] = None
            if 'last_weekday' in dom_spec:
                last_day = pendulum.datetime(year, month, 1, tz=self.tz).end_of('month')
                wd = last_day.weekday()  # 0 = Monday, 6 = Sunday
                if wd == 5:  # Saturday
                    last_day = last_day.subtract(days=1)
                elif wd == 6:  # Sunday
                    last_day = last_day.subtract(days=2)
                dom_candidates = {last_day.day}
            elif 'last_day' in dom_spec:
                dom_candidates = {days_in_month}
            elif 'step' in dom_spec:
                start_dom, step_dom = dom_spec['step']  # type: ignore[assignment]
                candidates: Set[int] = set()
                d = start_dom
                while d <= days_in_month:
                    if d >= 1:
                        candidates.add(d)
                    d += step_dom
                dom_candidates = candidates
            elif 'days' in dom_spec:
                candidates: Set[int] = {d for d in dom_spec['days'] if 1 <= d <= days_in_month}  # type: ignore[assignment]
                dom_candidates = candidates
            elif 'wildcard' in dom_spec:
                dom_candidates = None
            # Build day‑of‑week candidate set by scanning each day rather than using monthcalendar
            dow_candidates: Optional[Set[int]] = None
            if 'any' not in dow_spec:
                candidates: Set[int] = set()
                # nth weekday (e.g. MON#2)
                if 'nth' in dow_spec:
                    for py_wd, nth_list in dow_spec['nth'].items():  # type: ignore[assignment]
                        for nth in nth_list:
                            count = 0
                            matched_day = None
                            for d in range(1, days_in_month + 1):
                                if pendulum.datetime(year, month, d, tz=self.tz).weekday() == py_wd:
                                    count += 1
                                    if count == nth:
                                        matched_day = d
                                        break
                            if matched_day is not None:
                                candidates.add(matched_day)
                # last weekday (e.g. 5L)
                if 'last' in dow_spec:
                    for py_wd in dow_spec['last']:  # type: ignore[assignment]
                        for d in range(days_in_month, 0, -1):
                            if pendulum.datetime(year, month, d, tz=self.tz).weekday() == py_wd:
                                candidates.add(d)
                                break
                # simple weekday sets (e.g. MON-FRI)
                if 'weekdays' in dow_spec:
                    for d in range(1, days_in_month + 1):
                        if pendulum.datetime(year, month, d, tz=self.tz).weekday() in dow_spec['weekdays']:  # type: ignore[assignment]
                            candidates.add(d)
                dow_candidates = candidates
            # Combine DOM and DOW constraints
            if dom_candidates is None and dow_candidates is None:
                return list(range(1, days_in_month + 1))
            if dom_candidates is not None and dow_candidates is None:
                return sorted(dom_candidates)
            if dom_candidates is None and dow_candidates is not None:
                return sorted(dow_candidates)
            return sorted(set(dom_candidates) & set(dow_candidates))
        # Starting point: strictly after dt
        start_dt = dt.add(seconds=1)
        start_year = start_dt.year
        # Define search horizon for years
        horizon_year = (max(year_spec) if year_spec else start_year + 100)
        for year in range(start_year, horizon_year + 1):
            if year_spec is not None and year not in year_spec:
                continue
            # Allowed months: specified or all
            months = mon_spec if mon_spec is not None else list(range(1, 13))
            for month in months:
                if year == start_year and month < start_dt.month:
                    continue
                candidate_days = compute_candidate_days(year, month)
                if not candidate_days:
                    continue
                for day in candidate_days:
                    if (year == start_year and month == start_dt.month and day < start_dt.day):
                        continue
                    for hour in hr_list:
                        if (year == start_year and month == start_dt.month and day == start_dt.day and hour < start_dt.hour):
                            continue
                        for minute in min_list:
                            if (year == start_year and month == start_dt.month and day == start_dt.day and
                                    hour == start_dt.hour and minute < start_dt.minute):
                                continue
                            for second in sec_list:
                                if (year == start_year and month == start_dt.month and day == start_dt.day and
                                        hour == start_dt.hour and minute == start_dt.minute and second < start_dt.second):
                                    continue
                                candidate = pendulum.datetime(year, month, day, hour, minute, second, tz=self.tz)
                                if candidate > dt:
                                    return candidate
        # No valid candidate found
        return None

    # ------------------------------------------------------------------
    # Helper: Ensure cron fields list has 7 items (sec, min, hour, dom, mon, dow, year)
    # ------------------------------------------------------------------
    def _ensure_quartz_fields(self, parts: List[str]) -> List[str]:
        """Ensure the cron expression has exactly 7 fields by appending a wildcard year."""
        if len(parts) == 6:
            return parts + ['*']
        return parts