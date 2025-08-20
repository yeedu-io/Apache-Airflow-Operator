from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import Timetable
from yeedu.plugins.quartz_timetable import QuartzTimetable


class QuartzTimetablePlugin(AirflowPlugin):
    name = "quartz_timetable_plugin"
    timetables = [QuartzTimetable]
