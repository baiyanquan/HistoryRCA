import time
import pytz
import datetime


class TimeHandler:
    def __init__(self):
        pass

    @staticmethod
    def utc_str_to_timestamp(utc_time_str, utc_format='%Y-%m-%d %H:%M:%S'):
        """
        将utc时间字符串转换为时间戳
        :param utc_time_str: 表示utc时间的字符串
        :param utc_format: 时间字符串的格式，默认为'%Y-%m-%d %H:%M:%S'
        :return: int，即转换后的时间戳
        """
        local_tz = pytz.timezone('Asia/Shanghai')
        utc_dt = datetime.datetime.strptime(utc_time_str, utc_format)
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
        return int(time.mktime(local_dt.timetuple()))

    @staticmethod
    def datetime_timestamp(dt):
        time.strptime(dt, '%Y-%m-%d %H:%M:%S')
        s = time.mktime(time.strptime(dt, '%Y-%m-%d %H:%M:%S'))
        return int(s)

    @staticmethod
    def timestamp_datetime(ts):
        dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        return dt

    @staticmethod
    def get_now_datetime():
        now_time = time.localtime(time.time())
        result = {"standard_format": time.strftime('%Y-%m-%d %H:%M:%S', now_time),
                  "simplified_format": time.strftime('%Y%m%d_%H%M%S', now_time),
                  "timestamp": TimeHandler.datetime_timestamp(time.strftime('%Y-%m-%d %H:%M:%S', now_time))}
        return result
