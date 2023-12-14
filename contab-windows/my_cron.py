import subprocess
import time
from datetime import datetime
import traceback
import re
import os
import sys
import logging
import psutil


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, stream=sys.stdout)
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULT_CONFIG = os.path.join(CURRENT_DIR, "crontab.txt")
WAIT_PROCESS_TIME = 0.2



def parse_cron_line(cron_line):
    '''
    解析单行crontab记录
    '''
    parts = cron_line.strip().split()
    if len(parts) < 6:
        return None, None
    schedule, command = parts[:5], ' '.join(parts[5:])
    
    # 将星号替换成数字范围
    mins = schedule[0].replace('*', '0-59')
    hours = schedule[1].replace('*', '0-23')
    days = schedule[2].replace('*', '1-31')
    months = schedule[3].replace('*', '1-12')
    weekdays = schedule[4].replace('*', '0-6')

    cron_schedule = {
        'mins': mins,
        'hours': hours,
        'days': days,
        'months': months,
        'weekdays': weekdays
    }
    
    return cron_schedule, command



def is_time_to_run(cron_schedule):
    '''
    判断当前时间是否符合crontab时间设置
    '''
    if not cron_schedule:
        return False
    now = datetime.now()
    if not is_within_range(now.minute, cron_schedule['mins']):
        return False
    if not is_within_range(now.hour, cron_schedule['hours']):
        return False
    if not is_within_range(now.day, cron_schedule['days']):
        return False
    if not is_within_range(now.month, cron_schedule['months']):
        return False
    if not is_within_range(now.weekday(), cron_schedule['weekdays']):
        return False
    return True


def is_within_range(current_value, cron_field):
    '''
    检查当前值是否在crontab字段范围内
    TODO: 1-10/2 会被识别为 */2
    不识别LW等特殊字符
    '''
    for part in cron_field.split(','):
        if '/' in part:
            denominator = int(part.split('/')[1])
            if current_value % denominator == 0:
                return True
        elif '-' in part:
            start, end = map(int, part.split('-'))
            if start <= current_value <= end:
                return True
        elif int(part) == current_value:
            return True
    return False


def run_command(command):
    '''
    执行命令, 启动进程
    '''
    try:
        logging.info(f"CMD = {command}")
        # 启动进程
        proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # 等待后看是否直接执行失败
        time.sleep(WAIT_PROCESS_TIME)
        proc.poll()
        # 执行完毕, 且执行失败
        if proc.returncode is not None and proc.returncode != 0:
            logging.info(f"proc.returncode={proc.returncode}")
            logging.info(proc.stdout.read().decode('gbk').strip())
        logging.info(f"start CMD finish")
    except Exception:
        exc = traceback.format_exc()
        logging.error(exc)


def main_loop(config_file):
    while True:
        try:
            # 读取配置文件
            conf_lines = []
            with open(config_file, encoding="utf-8") as f:
                conf_lines = f.readlines()
            # 解析配置
            for line in conf_lines:
                cron_schedule, command = parse_cron_line(line)
                if cron_schedule is None:
                    continue
                # 判断是否应该启动
                if is_time_to_run(cron_schedule):
                    # 启动命令
                    run_command(command)
        except Exception:
            exc = traceback.format_exc()
            logging.error(exc)
        time.sleep(60)


def is_process_running():
    '''
    判断是否已有进程启动
    '''
    # 获取当前进程及父进程列表
    my_pid = os.getpid()
    my_pid_list = set()
    try:
        the_pid = my_pid
        while True:
            my_pid_list.add(the_pid)
            the_pid = psutil.Process(the_pid).ppid()
            if the_pid in my_pid_list:
                break
            if not the_pid:
                break
    except BaseException:
        pass
    # 查看是否有其他进程执行中
    process_count = 0
    py_file_name = os.path.basename(__file__)
    for one_pid in psutil.pids():
        if one_pid in my_pid_list:
            continue
        # 获取进程详情
        try:
            p = psutil.Process(one_pid)
            p_cmd = p.cmdline()
            cmd_line = ' '.join(p_cmd).lower()
            if py_file_name.lower() in cmd_line and 'python' in cmd_line:
                logging.info(f"pid={one_pid}, cmd={cmd_line}")
                process_count += 1
        except BaseException:
            continue
    return process_count


if __name__ == "__main__":
    if is_process_running():
        logging.error("Another Process Running")
        exit(1)
    config_path = DEFAULT_CONFIG
    if len(sys.argv) == 2:
        config_path = sys.argv[1]
    
    # 校验配置文件路径
    if not os.path.exists(config_path):
        raise ValueError(f"Invalid crontab file: {config_path}")
    
    main_loop(config_path)
