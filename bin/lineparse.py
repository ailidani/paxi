import os
import datetime
import time
location = "D:\\Programs\\Go\\goworkspace\\src\\paxi\\bin"
epoch = datetime.datetime.utcfromtimestamp(0)

server_logs = []


def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0


def read_logger_and_form_tuple(name):
    file = open(os.path.join(location, name), "r")
    lines = file.readlines()
    file.close()

    for line in lines:
        if "[TEST]" in line:
            l = line.split()
            # convert timestamp YYYY/MM/DD HH:MM:SS.F to UNIX timestamp or epoch time.
            timestamp = l[1] + " " + l[2]
            timestamp = datetime.datetime.strptime(
                timestamp, "%Y/%m/%d %H:%M:%S.%f")
            timestamp = unix_time_millis(timestamp)

            state = l[4]

            mytuple = (timestamp, state)
            # create a list of tuples
            server_logs.append(mytuple)


def print_logs(logfile):
    for log in logfile:
        print(log)


read_logger_and_form_tuple("server.exe.16600.log")
read_logger_and_form_tuple("server.exe.18472.log")
read_logger_and_form_tuple("server.exe.18816.log")
read_logger_and_form_tuple("server.exe.20424.log")
read_logger_and_form_tuple("server.exe.21796.log")

print_logs(server_logs)
print("======================================")
final_logs = sorted(server_logs)

print_logs(final_logs)


# server_log2 = read_logger_form_array("server.exe.9760.log")
# server_log3 = read_logger_form_array("server.exe.10564.log")
# server_log4 = read_logger_form_array("server.exe.11216.log")
# server_log5 = read_logger_form_array("server.exe.19740.log")

# for log in server_log1:
#     loglines = log.split()
