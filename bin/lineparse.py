import os
import datetime
import time
import sys

location = "D:\\Programs\\Go\\goworkspace\\src\\paxi\\bin"
epoch = datetime.datetime.utcfromtimestamp(0)

server_logs = []


class LogLine:
	# ordered and sorted by timestamp
	timestamp: datetime.datetime
	location: str
	line: str
	state: dict

	def __init__(self, timestamp, location, line, state):
		self.timestamp = timestamp
		self.location = location
		self.line = line
		self.state = state

	def __repr__(self):
		return "{timestamp: ", self.timestamp, "location:", self.location, "line: ", self.line, "state: ", self.state, " }"

	def __str__(self):
		return "LogLine(timestamp="+str(self.timestamp)+" location="+self.location+" line="+self.line+" state= "+str(self.state) + ")"


class LogEvent:
	# timestamp of the event
	timestamp: datetime.datetime
	# file nae and line number
	location: str
	line: str
	# TLA+ action of the event
	action: str
	# ID of the replica
	server_id: str
	# slots
	slot: int
	# ballots
	ballot: int

	def __init__(self, timestamp, location, line, state):
		self.timestamp = timestamp
		self.location = location
		self.line = line
		self.action = state["action"]
		self.server_id = state.get("ID")
		self.slot = int(state.get("slot"))
		self.ballot = int(state.get("ballot").split(".")[2])

	def __str__(self):
		return "LogEvent(timestamp="+str(self.timestamp)+", location="+self.location+", line="+self.line+", action="+self.action+", server_id="+self.server_id+", slot="+str(self.slot)+", ballot="+str(self.ballot)+")"


def unix_time_millis(dt):
	return (dt - epoch).total_seconds() * 1000.0


def parse_log_file(*filenames):
	""" This function will convert the log to class LogLines """
	list_logline = []
	print(filenames, type(filenames))
	for name in filenames:
		print(name)
		file = open(os.path.join(location, name), "r")
		lines = file.readlines()
		line_number = 0
		print(file.name)

		for line in lines:

			if "[TEST]" in line:
				line_number += 1
				# print("Line Encountered: ", line)
				l = line.split()
				# convert timestamp YYYY/MM/DD HH:MM:SS.F   to UNIX timestamp or epoch time.
				timestamp = l[1] + " " + l[2]
				timestamp = datetime.datetime.strptime(
					timestamp, "%Y/%m/%d %H:%M:%S.%f")
				timestamp = unix_time_millis(timestamp)

				state = l[4]
				state_dictionary = process_state(state)

				# list_logline.append(
				#     LogLine(timestamp, file.name, line, state_dictionary))
				list_logline.append(
					(timestamp, str(file.name), line, state_dictionary))

		file.close()

	# print(list_logline[0])
	return sorted(list_logline, key=lambda x: x[0])


def parse_log_line(log_line):
	""" Convert LogLine to a LogEvent """
	# return LogEvent(log_line.timestamp, log_line.location, log_line.line, log_line.state)
	return LogEvent(log_line[0], log_line[1], log_line[2], log_line[3])


def print_logs(logfile):
	for log in logfile:
		print(log[0], log[3])


def process_state(state_recd):
	each_state = state_recd.split(',')
	state_dict = {}
	for s in each_state:
		temp = s.split(':')
		key = temp[0]
		value = temp[1]
		state_dict[key] = value

	return state_dict


# log1 = parse_log_file("server.exe.2796.log",
	#   "server.exe.8216.log", "server.exe.17368.log")
log1 = parse_log_file("server.exe.2796.test.log",
					  "server.exe.8216.test.log", "server.exe.17368.test.log")



# print_logs(log1)
print(log1[0])
print("-" * 10)
log_event = parse_log_line(log1[0])
print(log_event)
