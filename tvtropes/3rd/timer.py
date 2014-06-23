import time
class Timer:
	def __init__(self):
		self.time_of_last_call = time.time()
	def elapsed_seconds(self):
		current_time = time.time()
		elapsed_seconds = current_time - self.time_of_last_call
		self.time_of_last_call = current_time
		return str(round(elapsed_seconds, 2)) + " seconds"

if __name__ == '__main__':
	t = Timer()
	print t.time_of_last_call
	time.sleep(1)
	print t.elapsed_seconds()
