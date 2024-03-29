import time


class Timer:
    start_time = time.time()

    def time_past(self):
        end = time.time()
        hours, rem = divmod(end - self.start_time, 3600)
        minutes, seconds = divmod(rem, 60)
        return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)

