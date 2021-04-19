import time
from skt.ye import slack_send 
from process import Process

class Notifier(Process):
    def __init__(self, log_queue, slack_queue):
       super().__init__(log_queue=log_queue)
       self.slack_queue = slack_queue

    def run(self):
        while not self.stopped():
            if not self.slack_queue.empty():
                count = 1
                msg = None
                while not self.slack_queue.empty() and count < 101:
                    curr = self.slack_queue.get()
                    msg = f"[{count}] " + curr if msg is None else msg + f"\n[{count}] " + curr 
                    count += 1
                if msg is not None:
                    try:
                        self.notify_slack(msg)
                    except Exception as e:
                        self.logger.exception(e)
                        msg = f"Unexpected error: {e} [{msg}]"
                        self.error(msg)
            time.sleep(10)

    def notify_slack(self, text):
        slack_send(text,
                   username=self.config.slack_username,
                   channel=self.config.slack_channel,
                   icon_emoji=self.config.slack_icon_emoji)
