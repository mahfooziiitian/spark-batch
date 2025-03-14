from pyspark.sql.streaming import StreamingQueryListener
import logging

class MyListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        logging.warning(f"'{event.name}' [{event.id}] got started!")

    def onQueryProgress(self, event):
        # print("Query made progress: " + queryProgress.progress)
        logging.warning(f"'{event} is running!")
        row = event.progress.observedMetrics.get("metric")
        if row is not None:
            if row.malformed / row.cnt > 0.5:
                logging.warning("ALERT! Ouch! there are too many malformed "
                      f"records {row.malformed} out of {row.cnt}!")
            else:
                logging.warning(f"{row.cnt} rows processed!")
        else:
            logging.warning(f'there is no metric')

    def onQueryIdle(self, event):
        logging.warning(f"{event.id} is idle!")

    def onQueryTerminated(self, event):
        logging.warning(f"{event.id} got terminated!")
