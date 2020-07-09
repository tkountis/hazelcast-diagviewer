
import os
import time
import argparse

from datetime import datetime
from pathlib import Path
from influxdb import InfluxDBClient

SCANNED_DIR_MARKER = '.diag_viewer_done'
DIAG_LOGFILE_EXT = ".log"
DIAG_LOGFILE_PREFIX = "diagnostics-"
LOG_TS_FORMAT = '%d-%m-%Y %H:%M:%S'
INFLUX_TS_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
METRIC_LINE = " Metric["


class DiagParser(object):

    def __init__(self, root_dir: str, recursive: bool, db_host: str, db_port: int, db_name: str,
                 force: bool, batch_size: int, import_id: str, normalize: bool):
        self.root_dir = root_dir
        self.recursive = recursive
        self.processed_metrics_count = 0
        self.force = force
        self.batch_size = batch_size
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.import_id = import_id
        self.normalize = normalize

        self.client = None
        self.queue = []

    def run(self):
        try:
            self.client = InfluxDBClient(host=self.db_host, port=self.db_port)
            print("Connected to InfluxDB")

            if self.db_name not in str(self.client.get_list_database()):
                print("Creating database ", self.db_name)
                self.client.create_database(self.db_name)

            print("Switching database ", self.db_name)
            self.client.switch_database(self.db_name)

            self._try_dir(self.root_dir)
            self.flush()
            print("Finished, processed metrics: ", self.processed_metrics_count)
        except RuntimeError as e:
            print(e)
        finally:
            self.client.close()
            print("Connection to InfluxDB closed")

    def flush(self):
        self.client.write_points(self.queue)
        self.queue.clear()

    def _try_dir(self, dir_path):
        if not self.force and SCANNED_DIR_MARKER in os.listdir(dir_path):
            print("Skipped ", dir_path)
            return

        print("Trying dir ", dir_path)
        for log in (f for f in os.listdir(dir_path)
                    if f.startswith(DIAG_LOGFILE_PREFIX) and f.endswith(DIAG_LOGFILE_EXT)):
            f = os.path.join(dir_path, log)
            print("Found ", f)
            try:
                with open(f, 'r') as trace_lines:
                    self._process_file(dir_path.split("/")[-2], log, enumerate(trace_lines))
            except PermissionError as e:
                print(e)

        if self.recursive:
            for path in (os.path.join(dir_path, f) for f in os.listdir(dir_path)):
                if not os.path.isdir(path):
                    continue

                sub_dir = path
                try:
                    self._try_dir(sub_dir)
                    # mark it so duplicates shouldn't happen
                    Path(os.path.join(sub_dir, SCANNED_DIR_MARKER)).touch()
                except PermissionError as e:
                    print(e)

    def _process_file(self, benchmark_id, log, lines_enumerator):
        cycle = {}
        RUN = lambda cycle: (
            self._apply_transformations(cycle),
            self._push_collection(benchmark_id, cycle))

        for i, line in lines_enumerator:
            try:
                if METRIC_LINE in line:
                    nodename, timestamp, meta, value = self._parse_metric_line(log, line)
                    if not cycle.get('tick'):
                        cycle['tick'] = timestamp

                    if timestamp != cycle['tick']:
                        RUN(cycle)
                        cycle = {}
                    else:
                        self._group_metric_with_same_timestamp(cycle, nodename, timestamp, meta, value)

                self.processed_metrics_count += 1
            except Exception as e:
                print("Problem sending metric ", log, line)
                print(e)

        # Remaining
        try:
            if len(cycle) is 0:
                return

            RUN(cycle)
        except Exception as e:
            print("Problem sending metric ", log, line)
            print(e)

    def _parse_metric_line(self, log_file, line):
        timestamp = DiagParser.extract_timestamp(line)
        nodename = DiagParser.extract_node_name(log_file)
        # Sample line
        # 10-03-2020 14:20:42 1583850042297 Metric[[thread=hz.upbeat_dubinsky.partition-operation.thread-1,
        # unit=count,metric=operation.thread.priorityPendingCount]=0]
        metric_tags_end = line.index(']=')
        tags_str = line[42:metric_tags_end].split(',')
        meta = dict((entry.split('=') for entry in tags_str))
        value = float(line[metric_tags_end + 2:line.index(']', metric_tags_end + 1)])
        return nodename, timestamp, meta, value

    def _group_metric_with_same_timestamp(self, cycle, nodename, timestamp, meta, value):
        measurement = meta['metric']
        cycle[measurement] = (nodename, timestamp, meta, value)

    def _apply_transformations(self, cycle):
        if 'wan.totalPublishLatency' in cycle and 'wan.totalPublishedEventCount' in cycle:
            nodename, _, _, total_pub_latency = cycle['wan.totalPublishLatency']
            _, _, _, total_pub_count = cycle['wan.totalPublishedEventCount']
            if total_pub_count and total_pub_latency:
                value = total_pub_latency / total_pub_count
                meta = {'unit': 'avg', 'metric': 'wan.publishLatencyAvg'}
                cycle['wan.publishLatencyAvg'] = (nodename, cycle['tick'], meta, value)


    def _push_collection(self, benchmark_id, cycle):
        for entry in cycle:
            # Ignore the timestamp entry, its not a metric
            if entry is not "tick":
                node_name, timestamp, meta, value = cycle[entry]
                self._push_metric(benchmark_id, node_name, timestamp, meta, value)

    def _push_metric(self, benchmark_id, node_name, timestamp, meta, value):
        # Sample output
        #
        # json_body = [
        #     {
        #         "measurement": "brushEvents",
        #         "tags": {
        #             "user": "Carol",
        #             "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        #         },
        #         "time": "2018-03-28T8:01:00Z",
        #         "fields": {
        #             "duration": 127
        #         }
        #     } ...
        # ]
        measurement = meta['metric']

        payload = dict()
        payload['measurement'] = measurement
        payload['time'] = timestamp if not self.normalize else DiagParser.normalize_timestamp(timestamp)

        fields = dict()
        fields[meta.get('unit', 'count')] = value
        payload['fields'] = fields

        tags = dict()
        if meta.get('thread'):
            tags['thread'] = meta.get('thread')

        tags['benchmark'] = benchmark_id
        tags['node'] = node_name
        tags['import_id'] = self.import_id
        payload['tags'] = tags

        self.queue.append(payload)
        if len(self.queue) >= self.batch_size:
            self.flush()

    @staticmethod
    def extract_timestamp(trace):
        timestamp = trace[0:20].strip(' ')
        # 2018-03-28T8:01:00Z
        time_tuple = time.strptime(timestamp, LOG_TS_FORMAT)
        return time.strftime(INFLUX_TS_FORMAT, time_tuple)

    @staticmethod
    def normalize_timestamp(timestamp):
        fixed_date = datetime.fromisoformat('2020-01-01')
        imported_date = datetime.strptime(timestamp, INFLUX_TS_FORMAT)
        fixed_date = fixed_date.replace(hour=12, minute=imported_date.minute,
                                        second=imported_date.second, microsecond=imported_date.microsecond)
        return time.strftime(INFLUX_TS_FORMAT, fixed_date.timetuple())

    @staticmethod
    def extract_node_name(filename):
        name = filename[filename.index(DIAG_LOGFILE_PREFIX) +
                        len(DIAG_LOGFILE_PREFIX):filename.index("-", len(DIAG_LOGFILE_PREFIX))]
        name = name.replace(".", "_")
        return name


parser = argparse.ArgumentParser(description='Process Hazelcast diagnostic metrics and push them to InfluxDB')
parser.add_argument('--dir', help='the root directory to start scanning from')
parser.add_argument('--force', default=False, action='store_true',
                    help='re-process directories previously marked as done')
parser.add_argument('--recursive', default=False, action='store_true',
                    help='scan the directory recursively')
parser.add_argument('--db_host', nargs='?', default='127.0.0.1', help='InfluxDB host')
parser.add_argument('--db_port', nargs='?', default=8086, type=int, help='InfluxDB port')
parser.add_argument('--db_name', nargs='?', default='diagnostics',
                    help='InfluxDB database name, it will be created if it doesn\'t exist')
parser.add_argument('--batch_size', nargs='?', default=1000, type=int,
                    help='how many metrics will be batched together before they are flushed to the DB')
parser.add_argument('--import_id', nargs='?', default=time.time_ns(), type=str,
                    help='an import identified to filter multiple imports of the same data')
parser.add_argument('--normalize', default=False, action='store_true',
                    help='normalize benchmark dates to a fixed date, making time range selection easier')

args = parser.parse_args()

parser = DiagParser(root_dir=args.dir, recursive=args.recursive, db_host=args.db_host, db_port=args.db_port,
                    force=args.force, db_name=args.db_name, batch_size=args.batch_size, import_id=args.import_id,
                    normalize=args.normalize)
parser.run()

