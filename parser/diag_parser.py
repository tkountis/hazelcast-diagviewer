import os
import socket
import time
import re
from collections import OrderedDict

CARBON_SERVER = '127.0.0.1'
CARBON_PORT = 2003
CARBON_LINE_PREFIX = "diagnostics"

LIMIT_METRICS = "inf"

DIAG_LOGFILE_EXT = ".log"
DIAG_LOGFILE_PREFIX = "diagnostics-"
DIAG_LOGFILE_TS_FORMAT = '%d-%m-%Y %H:%M:%S'
DIAG_LOGFILE_METRICS_PLUGIN_SECTION_TAG = " Metrics["

METRIC_KEY_FORMAT_RULES = OrderedDict({
    '[': '',
    ']': '',
    '->': '.',
    '/': '.',
    '..': '.',
    r'(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}):(\d{2,5})': r'\1_\2_\3_\4_\5',
    ':': '_'
})


class DiagParser(object):

    def __init__(self, diagnostics_dir, metric_tag):
        self.metric_tag = metric_tag
        self.diagnostics_dir = diagnostics_dir
        self.processed_metrics_count = 0
        self.sock = socket.socket()

    def try_read_file(self):
        try:
            self.sock.connect((CARBON_SERVER, CARBON_PORT))
            print "Connected to remote carbon"
            self._read_file()
            print "Reported so far ", self.processed_metrics_count
            print "Finished processing"
        except RuntimeError, e:
            print e.message
        finally:
            self.sock.close()
            print "Connection to remote carbon closed"

    def _read_file(self):
        for log in (f for f in os.listdir(self.diagnostics_dir) if f.endswith(DIAG_LOGFILE_EXT)):
            with open(os.path.join(self.diagnostics_dir, log), 'r') as trace_lines:
                self._do_read_lines(log, enumerate(trace_lines))

    def _do_read_lines(self, log, lines_enumerator):
        for i, line in lines_enumerator:
            self._check_processed_metrics_limit()
            if DIAG_LOGFILE_METRICS_PLUGIN_SECTION_TAG in line:
                nodename, timestamp, metrics = self._read_detected_metric_line(log, line, lines_enumerator)
                self._report_metrics(nodename, timestamp, metrics)
                self.processed_metrics_count += 1

    def _check_processed_metrics_limit(self):
        if LIMIT_METRICS != "inf" and self.processed_metrics_count >= LIMIT_METRICS:
            raise RuntimeError("Reached processing limit, exiting")

    def _read_detected_metric_line(self, log, starting_line, lines_enumerator):
        metric_trace = ""
        timestamp = DiagParser.extract_timestamp(starting_line)

        # that's terrible bro, you should produce excellence not abominations like that
        for j, trace_line in lines_enumerator:
            metric_trace += trace_line
            if trace_line.endswith("]\n"):
                nodename = DiagParser.extract_node_name(log)
                return nodename, timestamp, DiagParser.extract_metrics(metric_trace)

    @staticmethod
    def extract_timestamp(trace):
        timestamp = trace[0:trace.index(DIAG_LOGFILE_METRICS_PLUGIN_SECTION_TAG)].strip(' ')
        epoch = time.mktime(time.strptime(timestamp, DIAG_LOGFILE_TS_FORMAT))
        epoch = str(epoch).strip('.0')
        return epoch


    @staticmethod
    def extract_node_name(filename):
        name = filename[filename.index(DIAG_LOGFILE_PREFIX) +
                        len(DIAG_LOGFILE_PREFIX):filename.index("-", len(DIAG_LOGFILE_PREFIX))]
        name = name.replace(".", "_")
        return name

    @staticmethod
    def extract_metrics(trace):
        metrics = {}

        for pair in trace.split("\n"):
            if len(pair) <= 1:
                continue

            key, value = pair.split("=")

            # Clean up keys and values - order matters
            key = key.strip(' ')
            key = key.replace('[', '')
            key = key.replace(']', '')
            key = key.replace('->', '.')
            key = key.replace('/', '.')
            key = key.replace('..', '.')

            key = re.sub(r'(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}):(\d{2,5})', r'\1_\2_\3_\4_\5', key)
            key = key.replace(':', '_')

            # for rule, replacement in KEY_FORMAT_RULES.iteritems():
            #     if re.
            #     key = key.replace(rule, replacement)

            value = value.strip(' ')
            value = value.replace("]", "")
            value = value.replace(",", "")
            value = value if value != "0.0" else "0"
            if "NaN" in value:
                value = 0
            elif "." in value:
                value = float(value)
            else:
                value = int(value)

            metrics[key] = value

        return metrics

    def _report_metrics(self, node_name, timestamp, metrics):
        prefix = ".".join([CARBON_LINE_PREFIX, self.metric_tag, node_name])

        for metric_name, metric_value in metrics.items():
            fqmn = ".".join([prefix, metric_name])
            # print " ".join([fqmn, str(metric_value), str(timestamp), "\n"])
            self.sock.sendall(" ".join([fqmn, str(metric_value), str(timestamp), "\n"]))
            if self.processed_metrics_count % 1000 == 0:
                print "Reported so far ", self.processed_metrics_count
                time.sleep(0.5)


parser = DiagParser(diagnostics_dir="/Users/zozeo/workspace/hazelcast/playground/", metric_tag="hz_demo")
print "Diagnostics parser started"
time.sleep(1)

parser.try_read_file()

