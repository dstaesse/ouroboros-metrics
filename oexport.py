#!./venv/bin/python
"""
Ouroboros InfluxDB metrics exporter

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following
disclaimer in the documentation and/or other materials provided
with the distribution.

3. Neither the name of the copyright holder nor the names of its
contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import ctypes
import ctypes.util
import os
import socket
import struct
import sys
import time
import argparse
import configparser
from abc import ABC, abstractmethod
from datetime import datetime

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WriteOptions, PointSettings
from influxdb_client.rest import ApiException
from pytz import utc

from ribreader import OuroborosRIBReader, IPCP_TYPES

# ---------------------------------------------------------------------------
# High-resolution periodic timer using Linux timerfd
# Falls back to time.sleep() on non-Linux platforms or if libc is unavailable.
# ---------------------------------------------------------------------------

_CLOCK_MONOTONIC = 1
_TFD_CLOEXEC = 0o2000000  # same as O_CLOEXEC

_libc = None
_libc_path = ctypes.util.find_library('c')
if sys.platform == 'linux' and _libc_path:
    try:
        _libc = ctypes.CDLL(_libc_path, use_errno=True)
    except OSError:
        pass


def _jain_index(values):
    """Jain's fairness index over a list of non-negative rates.

    J = (sum x)^2 / (n * sum x^2), in (0, 1]; 1.0 means perfectly fair.
    Returns None when there is nothing to measure (empty or all-zero).
    """
    n = len(values)
    if n == 0:
        return None

    total = sum(values)
    sq = sum(v * v for v in values)
    if sq == 0:
        return None

    return (total * total) / (n * sq)


# Additive inc, proportional dec, loss recovery: the pacer is binding.
_FAIRNESS_REGIMES = {1, 2, 4}


def _fairness_rates(flows):
    """Paced rates that count toward the Jain fairness index.

    Fairness is only meaningful over senders whose pacer constrains
    real data traffic. Slow start (0) has no allocation yet; source
    limited (3) covers sinks, where paced_rate is reverse-ACK pacing.
    Older RIBs without a regime code fall back to a byte-ratio test
    to weed out sinks: ACK-only traffic is a few percent of the
    received volume, nowhere near the 25% cutoff.
    """
    rates = []
    for flow in flows:
        rate = flow.get('paced_rate', 0)
        if rate <= 0:
            continue

        regime = flow.get('cong_regime')
        sent = flow.get('sent_bytes_total', 0)
        rcvd = flow.get('recv_bytes_total', 0)
        if regime is not None:
            if regime in _FAIRNESS_REGIMES:
                rates.append(rate)
        elif sent * 4 > rcvd:
            rates.append(rate)

    return rates


def _make_timerfd(interval_ns: int) -> int:
    """Create and arm a CLOCK_MONOTONIC timerfd for *interval_ns* nanoseconds.

    Returns the file descriptor.  The caller must close it when done.
    Raises :exc:`OSError` if the syscalls fail.
    """
    fd = _libc.timerfd_create(_CLOCK_MONOTONIC, _TFD_CLOEXEC)
    if fd < 0:
        errno = ctypes.get_errno()
        raise OSError(errno, f"timerfd_create failed: {os.strerror(errno)}")
    sec = interval_ns // 1_000_000_000
    nsec = interval_ns % 1_000_000_000
    # struct itimerspec: it_interval (tv_sec, tv_nsec), it_value (tv_sec, tv_nsec)
    # Both set to the same interval so the timer fires immediately and then
    # auto-rearms at the same period.  Layout is 4 × int64 on 64-bit Linux.
    itimerspec = struct.pack('qqqq', sec, nsec, sec, nsec)
    buf = ctypes.create_string_buffer(itimerspec)
    ret = _libc.timerfd_settime(fd, 0, buf, None)
    if ret < 0:
        errno = ctypes.get_errno()
        os.close(fd)
        raise OSError(errno, f"timerfd_settime failed: {os.strerror(errno)}")
    return fd


class MetricsWriter(ABC):
    """Abstract base class for metrics output writers."""

    @abstractmethod
    def write(self, bucket: str, record: Point):
        """Write a single metric point."""

    @abstractmethod
    def close(self):
        """Release resources held by the writer."""


class InfluxDBWriter(MetricsWriter):
    """Write metrics to an InfluxDB instance."""

    def __init__(self,  # pylint: disable=too-many-arguments,too-many-positional-arguments
                 cfg_path: str = './config.ini',
                 tags: dict = None,
                 url: str = None,
                 org: str = None,
                 token: str = None):

        point_settings = PointSettings()
        point_settings.add_default_tag('system', socket.gethostname())

        if tags:
            for key, value in tags.items():
                point_settings.add_default_tag(key, value)

        write_options = WriteOptions(batch_size=500,
                                     flush_interval=10_000,
                                     jitter_interval=1_000,
                                     retry_interval=1_000,
                                     max_retries=5,
                                     max_retry_delay=30_000,
                                     exponential_base=2)

        if url:
            self.client = InfluxDBClient(url=url, org=org, token=token)
        else:
            self.client = InfluxDBClient.from_config_file(cfg_path)
        self._write_api = self.client.write_api(write_options=write_options,
                                                point_settings=point_settings).write

    def write(self, bucket: str, record: Point):
        try:
            self._write_api(bucket=bucket, record=record)
        except ApiException as e:
            print(e)

    def close(self):
        self.client.close()


class FileWriter(MetricsWriter):
    """Write metrics as InfluxDB line protocol to a file."""

    def __init__(self,
                 path: str,
                 tags: dict = None):

        self._tags = tags or {}
        self._file = open(path, 'a', encoding='utf-8')  # pylint: disable=consider-using-with

    def write(self, bucket: str, record: Point):
        for key, value in self._tags.items():
            record = record.tag(key, value)
        record = record.tag('system', socket.gethostname())
        self._file.write(record.to_line_protocol() + '\n')
        self._file.flush()

    def close(self):
        self._file.close()


class OuroborosExporter:
    """
    Export Ouroboros metrics to InfluxDB
    """

    def __init__(self,
                 metrics_writer: MetricsWriter,
                 bucket='ouroboros-metrics',
                 rib_path='/tmp/ouroboros/'):

        self.bucket = bucket
        self.writer = metrics_writer
        self.ribreader = OuroborosRIBReader(rib_path=rib_path)

    def __exit__(self, _type, value, traceback):
        self.writer.close()

    def _write_ouroboros_ipcps_total(self,
                                     now,
                                     ipcp_type,
                                     n_ipcps):

        point = {
            'measurement': f'ouroboros_{ipcp_type}_ipcps_total',
            'tags': {
                'type': ipcp_type,
            },
            'fields': {
                'ipcps': n_ipcps,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))

    def _write_ouroboros_flow_allocator_flows_total(self,
                                                    now,
                                                    ipcp,
                                                    layer,
                                                    n_flows):
        point = {
            'measurement': 'ouroboros_flow_allocator_flows_total',
            'tags': {
                'ipcp': ipcp,
                'layer': layer
            },
            'fields': {
                'flows': n_flows,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))

    # pylint: disable-msg=too-many-arguments,too-many-positional-arguments
    def _write_ouroboros_fa_congestion_metric(self,
                                              metric: str,
                                              now: str,
                                              ipcp_name: str,
                                              eid: str,
                                              layer,
                                              value):

        point = {
            'measurement': f'ouroboros_flow_allocator_{metric}',
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer,
                'flow_id': eid
            },
            'fields': {
                metric: value,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))

    def _write_ouroboros_fa_fairness_metric(self,
                                            now: str,
                                            ipcp_name: str,
                                            layer,
                                            value):

        point = {
            'measurement': 'ouroboros_flow_allocator_jain_index',
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer,
            },
            'fields': {
                'jain_index': value,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))

    def _write_ouroboros_lsdb_node_metric(self,
                                          metric: str,
                                          now: str,
                                          ipcp_name: str,
                                          layer: str,
                                          value):

        point = {
            'measurement': f'ouroboros_lsdb_{metric}_total',
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer
            },
            'fields': {
                metric: value,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))

    def _write_ouroboros_dht_node_metric(self,
                                         metric: str,
                                         now: str,
                                         ipcp_name: str,
                                         layer: str,
                                         value):

        point = {
            'measurement': f'ouroboros_dht_{metric}_total',
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer
            },
            'fields': {
                metric: value,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))

    def _write_ouroboros_data_transfer_metric(self,
                                              metric: str,
                                              now: str,
                                              qos_cube: str,
                                              fd: str,
                                              endpoint: str,
                                              ipcp_name: str,
                                              layer,
                                              value):

        point = {
            'measurement': f'ouroboros_data_transfer_{metric}',
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer,
                'flow_descriptor': fd,
                'qos_cube': qos_cube,
                'endpoint': endpoint
            },
            'fields': {
                metric: value,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))

    def _write_ouroboros_data_transfer_queued(self,
                                              now,
                                              fd,
                                              ipcp_name,
                                              layer,
                                              metrics) -> None:
        for metric in metrics:
            point = {
                'measurement': f'ouroboros_data_transfer_{metric}',
                'tags': {
                    'ipcp': ipcp_name,
                    'layer': layer,
                    'flow_descriptor': fd,
                },
                'fields': {
                    metric: metrics[metric],
                },
                'time': now,
            }

            self.writer.write(bucket=self.bucket,
                              record=Point.from_dict(point))

    def _write_ouroboros_process_frct_metric(self,
                                             now,
                                             metric,
                                             fd,
                                             process,
                                             value):
        point = {
            'measurement': f'ouroboros_process_frct_{metric}',
            'tags': {
                'process': process,
                'flow_descriptor': fd,
            },
            'fields': {
                metric: value,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))

    def _write_ouroboros_eth_summary_metric(self,
                                            now,
                                            metric,
                                            ipcp_name,
                                            layer,
                                            value):
        point = {
            'measurement': f'ouroboros_eth_{metric}',
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer,
            },
            'fields': {
                metric: value,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))

    def _write_ouroboros_eth_flow_metric(self,
                                         now,
                                         metric,
                                         fd,
                                         ipcp_name,
                                         layer,
                                         value):
        point = {
            'measurement': f'ouroboros_eth_flow_{metric}',
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer,
                'flow_descriptor': fd,
            },
            'fields': {
                metric: value,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))
    # pylint: enable-msg=too-many-arguments,too-many-positional-arguments

    @staticmethod
    def _filter_ipcp_list(ipcp_list: list[dict],
                          ipcp_type: str = None,
                          ipcp_state: str = None,
                          layer: str = None) -> list[dict]:

        if ipcp_type not in IPCP_TYPES:
            print(f"Unknown IPCP TYPE: {ipcp_type}")
            return []

        if ipcp_type:
            ipcp_list = [ipcp for ipcp in ipcp_list if ipcp['type'] == ipcp_type]

        if ipcp_state:
            ipcp_list = [ipcp for ipcp in ipcp_list if ipcp['state'] == ipcp_state]

        if layer:
            ipcp_list = [ipcp for ipcp in ipcp_list if ipcp['layer'] == layer]

        return ipcp_list

    def _export_ouroboros_ipcps_total(self):

        ipcps = self.ribreader.get_ipcp_list()

        ipcps_total = {}

        for _type in IPCP_TYPES:
            ipcps_total[_type] = len(self._filter_ipcp_list(ipcps, ipcp_type=_type))

        now = datetime.now(utc)

        for _type, n_ipcps in ipcps_total.items():
            self._write_ouroboros_ipcps_total(now, _type, n_ipcps)

    def _export_ouroboros_flow_allocator_flows_total(self):

        ipcps = self.ribreader.get_ipcp_list()

        now = datetime.now(utc)

        for ipcp in [ipcp for ipcp in ipcps if ipcp['flows'] is not None]:
            self._write_ouroboros_flow_allocator_flows_total(
                now, ipcp['name'], ipcp['layer'], ipcp['flows'])

    def _export_ouroboros_fa_congestion_metrics(self):

        ipcps = self.ribreader.get_ipcp_list(names_only=True)

        now = datetime.now(utc)

        for ipcp in ipcps:
            flows = self.ribreader.get_flow_allocator_flow_info_for_ipcp(ipcp['name'])
            for flow in flows:
                for metric, value in flow.items():
                    if metric == 'endpoint_id':
                        continue

                    self._write_ouroboros_fa_congestion_metric(
                        metric, str(now), ipcp['name'],
                        flow['endpoint_id'], ipcp['layer'], value)

            # Per-IPCP Jain fairness over the CA-allocated (paced) rates;
            # a bottleneck-fairness proxy, meaningful with >=2 flows.
            rates = _fairness_rates(flows)
            if len(rates) >= 2:
                jain = _jain_index(rates)
                if jain is not None:
                    self._write_ouroboros_fa_fairness_metric(
                        str(now), ipcp['name'], ipcp['layer'], jain)

    def _export_ouroboros_lsdb_metrics(self):

        ipcps = self.ribreader.get_ipcp_list(names_only=True)

        now = datetime.now(utc)

        for ipcp in ipcps:
            metrics = self.ribreader.get_lsdb_stats_for_ipcp(ipcp['name'])
            for metric, value in metrics.items():
                self._write_ouroboros_lsdb_node_metric(metric,
                                                       str(now),
                                                       ipcp['name'],
                                                       ipcp['layer'],
                                                       value)

    def _export_ouroboros_dht_metrics(self):

        ipcps = self.ribreader.get_ipcp_list(names_only=True)

        now = datetime.now(utc)

        for ipcp in ipcps:
            metrics = self.ribreader.get_dht_stats_for_ipcp(ipcp['name'])
            for metric, value in metrics.items():
                self._write_ouroboros_dht_node_metric(metric,
                                                      str(now),
                                                      ipcp['name'],
                                                      ipcp['layer'],
                                                      value)

    def _export_ouroboros_data_transfer_metrics(self):
        ipcps = self.ribreader.get_ipcp_list(names_only=True)

        now = datetime.now(utc)

        for ipcp in ipcps:
            info = self.ribreader.get_data_transfer_flow_info_for_ipcp(ipcp['name'])
            for flow in info:
                qoscubes = [_field for _field in flow if str(_field).startswith('QoS cube')]
                for qoscube in qoscubes:
                    for metric in flow[qoscube]:
                        self._write_ouroboros_data_transfer_metric(
                            metric, str(now), qoscube,
                            flow['fd'], flow['endpoint'],
                            ipcp['name'], ipcp['layer'],
                            flow[qoscube][metric])
                queued = {
                    'queued_packets_rx': flow['queued_packets_rx'],
                    'queued_packets_tx': flow['queued_packets_tx']
                }
                self._write_ouroboros_data_transfer_queued(
                    str(now), flow['fd'], ipcp['name'],
                    ipcp['layer'], queued)

    def _export_ouroboros_process_frct_metrics(self):
        processes = self.ribreader.get_process_list()

        now = datetime.now(utc)

        for process in processes:
            for frct_info in self.ribreader.get_frct_info_for_process(process):
                for metric, value in frct_info.items():
                    self._write_ouroboros_process_frct_metric(
                        str(now), metric, frct_info['fd'],
                        process, value)

    def _export_ouroboros_eth_metrics(self):
        ipcps = self.ribreader.get_ipcp_list(names_only=True)

        now = datetime.now(utc)

        for ipcp in ipcps:
            summary = self.ribreader.get_eth_summary_for_ipcp(
                ipcp['name'])
            for metric, value in summary.items():
                self._write_ouroboros_eth_summary_metric(
                    str(now), metric, ipcp['name'],
                    ipcp['layer'], value)

            flows = self.ribreader.get_eth_flow_info_for_ipcp(
                ipcp['name'])
            for flow in flows:
                for metric, value in flow.items():
                    if metric == 'fd':
                        continue
                    self._write_ouroboros_eth_flow_metric(
                        str(now), metric, flow['fd'],
                        ipcp['name'], ipcp['layer'], value)

    def export(self):
        """
        Export all available metrics
        :return:
        """

        self._export_ouroboros_ipcps_total()
        self._export_ouroboros_flow_allocator_flows_total()
        self._export_ouroboros_fa_congestion_metrics()
        self._export_ouroboros_lsdb_metrics()
        self._export_ouroboros_dht_metrics()
        self._export_ouroboros_data_transfer_metrics()
        self._export_ouroboros_eth_metrics()
        self._export_ouroboros_process_frct_metrics()

    def run(self,
            interval_ms: float = 1000,
            slow_interval_ms: float = 1000):
        """
        Run the ouroboros exporter

        :param interval_ms: FRCT poll interval in milliseconds
        :param slow_interval_ms: interval for all other metrics (IPCP, LSDB,
            DHT, FA, DT).  These change at roughly 1 Hz so there is no benefit
            in polling them at the FRCT rate.  Must be >= interval_ms.
        :return: None
        """
        slow_every = max(1, round(slow_interval_ms / interval_ms))
        tick = 0

        interval_ns = int(interval_ms * 1_000_000)
        if _libc is not None:
            tfd = _make_timerfd(interval_ns)
            try:
                while True:
                    os.read(tfd, 8)  # blocks until next tick; value = missed count
                    if tick % slow_every == 0:
                        self.export()
                    else:
                        self._export_ouroboros_process_frct_metrics()
                    tick += 1
            finally:
                os.close(tfd)
        else:
            while True:
                time.sleep(interval_ms / 1000.0)
                if tick % slow_every == 0:
                    self.export()
                else:
                    self._export_ouroboros_process_frct_metrics()
                tick += 1


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="Ouroboros metrics exporter")
    argparser.add_argument('-i', '--interval', type=int, default='1000',
                           help="Interval at which to collect FRCT metrics (milliseconds)")
    argparser.add_argument('--slow-interval', type=int, default=None,
                           help="Interval for IPCP/LSDB/DHT/FA/DT metrics (milliseconds, "
                                "default: max(interval, 1000))")
    argparser.add_argument('-b', '--bucket', type=str, default='ouroboros-metrics',
                           help="InfluxDB bucket to write to")
    argparser.add_argument('-o', '--output-file', type=str, default=None,
                           help="Write line protocol to file instead of InfluxDB")
    argparser.add_argument('-t', '--tag', action='append', default=[],
                           help="Extra tag as key=value (repeatable)")
    argparser.add_argument('-c', '--config', type=str, default='./config.ini',
                           help="Path to config.ini")
    argparser.add_argument('--url', type=str, default=None,
                           help="InfluxDB server URL")
    argparser.add_argument('--org', type=str, default=None,
                           help="InfluxDB organization")
    argparser.add_argument('--token', type=str, default=None,
                           help="InfluxDB authentication token")
    args = argparser.parse_args()

    # Parse extra tags from CLI
    extra_tags = {}
    for tag_str in args.tag:
        if '=' not in tag_str:
            print(f"Error: --tag must be in key=value format, got: '{tag_str}'",
                  file=sys.stderr)
            sys.exit(1)
        k, v = tag_str.split('=', 1)
        extra_tags[k] = v

    # Read config file for defaults
    output_file = args.output_file
    interval = args.interval
    export_bucket = args.bucket
    influx_url = args.url
    influx_org = args.org
    influx_token = args.token

    config = configparser.ConfigParser()
    if os.path.exists(args.config):
        config.read(args.config)
        if config.has_section('output'):
            if output_file is None and config.has_option('output', 'file'):
                output_file = config.get('output', 'file')
            if config.has_option('output', 'tags'):
                for tag_str in config.get('output', 'tags').split(','):
                    tag_str = tag_str.strip()
                    if tag_str and '=' in tag_str:
                        k, v = tag_str.split('=', 1)
                        extra_tags.setdefault(k, v)
        if config.has_section('exporter'):
            if interval == 1000 and config.has_option('exporter', 'interval'):
                interval = config.getint('exporter', 'interval')
            if export_bucket == 'ouroboros-metrics' and config.has_option('exporter', 'bucket'):
                export_bucket = config.get('exporter', 'bucket')
        if config.has_section('influx2'):
            if influx_url is None and config.has_option('influx2', 'url'):
                influx_url = config.get('influx2', 'url')
            if influx_org is None and config.has_option('influx2', 'org'):
                influx_org = config.get('influx2', 'org')
            if influx_token is None and config.has_option('influx2', 'token'):
                influx_token = config.get('influx2', 'token')

    # Instantiate the appropriate writer
    if output_file:
        writer = FileWriter(path=output_file, tags=extra_tags)
    else:
        writer = InfluxDBWriter(cfg_path=args.config, tags=extra_tags,
                                url=influx_url, org=influx_org,
                                token=influx_token)

    exporter = OuroborosExporter(metrics_writer=writer, bucket=export_bucket)
    try:
        slow_interval = args.slow_interval if args.slow_interval is not None \
            else max(interval, 1000)
        exporter.run(interval_ms=interval, slow_interval_ms=slow_interval)
    finally:
        writer.close()
