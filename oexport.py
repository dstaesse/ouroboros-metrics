#
# SPDX-FileCopyrightText: 2021 - 2026 Dimitri Staessens
# SPDX-License-Identifier: BSD-3-Clause
#

"""
Ouroboros InfluxDB metrics exporter

Samples the Ouroboros RIB on a fixed period and writes the readings to
InfluxDB or to a file.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import socket
import struct
import sys
import threading
import time
import traceback
import argparse
import configparser
from abc import ABC, abstractmethod
from datetime import datetime
from types import TracebackType
from typing import Any, Optional

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WriteOptions, PointSettings
from influxdb_client.rest import ApiException
from pytz import utc

from ribreader import OuroborosRIBReader, IPCP_TYPES

_CLOCK_MONOTONIC = 1
_TFD_CLOEXEC = 0o2000000
_FAIRNESS_REGIMES = {1, 2, 4}

_libc = None

_libc_path = ctypes.util.find_library('c')
if sys.platform == 'linux' and _libc_path:
    try:
        _libc = ctypes.CDLL(_libc_path, use_errno=True)
    except OSError:
        pass


def _jain_index(values: list[float]) -> Optional[float]:
    """
    Jain's fairness index over a list of non-negative rates.
    """

    n = len(values)
    if n == 0:
        return None

    total = sum(values)

    sq = sum(v * v for v in values)
    if sq == 0:
        return None

    return (total * total) / (n * sq)


def _fairness_rates(flows: list[dict]) -> list[float]:
    """
    Paced rates that count toward the Jain fairness index.
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
    """
    Create and arm a CLOCK_MONOTONIC timerfd for *interval_ns*
    nanoseconds.

    Returns the file descriptor. The caller must close it when done.
    Raises :exc:`OSError` if the syscalls fail.
    """

    fd = _libc.timerfd_create(_CLOCK_MONOTONIC, _TFD_CLOEXEC)
    if fd < 0:
        errno = ctypes.get_errno()
        raise OSError(errno, f"timerfd_create failed: {os.strerror(errno)}")

    sec = interval_ns // 1_000_000_000
    nsec = interval_ns % 1_000_000_000

    itimerspec = struct.pack('qqqq', sec, nsec, sec, nsec)
    buf = ctypes.create_string_buffer(itimerspec)

    ret = _libc.timerfd_settime(fd, 0, buf, None)
    if ret < 0:
        errno = ctypes.get_errno()

        os.close(fd)
        raise OSError(errno, f"timerfd_settime failed: {os.strerror(errno)}")

    return fd


class MetricsWriter(ABC):
    """
    Abstract base class for metrics output writers.
    """

    @abstractmethod
    def write(self, bucket: str, record: Point) -> None:
        """
        Write a single metric point.
        """

    @abstractmethod
    def close(self) -> None:
        """
        Release resources held by the writer.
        """


class InfluxDBWriter(MetricsWriter):
    """
    Write metrics to an InfluxDB instance.
    """

    def __init__(self,  # pylint: disable=too-many-arguments,too-many-positional-arguments
                 cfg_path: str = './config.ini',
                 tags: dict = None,
                 url: str = None,
                 org: str = None,
                 token: str = None) -> None:

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

    def write(self, bucket: str, record: Point) -> None:
        try:
            self._write_api(bucket=bucket, record=record)
        except ApiException as e:
            print(e)

    def close(self) -> None:
        self.client.close()


class FileWriter(MetricsWriter):
    """
    Write metrics as InfluxDB line protocol to a file.
    """

    def __init__(self,
                 path: str,
                 tags: dict = None) -> None:

        self._tags = tags or {}
        self._lock = threading.Lock()
        self._file = open(path, 'a', encoding='utf-8')  # pylint: disable=consider-using-with

    def write(self, bucket: str, record: Point) -> None:
        for key, value in self._tags.items():
            record = record.tag(key, value)

        record = record.tag('system', socket.gethostname())

        line = record.to_line_protocol() + '\n'

        with self._lock:
            self._file.write(line)
            self._file.flush()

    def close(self) -> None:
        self._file.close()


class OuroborosExporter:
    """
    Export Ouroboros metrics through a MetricsWriter
    """

    def __init__(self,
                 metrics_writer: MetricsWriter,
                 bucket: str = 'ouroboros-metrics',
                 rib_path: str = '/tmp/ouroboros/') -> None:

        self.bucket = bucket
        self.writer = metrics_writer
        self.ribreader = OuroborosRIBReader(rib_path=rib_path)

    def __exit__(self,
                 _type: type[BaseException] | None,
                 value: BaseException | None,
                 _traceback: TracebackType | None) -> None:
        self.writer.close()

    def _write_ouroboros_ipcps_total(self,
                                     now: str,
                                     ipcp_type: str,
                                     n_ipcps: int) -> None:

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
                                                    now: str,
                                                    ipcp: str,
                                                    layer: str,
                                                    n_flows: int) -> None:
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

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _write_ouroboros_fa_congestion_metric(self,
                                              metric: str,
                                              now: str,
                                              ipcp_name: str,
                                              eid: str,
                                              layer: str,
                                              value: Any) -> None:

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
                                            layer: str,
                                            value: float) -> None:

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
                                          value: Any) -> None:

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
                                         value: Any) -> None:

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
                                              layer: str,
                                              value: Any) -> None:

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
                                              now: str,
                                              fd: str,
                                              ipcp_name: str,
                                              layer: str,
                                              metrics: dict) -> None:
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

    def _write_ouroboros_poa_metric(self,
                                    now: str,
                                    metric: str,
                                    poa: str,
                                    ipcp_name: str,
                                    layer: str,
                                    value) -> None:
        point = {
            'measurement': f'ouroboros_poa_{metric}',
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer,
                'poa': poa,
            },
            'fields': {
                metric: value,
            },
            'time': now,
        }

        self.writer.write(bucket=self.bucket,
                          record=Point.from_dict(point))

    def _write_ouroboros_process_frct_metric(self,
                                             now: str,
                                             metric: str,
                                             fd: str,
                                             process: str,
                                             value: Any) -> None:
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

    # pylint: enable=too-many-arguments,too-many-positional-arguments

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

    def _export_ouroboros_ipcps_total(self) -> None:

        ipcps = self.ribreader.get_ipcp_list()

        ipcps_total = {}

        for _type in IPCP_TYPES:
            ipcps_total[_type] = len(self._filter_ipcp_list(ipcps, ipcp_type=_type))

        now = datetime.now(utc)

        for _type, n_ipcps in ipcps_total.items():
            self._write_ouroboros_ipcps_total(now, _type, n_ipcps)

    def _export_ouroboros_flow_allocator_flows_total(self) -> None:

        ipcps = self.ribreader.get_ipcp_list()

        now = datetime.now(utc)

        for ipcp in [ipcp for ipcp in ipcps if ipcp['flows'] is not None]:
            self._write_ouroboros_flow_allocator_flows_total(
                now, ipcp['name'], ipcp['layer'], ipcp['flows'])

    def _export_ouroboros_fa_congestion_metrics(self) -> None:

        ipcps = self.ribreader.get_ipcp_list(names_only=True)

        now = datetime.now(utc)

        for ipcp in ipcps:
            flows = self.ribreader.get_flow_allocator_flow_info_for_ipcp(ipcp['name'])
            for flow in flows:
                eid = flow.get('endpoint_id')
                if eid is None:
                    continue

                for metric, value in flow.items():
                    if metric == 'endpoint_id':
                        continue

                    self._write_ouroboros_fa_congestion_metric(
                        metric, str(now), ipcp['name'],
                        eid, ipcp['layer'], value)

            rates = _fairness_rates(flows)
            if len(rates) >= 2:
                jain = _jain_index(rates)
                if jain is not None:
                    self._write_ouroboros_fa_fairness_metric(
                        str(now), ipcp['name'], ipcp['layer'], jain)

    def _export_ouroboros_lsdb_metrics(self) -> None:

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

    def _export_ouroboros_dht_metrics(self) -> None:

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

    def _export_ouroboros_data_transfer_metrics(self) -> None:
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

    def _export_ouroboros_process_frct_metrics(self) -> None:
        processes = self.ribreader.get_process_list()

        now = datetime.now(utc)

        for process in processes:
            for frct_info in self.ribreader.get_frct_info_for_process(process):
                for metric, value in frct_info.items():
                    self._write_ouroboros_process_frct_metric(
                        str(now), metric, frct_info['fd'],
                        process, value)

    def _export_ouroboros_poa_metrics(self) -> None:
        ipcps = self.ribreader.get_ipcp_list(names_only=True)

        now = datetime.now(utc)

        for ipcp in ipcps:
            poas = self.ribreader.get_poa_stats_for_ipcp(ipcp['name'])
            for poa, stats in poas.items():
                for metric, value in stats.items():
                    self._write_ouroboros_poa_metric(
                        str(now), metric, poa,
                        ipcp['name'], ipcp['layer'], value)

    def _safe_export(self, step: Any) -> None:
        """
        Run one export step, reporting a failure instead of raising.

        The RIB is a live filesystem: entries appear and vanish while it
        is walked, and a read can fail on a component that is shutting
        down. An exception escaping one step would end the process and
        with it every other series, so each step is fenced off.
        """

        try:
            step()
        except Exception:  # pylint: disable=broad-except
            print(f'{step.__name__} failed:', file=sys.stderr)
            traceback.print_exc()
            sys.stderr.flush()

    def export(self) -> None:
        """
        Export all available metrics
        """

        self._safe_export(self._export_ouroboros_ipcps_total)
        self._safe_export(self._export_ouroboros_flow_allocator_flows_total)
        self._safe_export(self._export_ouroboros_fa_congestion_metrics)
        self._safe_export(self._export_ouroboros_lsdb_metrics)
        self._safe_export(self._export_ouroboros_dht_metrics)
        self._safe_export(self._export_ouroboros_data_transfer_metrics)
        self._safe_export(self._export_ouroboros_poa_metrics)

    def export_frct(self) -> None:
        """
        Export the per-flow FRCT metrics of every application process.
        """

        self._safe_export(self._export_ouroboros_process_frct_metrics)

    def _loop(self,
              interval_ms: float,
              step: Any) -> None:
        """
        Call *step* every *interval_ms* until the process exits.
        """

        if _libc is None:
            while True:
                time.sleep(interval_ms / 1000.0)
                step()

        tfd = _make_timerfd(int(interval_ms * 1_000_000))
        try:
            while True:
                os.read(tfd, 8)
                step()
        finally:
            os.close(tfd)

    def run(self,
            interval_ms: float = 1000,
            slow_interval_ms: float = 1000) -> None:
        """
        Run the ouroboros exporter

        :param interval_ms: FRCT poll interval in milliseconds
        :param slow_interval_ms: interval for all other metrics (IPCP,
            LSDB, DHT, FA, DT) in milliseconds

        The two cadences run on separate threads. The IPCP walk reads
        one FUSE filesystem per IPCP, each served by a single-threaded
        loop in that daemon; a read that stalls there must not take the
        FRCT series with it.
        """

        slow = threading.Thread(target=self._loop,
                                args=(slow_interval_ms, self.export),
                                daemon=True)

        slow.start()

        self._loop(interval_ms, self.export_frct)


def _parse_args() -> argparse.Namespace:
    """
    The command line as an argparse namespace.
    """

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

    return argparser.parse_args()


def _tags_from_cli(tag_args: list[str]) -> dict:
    """
    The --tag key=value pairs as a dict. A malformed pair exits 1.
    """

    tags = {}

    for tag_str in tag_args:
        if '=' not in tag_str:
            print(f"Error: --tag must be in key=value format, got: '{tag_str}'",
                  file=sys.stderr)
            sys.exit(1)

        key, value = tag_str.split('=', 1)
        tags[key] = value

    return tags


def _merge_output_section(config: configparser.ConfigParser,
                          args: argparse.Namespace) -> None:
    """
    Apply the [output] section where the command line left a gap.
    """

    if not config.has_section('output'):
        return

    if args.output_file is None and config.has_option('output', 'file'):
        args.output_file = config.get('output', 'file')

    if not config.has_option('output', 'tags'):
        return

    for tag_str in config.get('output', 'tags').split(','):
        tag_str = tag_str.strip()
        if tag_str and '=' in tag_str:
            key, value = tag_str.split('=', 1)

            args.tags.setdefault(key, value)


def _merge_exporter_section(config: configparser.ConfigParser,
                            args: argparse.Namespace) -> None:
    """
    Apply the [exporter] section where the command line left a gap.
    """

    if not config.has_section('exporter'):
        return

    if args.interval == 1000 and config.has_option('exporter', 'interval'):
        args.interval = config.getint('exporter', 'interval')

    if (args.bucket == 'ouroboros-metrics'
            and config.has_option('exporter', 'bucket')):
        args.bucket = config.get('exporter', 'bucket')


def _merge_influx_section(config: configparser.ConfigParser,
                          args: argparse.Namespace) -> None:
    """
    Apply the [influx2] section where the command line left a gap.
    """

    if not config.has_section('influx2'):
        return

    if args.url is None and config.has_option('influx2', 'url'):
        args.url = config.get('influx2', 'url')

    if args.org is None and config.has_option('influx2', 'org'):
        args.org = config.get('influx2', 'org')

    if args.token is None and config.has_option('influx2', 'token'):
        args.token = config.get('influx2', 'token')


def _merge_config(path: str, args: argparse.Namespace) -> None:
    """
    Layer the config file at *path* underneath *args*. A missing file
    leaves *args* untouched.
    """

    if not os.path.exists(path):
        return

    config = configparser.ConfigParser()

    config.read(path)

    _merge_output_section(config, args)
    _merge_exporter_section(config, args)
    _merge_influx_section(config, args)


def _make_writer(args: argparse.Namespace) -> MetricsWriter:
    """
    The writer the options ask for.
    """

    if args.output_file:
        return FileWriter(path=args.output_file, tags=args.tags)

    return InfluxDBWriter(cfg_path=args.config, tags=args.tags,
                          url=args.url, org=args.org,
                          token=args.token)


def main() -> None:
    """
    Run the Ouroboros metrics exporter.
    """

    args = _parse_args()

    args.tags = _tags_from_cli(args.tag)

    _merge_config(args.config, args)

    writer = _make_writer(args)

    exporter = OuroborosExporter(metrics_writer=writer,
                                 bucket=args.bucket)
    try:
        if args.slow_interval is not None:
            slow_interval = args.slow_interval
        else:
            slow_interval = max(args.interval, 1000)

        exporter.run(interval_ms=args.interval,
                     slow_interval_ms=slow_interval)
    finally:
        writer.close()


if __name__ == '__main__':
    main()
