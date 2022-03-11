#! /usr/bin/python
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

import os
import re
import socket
import time
import argparse
from datetime import datetime
from typing import Optional

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WriteOptions, PointSettings
from influxdb_client.rest import ApiException
from pytz import utc

IPCP_TYPE_UNICAST = 'unicast'
IPCP_TYPE_BROADCAST = 'broadcast'
IPCP_TYPE_UDP = 'udp'
IPCP_TYPE_ETH_DIX = 'eth-dix'
IPCP_TYPE_ETH_LLC = 'eth-llc'
IPCP_TYPE_LOCAL = 'local'
IPCP_TYPE_UNKNOWN = 'unknown'

IPCP_TYPES = [IPCP_TYPE_UNICAST,
              IPCP_TYPE_BROADCAST,
              IPCP_TYPE_UDP,
              IPCP_TYPE_ETH_DIX,
              IPCP_TYPE_ETH_LLC,
              IPCP_TYPE_LOCAL,
              IPCP_TYPE_UNKNOWN]

IPCP_STATE_NULL = 'null'
IPCP_STATE_INIT = 'init'
IPCP_STATE_OPERATIONAL = 'operational'
IPCP_STATE_SHUTDOWN = 'shutdown'

IPCP_STATES = [IPCP_STATE_NULL,
               IPCP_STATE_INIT,
               IPCP_STATE_OPERATIONAL,
               IPCP_STATE_SHUTDOWN]


class OuroborosRIBReader:
    """
    Class for reading stuff from the Ouroboros system
    Resource Information Base (RIB)
    """
    def __init__(self,
                 rib_path: str):

        self.rib_path = rib_path

    def _get_dir_for_ipcp(self,
                          ipcp_name: str) -> str:

        return os.path.join(self.rib_path, ipcp_name)

    def _get_dir_for_process(self,
                             process_name: str) -> str:

        return os.path.join(self.rib_path, process_name)

    def _get_dt_dir_for_ipcp(self,
                             ipcp_name: str) -> Optional[str]:

        path = self._get_dir_for_ipcp(ipcp_name)
        try:
            _subdirs = [f.name for f in os.scandir(path)]
        except IOError as _:
            return None

        for _dir in _subdirs:
            if len(_dir) > 3 and _dir[:3] == 'dt.':
                return os.path.join(path, _dir)

        return None

    def _get_path_for_ipcp_flow_n_plus_1_info(self,
                                              ipcp_name: str,
                                              fd: str):

        return os.path.join(self.rib_path, ipcp_name, 'flow-allocator', fd)

    def _get_path_for_ipcp_flow_n_minus_1_info(self,
                                               ipcp_name: str,
                                               fd: str) -> str:

        dt_dir = self._get_dt_dir_for_ipcp(ipcp_name)
        return os.path.join(dt_dir, fd)

    def _get_path_for_frct_flow_info(self,
                                     process: str,
                                     fd: str) -> str:

        process_dir = self._get_dir_for_process(process)
        return os.path.join(process_dir, str(fd), 'frct')

    def _get_ipcp_type_for_ipcp(self,
                                ipcp_name: str) -> str:

        _dir = self._get_dir_for_ipcp(ipcp_name)
        path = '{}/info/_type'.format(_dir)
        if not os.path.exists(path):
            return IPCP_TYPE_UNKNOWN

        try:
            with open(path) as f:
                return f.readline()[:-1]
        except IOError as _:
            return IPCP_TYPE_UNKNOWN

    def _get_layer_name_for_ipcp(self,
                                 ipcp_name: str) -> str:

        _dir = self._get_dir_for_ipcp(ipcp_name)
        path = '{}/info/_layer'.format(_dir)
        if not os.path.exists(path):
            return '(error)'

        try:
            with open(path) as f:
                return f.readline()[:-1]
        except IOError as _:
            return '(error)'

    def _get_ipcp_state_for_ipcp(self,
                                 ipcp_name: str) -> str:

        _dir = self._get_dir_for_ipcp(ipcp_name)
        path = '{}/info/_state'.format(_dir)
        if not os.path.exists(path):
            return IPCP_TYPE_UNKNOWN

        try:
            with open(path) as f:
                return f.readline()[:-1]
        except IOError as e:
            print(e)
            return IPCP_TYPE_UNKNOWN

    def _get_n_plus_1_flows_for_ipcp(self,
                                     ipcp_name: str) -> list[str]:

        path = os.path.join(self._get_dir_for_ipcp(ipcp_name), 'flow-allocator')

        if not os.path.exists(path):
            return []

        try:
            return [f.name for f in os.scandir(path)]
        except IOError as e:
            print(e)

        return []

    def _get_n_minus_1_flows_for_ipcp(self,
                                      ipcp_name: str) -> list[str]:

        path = self._get_dt_dir_for_ipcp(ipcp_name)
        if path is None:
            return []

        if not os.path.exists(path):
            return []

        try:
            return [f.name for f in os.scandir(path)]
        except IOError as e:
            print(e)
            return []

    def _get_address_for_ipcp(self,
                              ipcp_name):

        path = self._get_dir_for_ipcp(ipcp_name)
        try:
            _subdirs = [f.name for f in os.scandir(path)]
        except IOError as _:
            return None

        for _dir in _subdirs:
            if len(_dir) > 3 and _dir[:3] == 'dt.':
                return _dir[3:]

        return None

    def get_lsdb_stats_for_ipcp(self,
                                ipcp_name: str) -> dict:
        """
        Get statistics for the link state database of an IPCP
        :param ipcp_name: name of the IPCP
        :return: statistics in a dict
        """

        address = self._get_address_for_ipcp(ipcp_name)
        if address is None:
            return {}

        path = os.path.join(self._get_dir_for_ipcp(ipcp_name), 'lsdb/')
        if not os.path.exists(path):
            return {}

        nodes = []
        neighbors = 0
        links = 0

        lsdb_entries = []
        try:
            lsdb_entries = [f.path for f in os.scandir(path)]
        except IOError as _:
            pass

        for lsdb_entry in lsdb_entries:
            try:
                with open(lsdb_entry) as e:
                    for line in e.readlines():
                        if 'src' in line:
                            src = line.split()[-1]
                            if src not in nodes:
                                nodes += [src]
                            if src == address:
                                neighbors += 1
                        if 'dst' in line:
                            dst = line.split()[-1]
                            if dst not in nodes:
                                nodes += [dst]
                links += 1
            except IOError as _:
                continue

        stats = {'neighbors': neighbors,
                 'nodes': len(nodes),
                 'links': links}

        return stats

    def _get_flows_for_process(self,
                               process_name: str) -> list[str]:
        path = self._get_dir_for_process(process_name)

        if not os.path.exists(path):
            return []

        try:
            return [f.name for f in os.scandir(path) if f.is_dir()]
        except IOError as e:
            print(e)

        return []

    @staticmethod
    def _get_trailing_number(s: str) -> int:
        m = re.search(r'\d+$', s)
        return int(m.group()) if m else None

    def _get_flow_info_for_n_plus_1_flow(self,
                                         ipcp_name: str,
                                         fd: str) -> dict:

        str_to_metric = {
            'Flow established at': None,
            'Remote address': None,
            'Local endpoint ID': 'endpoint_id',
            'Remote endpoint ID': None,
            'Sent (packets)': 'sent_pkts_total',
            'Sent (bytes)': 'sent_bytes_total',
            'Send failed (packets)': 'send_failed_packets_total',
            'Send failed (bytes)': 'send_failed_bytes_total',
            'Received (packets)': 'recv_pkts_total',
            'Received (bytes)': 'recv_bytes_total',
            'Receive failed (packets)': 'recv_failed_pkts_total',
            'Receive failed (bytes)': 'recv_failed_bytes_total',
            'Sent flow updates (packets)': 'sent_flow_updates_total',
            'Received flow updates (packets)': 'recv_flow_updates_total',
            'Congestion avoidance algorithm': None,
            'Upstream congestion level': 'up_cong_lvl',
            'Downstream congestion level': 'down_cong_lvl',
            'Upstream packet counter': 'up_pkt_ctr',
            'Downstream packet counter': 'down_pkt_ctr',
            'Congestion window size (ns)': 'cong_wnd_width_ns',
            'Packets in this window': 'cong_wnd_current_pkts',
            'Bytes in this window': 'cong_wnd_current_bytes',
            'Max bytes in this window': 'cong_wnd_size_bytes',
            'Current congestion regime': None
        }

        ret = dict()

        path = self._get_path_for_ipcp_flow_n_plus_1_info(ipcp_name, fd)

        if not os.path.exists(path):
            return dict()

        with open(path) as f:
            for line in f.readlines():
                split_line = line.split(':')
                phrase = split_line[0]
                metric = str_to_metric[phrase]
                if metric is not None:
                    value = self._get_trailing_number(split_line[1])
                    ret[metric] = value

        return ret

    def _get_frct_info_for_process_flow(self,
                                        process: str,
                                        fd: str) -> dict:

        str_to_metric = {
            'Maximum packet lifetime (ns)': 'mpl_timer_ns',
            'Max time to Ack (ns)': 'a_timer_ns',
            'Max time to Retransmit (ns)': 'r_timer_ns',
            'Smoothed rtt (ns)': 'srtt_ns',
            'RTT standard deviation (ns)': 'mdev_ns',
            'Retransmit timeout RTO (ns)': 'rto_ns',
            'Sender left window edge': 'snd_lwe',
            'Sender right window edge': 'snd_rwe',
            'Sender inactive (ns)': 'snd_inact',
            'Sender current sequence number': 'snd_seqno',
            'Receiver left window edge': 'rcv_lwe',
            'Receiver right window edge': 'rcv_rwe',
            'Receiver inactive (ns)': 'rcv_inact',
            'Receiver last ack': 'rcv_seqno',
            'Number of pkt retransmissions': 'n_rxm',
        }

        ret = dict()

        path = self._get_path_for_frct_flow_info(process, fd)

        if not os.path.exists(path):
            return dict()

        ret['fd'] = fd

        with open(path) as f:
            for line in f.readlines():
                split_line = line.split(':')
                phrase = split_line[0]
                metric = str_to_metric[phrase]
                if metric is not None:
                    value = self._get_trailing_number(split_line[1])
                    ret[metric] = value

        return ret

    def get_flow_allocator_flow_info_for_ipcp(self,
                                              ipcp_name: str) -> list[dict]:
        """
        Get the flow intformation for all N+1 flows in a certain IPCP
        :param ipcp_name: name of the IPCP
        :return: dict with flow information
        """
        flow_info = []

        flow_descriptors = self._get_n_plus_1_flows_for_ipcp(ipcp_name)
        for flow in flow_descriptors:
            info = self._get_flow_info_for_n_plus_1_flow(ipcp_name, flow)
            flow_info += [info]

        return flow_info

    def _get_flow_info_for_n_minus_1_flow(self,
                                          ipcp_name: str,
                                          fd: str) -> dict:

        ret = dict()

        path = self._get_path_for_ipcp_flow_n_minus_1_info(ipcp_name, fd)

        str_to_qos_metric = {
         'Flow established at': None,
         ' sent (packets)': 'sent_packets_total',
         ' sent (bytes)': 'sent_bytes_total',
         ' rcvd (packets)': 'recv_packets_total',
         ' rcvd (bytes)': 'recv_bytes_total',
         ' local sent (packets)': 'local_sent_packets_total',
         ' local sent (bytes)': 'local_sent_bytes_total',
         ' local rcvd (packets)': 'local_recv_packets_total',
         ' local rcvd (bytes)': 'local_recv_bytes_total',
         ' dropped ttl (packets)': 'ttl_packets_dropped_total',
         ' dropped ttl (bytes)': 'ttl_bytes_dropped_total',
         ' failed writes (packets)': 'write_packets_dropped_total',
         ' failed writes (bytes)': 'write_bytes_dropped_total',
         ' failed nhop (packets)': 'nhop_packets_dropped_total',
         ' failed nhop (bytes)': 'nhop_bytes_dropped_total'
        }

        if not os.path.exists(path):
            return dict()

        with open(path) as f:
            _current_cube = ''
            ret['fd'] = fd
            for line in [_line for _line in f.readlines() if _line != '\n']:
                if 'Endpoint address' in line:
                    ret['endpoint'] = line.split(':')[-1].replace(' ', '')[:-1]
                elif 'Queued packets (rx)' in line:
                    ret['queued_packets_rx'] = self._get_trailing_number(line)
                elif 'Queued packets (tx)' in line:
                    ret['queued_packets_tx'] = self._get_trailing_number(line)
                elif 'Qos cube' in line:
                    _cube = self._get_trailing_number(line[:-2])
                    _current_cube = 'QoS cube ' + str(_cube)
                    ret[_current_cube] = dict()
                else:
                    split_line = line.split(':')
                    metric = str_to_qos_metric[split_line[0]]
                    if metric is not None:
                        value = self._get_trailing_number(split_line[1])
                        ret[_current_cube][metric] = value

        return ret

    def get_data_transfer_flow_info_for_ipcp(self,
                                             ipcp_name: str) -> list[dict]:
        """
        Get the flow information for all Data Transfer (N-1) flows in a certain IPCP
        :param ipcp_name: name of the IPCP
        :return: flow information for the data transfer flows
        """

        flow_info = []

        flow_descriptors = self._get_n_minus_1_flows_for_ipcp(ipcp_name)
        for flow in flow_descriptors:
            info = self._get_flow_info_for_n_minus_1_flow(ipcp_name, flow)
            flow_info += [info]

        return flow_info

    def get_frct_info_for_process(self,
                                  process_name: str) -> list[dict]:
        """
        Get the frct information for all flows for a certain process
        :param process_name: name of the process
        :return: flow information for the N-1 flows
        """

        frct_info = []

        flow_descriptors = self._get_flows_for_process(process_name)

        for flow in flow_descriptors:
            info = self._get_frct_info_for_process_flow(process_name, flow)
            frct_info += [info]

        return frct_info

    # pylint: disable-msg=too-many-arguments
    def get_ipcp_list(self,
                      names_only: bool = False,  # return name and layer name
                      types: bool = True,
                      states: bool = True,
                      layers: bool = True,
                      flows: bool = True) -> list[dict]:
        """
        Get a list of all IPCPs
        :param names_only: only return IPCP names and layer names
        :param types: return IPCP type
        :param states: return IPCP state
        :param layers: return layer in which the IPCP is enrolled
        :param flows: return the number of allocated (N+1) flows for this IPCP
        :return: list of dicts containing IPCP info
        """

        ipcp_list = []

        if not os.path.exists(self.rib_path):
            return []

        for ipcp_dir in [f.path for f in os.scandir(self.rib_path)
                         if f.is_dir() and not f.name.startswith('proc.')]:
            ipcp_name = os.path.split(ipcp_dir)[-1]
            ipcp_type = None
            ipcp_state = None
            ipcp_layer = self._get_layer_name_for_ipcp(ipcp_name) if layers else None
            ipcp_flows = None
            if not names_only:
                ipcp_type = self._get_ipcp_type_for_ipcp(ipcp_name) if types else None
                ipcp_state = self._get_ipcp_state_for_ipcp(ipcp_name) if states else None
                ipcp_flows = self._get_n_plus_1_flows_for_ipcp(ipcp_name) if flows else None

            ipcp_list += [{'name': ipcp_name,
                           'type': ipcp_type,
                           'state': ipcp_state,
                           'layer': ipcp_layer,
                           'flows': len(ipcp_flows) if ipcp_flows else None}]
        return ipcp_list
        # pylint: enable-msg=too-many-arguments

    def get_process_list(self) -> list[str]:
        """
        Get a list of all the Ouroboros applications that may be exposing frct stats
        :return: list of process names ("proc.<pid>")
        """
        proc_list = []

        if not os.path.exists(self.rib_path):
            return []

        for proc in [f.name for f in os.scandir(self.rib_path)
                     if f.is_dir() and f.name.startswith('proc.')]:
            proc_list += [proc]

        return proc_list


class OuroborosExporter:
    """
    Export Ouroboros metrics to InfluxDB
    """

    def __init__(self,
                 bucket='ouroboros-metrics',
                 config='./config.ini',
                 rib_path='/tmp/ouroboros/'):

        point_settings = PointSettings()
        point_settings.add_default_tag('system', socket.gethostname())

        write_options = WriteOptions(batch_size=500,
                                     flush_interval=10_000,
                                     jitter_interval=1_000,
                                     retry_interval=1_000,
                                     max_retries=5,
                                     max_retry_delay=30_000,
                                     exponential_base=2)

        self.bucket = bucket
        self.client = InfluxDBClient.from_config_file(config)
        self.write_api = self.client.write_api(write_options=write_options,
                                               point_settings=point_settings).write
        self.query_api = self.client.query_api()
        self.ribreader = OuroborosRIBReader(rib_path=rib_path)

    def __exit__(self, _type, value, traceback):
        self.client.close()

    def _write_ouroboros_ipcps_total(self,
                                     now,
                                     ipcp_type,
                                     n_ipcps):

        point = {
            'measurement': 'ouroboros_{}_ipcps_total'.format(ipcp_type),
            'tags': {
                'type': ipcp_type,
            },
            'fields': {
                'ipcps': n_ipcps,
                'time': str(now)
            }
        }

        self.write_api(bucket=self.bucket,
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
                'time': str(now)
            }
        }

        self.write_api(bucket=self.bucket,
                       record=Point.from_dict(point))

    # pylint: disable-msg=too-many-arguments
    def _write_ouroboros_fa_congestion_metric(self,
                                              metric: str,
                                              now: str,
                                              ipcp_name: str,
                                              eid: str,
                                              layer,
                                              value):

        point = {
            'measurement': 'ouroboros_flow_allocator_' + metric,
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer,
                'flow_id': eid
            },
            'fields': {
                metric: value,
                'time': now
            }
        }

        try:
            self.write_api(bucket=self.bucket,
                           record=Point.from_dict(point))
        except ApiException as e:
            print(e, point)

    def _write_ouroboros_lsdb_node_metric(self,
                                          metric: str,
                                          now: str,
                                          ipcp_name: str,
                                          layer: str,
                                          value):

        point = {
            'measurement': 'ouroboros_lsdb_' + metric + '_total',
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer
            },
            'fields': {
                metric: value,
                'time': now
            }
        }

        try:
            self.write_api(bucket=self.bucket,
                           record=Point.from_dict(point))
        except ApiException as e:
            print(e, point)

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
            'measurement': 'ouroboros_data_transfer_' + metric,
            'tags': {
                'ipcp': ipcp_name,
                'layer': layer,
                'flow_descriptor': fd,
                'qos_cube': qos_cube,
                'endpoint': endpoint
            },
            'fields': {
                metric: value,
                'time': now
            }
        }

        try:
            self.write_api(bucket=self.bucket,
                           record=Point.from_dict(point))
        except ApiException as e:
            print(e, point)

    def _write_ouroboros_data_transfer_queued(self,
                                              now,
                                              fd,
                                              ipcp_name,
                                              layer,
                                              metrics) -> None:
        point = dict()
        for metric in metrics:
            point = {
                'measurement': 'ouroboros_data_transfer_' + metric,
                'tags': {
                    'ipcp': ipcp_name,
                    'layer': layer,
                    'flow_descriptor': fd,
                },
                'fields': {
                    metric: metrics[metric],
                    'time': now
                }
            }

        try:
            self.write_api(bucket=self.bucket,
                           record=Point.from_dict(point))
        except ApiException as e:
            print(e, point)

    def _write_ouroboros_process_frct_metric(self,
                                             now,
                                             metric,
                                             fd,
                                             process,
                                             value):
        point = {
            'measurement': 'ouroboros_process_frct_' + metric,
            'tags': {
                'process': process,
                'flow_descriptor': fd,
            },
            'fields': {
                metric: value,
                'time': now
            }
        }

        try:
            self.write_api(bucket=self.bucket,
                           record=Point.from_dict(point))
        except ApiException as e:
            print(e, point)
    # pylint: enable-msg=too-many-arguments

    @staticmethod
    def _filter_ipcp_list(ipcp_list: list[dict],
                          ipcp_type: str = None,
                          ipcp_state: str = None,
                          layer: str = None) -> list[dict]:

        if ipcp_type not in IPCP_TYPES:
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

        ipcps_total = dict()

        for _type in IPCP_TYPES:
            ipcps_total[_type] = len(self._filter_ipcp_list(ipcps, ipcp_type=_type))

        now = datetime.now(utc)

        for _type, n_ipcps in ipcps_total.items():
            self._write_ouroboros_ipcps_total(now, _type, n_ipcps)

    def _export_ouroboros_flow_allocator_flows_total(self):

        ipcps = self.ribreader.get_ipcp_list()

        now = datetime.now(utc)

        for ipcp in [ipcp for ipcp in ipcps if ipcp['flows'] is not None]:
            self._write_ouroboros_flow_allocator_flows_total(now, ipcp['name'], ipcp['layer'], ipcp['flows'])

    def _export_ouroboros_fa_congestion_metrics(self):

        ipcps = self.ribreader.get_ipcp_list(names_only=True)

        now = datetime.now(utc)

        for ipcp in ipcps:
            flows = self.ribreader.get_flow_allocator_flow_info_for_ipcp(ipcp['name'])
            for flow in flows:
                for metric in flow:
                    if metric == 'endpoint_id':
                        continue

                    self._write_ouroboros_fa_congestion_metric(metric,
                                                               str(now),
                                                               ipcp['name'],
                                                               flow['endpoint_id'],
                                                               ipcp['layer'],
                                                               flow[metric])

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

    def _export_ouroboros_data_transfer_metrics(self):
        ipcps = self.ribreader.get_ipcp_list(names_only=True)

        now = datetime.now(utc)

        for ipcp in ipcps:
            info = self.ribreader.get_data_transfer_flow_info_for_ipcp(ipcp['name'])
            for flow in info:
                qoscubes = [_field for _field in flow if str(_field).startswith('QoS cube')]
                for qoscube in qoscubes:
                    for metric in flow[qoscube]:
                        self._write_ouroboros_data_transfer_metric(metric,
                                                                   str(now),
                                                                   qoscube,
                                                                   flow['fd'],
                                                                   flow['endpoint'],
                                                                   ipcp['name'],
                                                                   ipcp['layer'],
                                                                   flow[qoscube][metric])
                self._write_ouroboros_data_transfer_queued(str(now),
                                                           flow['fd'],
                                                           ipcp['name'],
                                                           ipcp['layer'],
                                                           {'queued_packets_rx': flow['queued_packets_rx'],
                                                            'queued_packets_tx': flow['queued_packets_tx']})

    def _export_ouroboros_process_frct_metrics(self):
        processes = self.ribreader.get_process_list()

        now = datetime.now(utc)

        for process in processes:
            for frct_info in self.ribreader.get_frct_info_for_process(process):
                for metric in frct_info:
                    self._write_ouroboros_process_frct_metric(str(now),
                                                              metric,
                                                              frct_info['fd'],
                                                              process,
                                                              frct_info[metric])

    def export(self):
        """
        Export all available metrics
        :return:
        """

        self._export_ouroboros_ipcps_total()
        self._export_ouroboros_flow_allocator_flows_total()
        self._export_ouroboros_fa_congestion_metrics()
        self._export_ouroboros_lsdb_metrics()
        self._export_ouroboros_data_transfer_metrics()
        self._export_ouroboros_process_frct_metrics()

    def run(self,
            interval_ms: float = 1000):
        """
        Run the ouroboros exporter

        :param interval_ms: read interval in milliseconds
        :return: None
        """

        while True:
            time.sleep(interval_ms / 1000.0)
            self.export()


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="Ouroboros InfluxDB metrics exporter")
    argparser.add_argument('-i', '--interval', type=int, default='1000',
                           help="Interval at which to collect metrics (milliseconds)")
    argparser.add_argument('-b', '--bucket', type=str, default='ouroboros-metrics',
                           help="InfluxDB bucket to write to")
    args = argparser.parse_args()
    interval_ms = args.interval
    exporter = OuroborosExporter(bucket=args.bucket)
    exporter.run(interval_ms=interval_ms)
