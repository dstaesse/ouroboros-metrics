#
# SPDX-FileCopyrightText: 2021 - 2026 Dimitri Staessens
# SPDX-License-Identifier: BSD-3-Clause
#

"""
Ouroboros RIB filesystem reader
"""

from __future__ import annotations

import os
import re
from typing import Optional

IPCP_TYPE_UNICAST = "unicast"
IPCP_TYPE_BROADCAST = "broadcast"
IPCP_TYPE_LOCAL = "local"
IPCP_TYPE_UNKNOWN = "unknown"

IPCP_TYPES = [IPCP_TYPE_UNICAST, IPCP_TYPE_BROADCAST, IPCP_TYPE_LOCAL, IPCP_TYPE_UNKNOWN]

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
    Reader for the Ouroboros Resource Information Base (RIB)
    """

    def __init__(self,
                 rib_path: str) -> None:

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
                                              fd: str) -> str:

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

        path = f'{_dir}/info/_type'
        if not os.path.exists(path):
            return IPCP_TYPE_UNKNOWN

        try:
            with open(path, encoding='utf-8') as f:
                return f.readline()[:-1]
        except IOError as _:
            return IPCP_TYPE_UNKNOWN

    def _get_layer_name_for_ipcp(self,
                                 ipcp_name: str) -> str:

        _dir = self._get_dir_for_ipcp(ipcp_name)

        path = f'{_dir}/info/_layer'
        if not os.path.exists(path):
            return '(error)'

        try:
            with open(path, encoding='utf-8') as f:
                return f.readline()[:-1]
        except IOError as _:
            return '(error)'

    def _get_ipcp_state_for_ipcp(self,
                                 ipcp_name: str) -> str:

        _dir = self._get_dir_for_ipcp(ipcp_name)

        path = f'{_dir}/info/_state'
        if not os.path.exists(path):
            return IPCP_TYPE_UNKNOWN

        try:
            with open(path, encoding='utf-8') as f:
                return f.readline()[:-1]
        except IOError as e:
            print(e)
            return IPCP_TYPE_UNKNOWN

    def _get_n_plus_1_flows_for_ipcp(self,
                                     ipcp_name: str) -> list[str]:

        path = os.path.join(self._get_dir_for_ipcp(ipcp_name),
                            'flow-allocator')
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
                              ipcp_name: str) -> Optional[str]:

        path = self._get_dir_for_ipcp(ipcp_name)
        try:
            _subdirs = [f.name for f in os.scandir(path)]
        except IOError as _:
            return None

        for _dir in _subdirs:
            if len(_dir) > 3 and _dir[:3] == 'dt.':
                return _dir[3:]

        return None

    @staticmethod
    def _get_lsdb_link_entries(path: str) -> list[str]:

        entries = []

        try:
            for entry in os.scandir(path):
                if not entry.name.startswith('.'):
                    entries.append(entry.path)
        except IOError as _:
            pass

        return entries

    def get_lsdb_stats_for_ipcp(self,
                                ipcp_name: str) -> dict:
        """
        Get statistics for the link state database of an IPCP.
        Entries named '.dt <addr>' and '.mgmt <addr>' list the
        neighbours and are skipped; every other entry is a link.
        :param ipcp_name: name of the IPCP
        :return: statistics in a dict
        """

        address = self._get_address_for_ipcp(ipcp_name)
        if address is None:
            return {}

        path = os.path.join(self._get_dir_for_ipcp(ipcp_name),
                            'lsdb/')
        if not os.path.exists(path):
            return {}

        nodes = []
        neighbors = 0
        links = 0

        for lsdb_entry in self._get_lsdb_link_entries(path):
            try:
                with open(lsdb_entry, encoding='utf-8') as e:
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

    @staticmethod
    def _get_trailing_number(s: str) -> Optional[int]:

        m = re.search(r'-?\d+$', s)

        return int(m.group()) if m else None

    def get_dht_stats_for_ipcp(self,
                               ipcp_name: str) -> dict:
        """
        Get statistics for the DHT directory of an IPCP
        :param ipcp_name: name of the IPCP
        :return: statistics in a dict
        """

        str_to_metric = {
            '  Number of keys': 'keys',
            '  Number of local values': 'local_values',
            '  Number of non-local values': 'non_local_values'
        }

        path = os.path.join(self._get_dir_for_ipcp(ipcp_name),
                            'dht/stats')
        if not os.path.exists(path):
            return {}

        ret = {}

        with open(path, encoding='utf-8') as f:
            for line in f.readlines():
                split_line = line.split(':')

                phrase = split_line[0]
                if phrase not in str_to_metric:
                    continue

                metric = str_to_metric[phrase]
                value = self._get_trailing_number(split_line[1])
                ret[metric] = value

        return ret

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

    def _get_flow_info_for_n_plus_1_flow(self,
                                         ipcp_name: str,
                                         fd: str) -> dict:

        str_to_metric = {
            'Local endpoint ID': 'endpoint_id',
            'Local PoA ID': 'endpoint_id',
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
            'Upstream congestion level': 'up_cong_lvl',
            'Downstream congestion level': 'down_cong_lvl',
            'Paced rate (bytes/s)': 'paced_rate',
            'Pacer lead (bytes)': 'cong_tokens',
            'Congestion regime (code)': 'cong_regime',
            'Control steps (count)': 'ctrl_steps_total',
            'Control time elapsed (ns)': 'ctrl_time_ns_total',
            'Control time banked (ns)': 'ctrl_banked_ns_total',
            'Feedback updates (count)': 'fb_updates_total',
            'Feedback timeouts (count)': 'fb_timeouts_total',
            'Path capacity (bytes/s)': 'path_capacity',
            'Capacity rate floor (bytes/s)': 'cap_rate_floor',
            'Capacity updates (count)': 'cap_updates_total',
            'Slow start peak rate (bytes/s)': 'ss_peak_rate',
            'Signal-loss cuts (count)': 'loss_cuts_total',
            'Heartbeat RTT samples (count)': 'hb_rtt_samples_total',
            'Ramp time constant (ns)': 'ss_tc_ns',
        }

        ret = {}

        path = self._get_path_for_ipcp_flow_n_plus_1_info(
            ipcp_name, fd)
        if not os.path.exists(path):
            return {}

        with open(path, encoding='utf-8') as f:
            for line in f.readlines():
                split_line = line.split(':')

                phrase = split_line[0]
                if phrase not in str_to_metric:
                    continue

                metric = str_to_metric[phrase]
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
            'Minimum RTT (RACK base, ns)': 'min_rtt_ns',
            'Sender left window edge': 'snd_lwe',
            'Sender right window edge': 'snd_rwe',
            'Sender inactive (ns)': 'snd_inact',
            'Sender current sequence number': 'snd_seqno',
            'Receiver left window edge': 'rcv_lwe',
            'Receiver right window edge': 'rcv_rwe',
            'Receiver inactive (ns)': 'rcv_inact',
            'Receiver last ack': 'rcv_seqno',
            'RXM (RTO-driven) sent': 'rxm_rto',
            'RXM packets received': 'rxm_rcv',
            '  duplicates received': 'rxm_dup_rcv',
            'RXM (SACK mechanism) sent': 'rxm_sack',
            'RXM (RACK-driven) sent': 'rxm_rack',
            'RXM (zero reorder wnd) sent': 'rxm_zero_reo',
            'RXM (NACK-driven) sent': 'rxm_nack',
            'ACK packets sent': 'ack_snd',
            'Delayed-ACK timer fires': 'ack_fire',
            '  suppressed (seqno)': 'ack_supp_seqno',
            '  suppressed (inact)': 'ack_supp_inact',
            '  suppressed (rate)': 'ack_supp_rate',
            'ACK packets received': 'ack_rcv',
            '  fed RTT estimator': 'ack_rtt',
            '  wire dups dropped': 'ack_dup_rcv',
            'FRCTI_RCV time (ns)': 'rcv_proc_ns',
            'tw_move time (ns)': 'tw_move_ns',
            'drain_rx_nb calls': 'drain_calls',
            'Duplicates received': 'dup_rcv',
            'Out-of-window pkts received': 'out_rcv',
            'Out-of-rqueue pkts received': 'rqo_rcv',
            'OOO arrivals': 'ooo_rcv',
            'SACKs sent': 'sack_snd',
            'SACKs received': 'sack_rcv',
            'D-SACKs sent': 'dsack_snd',
            'D-SACKs received': 'dsack_rcv',
            'D-SACK out-of-range dropped': 'dsack_drop',
            'Pre-DRF NACKs sent': 'nack_snd',
            'Pre-DRF NACKs received': 'nack_rcv',
            'Tail loss probes sent': 'tlp_snd',
            'Inactivity drops (silent)': 'inact_drop',
            'DRF window rebases': 'drf_rebase',
            'rq slots cleared by release_rq': 'rq_released',
            'RTT probes sent': 'rttp_snd',
            'RTT probe replies received': 'rttp_rcv',
            'RTT estimator samples': 'rtt_smpl',
            'Rendez-vous packets sent': 'rdv_snd',
            'Rendez-vous packets received': 'rdv_rcv',
            'Keepalives sent': 'ka_snd',
            'Keepalives received': 'ka_rcv',
            'SDU writes fragmented': 'sdu_snd_frag',
            '  alloc fail mid-SDU': 'sdu_snd_alloc',
            '  tx fail mid-SDU': 'sdu_snd_tx',
            'Fragments sent': 'frag_snd',
            'Fragments received': 'frag_rcv',
            'SDUs delivered reassembled': 'sdu_reasm',
            'SDUs delivered (SOLE)': 'sdu_sole',
            'Fragments dropped (malformed)': 'frag_drop',
            'Stream bytes sent': 'strm_snd_byte',
            'Stream bytes received': 'strm_rcv_byte',
            'Stream bytes delivered': 'strm_dlv_byte',
            'Stream packets dropped': 'strm_drop',
            'Stream FINs dropped': 'strm_fin_drop',
            'RX rbuff queued': 'rx_q_now',
            'TX rbuff queued': 'tx_q_now',
            'RXM-due entries': 'rxm_due_count',
            '  bail (acked)': 'rxm_due_acked',
            '  bail (unowned)': 'rxm_due_unowned',
            '  bail (aged)': 'rxm_due_aged',
            '  bail (defer)': 'rxm_due_defer',
            '  defer, no rxm at HoL': 'rxm_hol_gone',
            '  skip (fast-rxm set)': 'rxm_fast_skip',
            '  skip (stuck past rto)': 'rxm_fast_stuck',
            '  skip (no repair budget)': 'rxm_no_budget',
            'RXM-arm malloc failures': 'rxm_arm_fail',
            'RXM cancels (teardown)': 'rxm_cancel',
            'RXM tx into dead flow': 'rxm_tx_dead',
            'Tx ring drops (any cause)': 'tx_drop',
            '  ack': 'tx_drop_ack',
            '  sack': 'tx_drop_sack',
            '  ka': 'tx_drop_ka',
            '  rttp': 'tx_drop_rttp',
            '  nack': 'tx_drop_nack',
            '  rdv': 'tx_drop_rdv',
            '  other': 'tx_drop_other',
        }

        ret = {}

        path = self._get_path_for_frct_flow_info(process, fd)
        if not os.path.exists(path):
            return {}

        ret['fd'] = fd

        with open(path, encoding='utf-8') as f:
            for line in f.readlines():
                split_line = line.split(':')

                phrase = split_line[0]
                if phrase not in str_to_metric:
                    continue

                metric = str_to_metric[phrase]
                value = self._get_trailing_number(split_line[1])
                ret[metric] = value

        return ret

    def get_flow_allocator_flow_info_for_ipcp(self,
                                              ipcp_name: str
                                              ) -> list[dict]:
        """
        Get the flow information for all N+1 flows in an IPCP.
        :param ipcp_name: name of the IPCP
        :return: list of dicts with per-flow information
        """

        flow_info = []

        flow_descriptors = self._get_n_plus_1_flows_for_ipcp(
            ipcp_name)
        for flow in flow_descriptors:
            info = self._get_flow_info_for_n_plus_1_flow(
                ipcp_name, flow)
            flow_info += [info]

        return flow_info

    def _get_flow_info_for_n_minus_1_flow(self,
                                          ipcp_name: str,
                                          fd: str) -> dict:

        ret = {}

        path = self._get_path_for_ipcp_flow_n_minus_1_info(
            ipcp_name, fd)

        str_to_qos_metric = {
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
            return {}

        with open(path, encoding='utf-8') as f:
            _current_cube = ''
            ret['fd'] = fd

            for line in [_l for _l in f.readlines() if _l != '\n']:
                if 'Endpoint address' in line:
                    ret['endpoint'] = (
                        line.split(':')[-1].replace(' ', '')[:-1])
                elif 'Queued packets (rx)' in line:
                    ret['queued_packets_rx'] = (
                        self._get_trailing_number(line))
                elif 'Queued packets (tx)' in line:
                    ret['queued_packets_tx'] = (
                        self._get_trailing_number(line))
                elif 'Qos cube' in line:
                    _cube = self._get_trailing_number(line[:-2])
                    _current_cube = f'QoS cube {_cube}'
                    ret[_current_cube] = {}
                else:
                    split_line = line.split(':')
                    if split_line[0] not in str_to_qos_metric:
                        continue
                    metric = str_to_qos_metric[split_line[0]]
                    value = self._get_trailing_number(
                        split_line[1])
                    ret[_current_cube][metric] = value

        return ret

    def get_data_transfer_flow_info_for_ipcp(self,
                                             ipcp_name: str
                                             ) -> list[dict]:
        """
        Get flow info for all Data Transfer (N-1) flows in an IPCP.
        :param ipcp_name: name of the IPCP
        :return: list of dicts with per-flow information
        """

        flow_info = []

        flow_descriptors = self._get_n_minus_1_flows_for_ipcp(
            ipcp_name)
        for flow in flow_descriptors:
            info = self._get_flow_info_for_n_minus_1_flow(
                ipcp_name, flow)
            flow_info += [info]

        return flow_info

    def get_frct_info_for_process(self,
                                  process_name: str) -> list[dict]:
        """
        Get the FRCT information for all flows of a process.
        :param process_name: name of the process
        :return: list of dicts with per-flow FRCT information
        """

        frct_info = []

        flow_descriptors = self._get_flows_for_process(
            process_name)
        for flow in flow_descriptors:
            info = self._get_frct_info_for_process_flow(
                process_name, flow)
            frct_info += [info]

        return frct_info

    def get_ipcp_list(self,
                      names_only: bool = False) -> list[dict]:
        """
        Get a list of all IPCPs.
        :param names_only: only return IPCP names and layer names
        :return: list of dicts containing IPCP info
        """

        ipcp_list = []

        if not os.path.exists(self.rib_path):
            return []

        for entry in os.scandir(self.rib_path):
            if not entry.is_dir() or entry.name.startswith('proc.'):
                continue

            ipcp_name = os.path.split(entry.path)[-1]
            ipcp_type = None
            ipcp_state = None
            ipcp_flows = None
            n_flows = None

            ipcp_layer = self._get_layer_name_for_ipcp(ipcp_name)

            if not names_only:
                ipcp_type = self._get_ipcp_type_for_ipcp(ipcp_name)
                ipcp_state = self._get_ipcp_state_for_ipcp(ipcp_name)
                ipcp_flows = self._get_n_plus_1_flows_for_ipcp(ipcp_name)

            if ipcp_flows:
                n_flows = len(ipcp_flows)

            ipcp_list += [{
                'name': ipcp_name,
                'type': ipcp_type,
                'state': ipcp_state,
                'layer': ipcp_layer,
                'flows': n_flows}]

        return ipcp_list

    def _get_poas_for_ipcp(self, ipcp_name: str) -> list[str]:

        path = os.path.join(self._get_dir_for_ipcp(ipcp_name), "poa")
        if not os.path.exists(path):
            return []

        try:
            return [entry.name for entry in os.scandir(path)]
        except IOError as e:
            print(e)

            return []

    def get_poa_stats_for_ipcp(self, ipcp_name: str) -> dict:
        """
        Get the transport statistics of each PoA of an IPCP.
        :param ipcp_name: name of the IPCP
        :return: {poa name: {metric: value}}
        """

        str_to_metric = {
            "Active flows": "n_flows",
            "Packets received": "n_rcv",
            "Bytes received": "n_rcv_bytes",
            "Packets sent": "n_snd",
            "Bytes sent": "n_snd_bytes",
            "Management frames rcvd": "n_mgmt_rcv",
            "Management frames sent": "n_mgmt_snd",
            "Bad EID packets": "n_bad_id",
            "Delivery (N+1) failures": "n_dlv_f",
            "Buffer alloc failures": "n_buf_f",
            "Packet read failures": "n_rcv_f",
            "Packet send failures": "n_snd_f",
            "Name queries sent": "n_qry_snd",
            "Name queries received": "n_qry_rcv",
            "Name replies sent": "n_rep_snd",
            "Name replies received": "n_rep_rcv",
            "Socket sndbuf (bytes)": "sock_sndbuf",
            "Socket rcvbuf (bytes)": "sock_rcvbuf",
            "Queued (transport)": "qlen_bytes",
            "Queued (packets)": "qlen_pkts",
            "Mean packet size (bytes)": "avg_len",
            "Mean packet cost": "avg_cost",
            "Kernel packets received": "kern_rcv",
            "Kernel packets dropped": "kern_drp",
            "Qdisc backlog (bytes)": "qdisc_bytes",
            "Qdisc backlog (packets)": "qdisc_pkts",
        }

        ret = {}

        for poa in self._get_poas_for_ipcp(ipcp_name):
            path = os.path.join(self._get_dir_for_ipcp(ipcp_name), "poa", poa)
            stats = {}

            try:
                with open(path, encoding="utf-8") as f:
                    for line in f.readlines():
                        split_line = line.split(":")

                        phrase = split_line[0].strip()
                        if phrase not in str_to_metric:
                            continue
                        stats[str_to_metric[phrase]] = self._get_trailing_number(split_line[-1])
            except IOError:
                continue

            if stats:
                ret[poa] = stats

        return ret

    def get_process_list(self) -> list[str]:
        """
        Get a list of all Ouroboros application processes in the RIB.
        :return: list of process names ("proc.<pid>")
        """

        proc_list = []

        if not os.path.exists(self.rib_path):
            return []

        for entry in os.scandir(self.rib_path):
            if entry.is_dir() and entry.name.startswith("proc."):
                proc_list += [entry.name]

        return proc_list
