"""LAN helpers: persist camera MAC, find a new IP when the old one dies."""

from __future__ import annotations

import ipaddress
import re
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, List, Optional, Set
from urllib.parse import quote, urlparse

_MAC_RE = re.compile(r"([0-9A-Fa-f]{2})[:-]([0-9A-Fa-f]{2})[:-]([0-9A-Fa-f]{2})[:-]([0-9A-Fa-f]{2})[:-]([0-9A-Fa-f]{2})[:-]([0-9A-Fa-f]{2})")
_ARP_PATH = "/proc/net/arp"


def normalize_mac(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    m = _MAC_RE.search(str(value).strip())
    if not m:
        return None
    return ":".join(p.lower() for p in m.groups())


def replace_host_in_url(url: str, old_ip: Optional[str], new_ip: str) -> str:
    src = str(url or "")
    if old_ip and old_ip in src:
        return src.replace(old_ip, new_ip, 1)
    parsed = urlparse(src)
    if not parsed.scheme:
        return src
    userinfo = ""
    if parsed.username:
        userinfo = quote(parsed.username, safe="")
        if parsed.password is not None:
            userinfo += ":" + quote(parsed.password, safe="")
        userinfo += "@"
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{userinfo}{new_ip}{port}"
    return parsed._replace(netloc=netloc).geturl()


def mac_from_arp(ip: str) -> Optional[str]:
    ip = str(ip or "").strip()
    if not ip:
        return None
    try:
        with open(_ARP_PATH, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()[1:]
    except OSError:
        return None
    for line in lines:
        parts = line.split()
        if len(parts) < 4:
            continue
        if parts[0] == ip:
            return normalize_mac(parts[3])
    return None


def arp_table() -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        with open(_ARP_PATH, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()[1:]
    except OSError:
        return out
    for line in lines:
        parts = line.split()
        if len(parts) < 4:
            continue
        mac = normalize_mac(parts[3])
        if mac:
            out[mac] = parts[0]
    return out


def tcp_probe(ip: str, port: int, timeout: float = 0.25) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except OSError:
        return False


def _default_local_ipv4() -> Optional[str]:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1.0)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if ip and not ip.startswith("127."):
            return ip
    except OSError:
        pass
    return None


def candidate_networks(hint_ip: Optional[str] = None) -> List[ipaddress.IPv4Network]:
    nets: List[ipaddress.IPv4Network] = []
    seen: Set[str] = set()

    def _add(ip: Optional[str]) -> None:
        if not ip:
            return
        try:
            net = ipaddress.ip_network(f"{ip}/24", strict=False)
        except ValueError:
            return
        key = str(net)
        if key in seen:
            return
        seen.add(key)
        nets.append(net)

    _add(hint_ip)
    _add(_default_local_ipv4())
    return nets


def _scan_hosts(hosts: Iterable[str], port: int, timeout: float, workers: int) -> None:
    with ThreadPoolExecutor(max_workers=max(4, workers)) as pool:
        futs = [pool.submit(tcp_probe, ip, port, timeout) for ip in hosts]
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception:
                pass


def find_ip_for_mac(
    mac: str,
    *,
    hint_ip: Optional[str] = None,
    port: int = 554,
    timeout: float = 0.2,
    workers: int = 48,
    skip_ips: Optional[Set[str]] = None,
) -> Optional[str]:
    want = normalize_mac(mac)
    if not want:
        return None
    skip = set(skip_ips or ())
    table = arp_table()
    found = table.get(want)
    if found and found not in skip:
        return found

    hosts: List[str] = []
    for net in candidate_networks(hint_ip):
        for host in net.hosts():
            ip = str(host)
            if ip in skip:
                continue
            hosts.append(ip)
    _scan_hosts(hosts, port, timeout, workers)
    time.sleep(0.2)
    table = arp_table()
    found = table.get(want)
    if found and found not in skip:
        return found
    return None


def learn_mac_for_ip(
    ip: str,
    *,
    port: int = 554,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> Optional[str]:
    ip = str(ip or "").strip()
    if not ip:
        return None
    tcp_probe(ip, port or 554, timeout=0.8)
    mac = mac_from_arp(ip)
    if mac:
        return mac
    return _mac_from_isapi(ip, username, password)


def _mac_from_isapi(ip: str, username: Optional[str], password: Optional[str]) -> Optional[str]:
    if not username:
        return None
    try:
        import requests
        from requests.auth import HTTPBasicAuth, HTTPDigestAuth
    except ImportError:
        return None
    urls = (
        f"http://{ip}/ISAPI/System/Network/interfaces",
        f"http://{ip}/ISAPI/System/deviceInfo",
    )
    auths = [HTTPBasicAuth(username, password or ""), HTTPDigestAuth(username, password or "")]
    for url in urls:
        for auth in auths:
            try:
                resp = requests.get(url, auth=auth, timeout=3.0, verify=False)
            except Exception:
                continue
            if resp.status_code >= 400 or not resp.text:
                continue
            mac = normalize_mac(resp.text)
            if mac:
                return mac
    return None
