"""LAN helpers: persist camera MAC, find a new IP when the old one dies."""

from __future__ import annotations

import ipaddress
import re
import shutil
import socket
import subprocess
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


def build_rtsp_url(
    ip: str,
    *,
    username: Optional[str] = None,
    password: Optional[str] = None,
    port: int = 554,
    path: str = "/",
) -> str:
    host = str(ip or "").strip()
    if not host:
        return ""
    rtsp_path = str(path or "/").strip()
    if not rtsp_path.startswith("/"):
        rtsp_path = "/" + rtsp_path
    port_suffix = f":{int(port)}" if port else ""
    if username and password is not None:
        return f"rtsp://{username}:{password}@{host}{port_suffix}{rtsp_path}"
    return f"rtsp://{host}{port_suffix}{rtsp_path}"


def rtsp_probe_reachable(url: str, *, timeout: float = 6.0) -> bool:
    """True when ffprobe can open the RTSP video stream."""
    raw = str(url or "").strip()
    if not raw.lower().startswith("rtsp"):
        return False
    ffprobe = shutil.which("ffprobe") or "ffprobe"
    cmd = [
        ffprobe,
        "-v",
        "error",
        "-rtsp_transport",
        "tcp",
        "-timeout",
        "5000000",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_type",
        "-of",
        "csv=p=0",
        raw,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=timeout, check=False)
        return result.returncode == 0 and bool((result.stdout or b"").strip())
    except (subprocess.TimeoutExpired, OSError):
        return False


def ip_rtsp_reachable(
    ip: str,
    *,
    username: Optional[str] = None,
    password: Optional[str] = None,
    port: int = 554,
    path: str = "/",
) -> bool:
    host = str(ip or "").strip()
    if not host:
        return False
    if not tcp_probe(host, int(port or 554), timeout=0.5):
        return False
    return rtsp_probe_reachable(
        build_rtsp_url(host, username=username, password=password, port=port, path=path)
    )


def find_rtsp_by_credentials(
    *,
    username: Optional[str] = None,
    password: Optional[str] = None,
    hint_ip: Optional[str] = None,
    port: int = 554,
    path: str = "/",
    skip_ips: Optional[Set[str]] = None,
    workers: int = 48,
) -> Optional[str]:
    """
    Find a camera on the local /24 when backend IP is stale.
    Prefer hint_ip, then other hosts with RTSP port open that accept credentials.

    Unsafe when multiple cameras share the same login/password — disabled by
    default via resolve_effective_ip(lan_scan=False). Prefer MAC rediscovery.
    """
    skip = set(skip_ips or ())
    candidates: List[str] = []
    seen: Set[str] = set()

    def _add(ip: Optional[str]) -> None:
        host = str(ip or "").strip()
        if not host or host in skip or host in seen:
            return
        seen.add(host)
        candidates.append(host)

    _add(hint_ip)
    open_hosts: List[str] = []
    for net in candidate_networks(hint_ip):
        for host in net.hosts():
            _add(str(host))

    with ThreadPoolExecutor(max_workers=max(4, workers)) as pool:
        probes = {pool.submit(tcp_probe, ip, int(port or 554), 0.25): ip for ip in candidates}
        for fut in as_completed(probes):
            ip = probes[fut]
            try:
                if fut.result():
                    open_hosts.append(ip)
            except Exception:
                pass

    for ip in open_hosts:
        if ip_rtsp_reachable(ip, username=username, password=password, port=port, path=path):
            return ip
    return None


def resolve_effective_ip(
    *,
    ip: Optional[str] = None,
    health_ip: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    port: int = 554,
    path: str = "/",
    mac: Optional[str] = None,
    lan_scan: bool = False,
    skip_ips: Optional[Set[str]] = None,
) -> Optional[str]:
    """
    Pick the IP both capture and streaming should use.
    Order: health_ip, configured ip, MAC rediscovery.
    Credential LAN scan is off by default (unsafe when cameras share login/password).
    """
    skip = set(skip_ips or ())
    candidates: List[str] = []
    for candidate in (health_ip, ip):
        host = str(candidate or "").strip()
        if host and host not in candidates:
            candidates.append(host)

    for host in candidates:
        if host in skip:
            continue
        if ip_rtsp_reachable(host, username=username, password=password, port=port, path=path):
            return host

    mac_ip = find_ip_for_mac(mac or "", hint_ip=ip or health_ip, port=port, skip_ips=skip)
    if mac_ip and mac_ip not in skip:
        if ip_rtsp_reachable(mac_ip, username=username, password=password, port=port, path=path):
            return mac_ip

    if not lan_scan:
        return None
    return find_rtsp_by_credentials(
        username=username,
        password=password,
        hint_ip=ip or health_ip,
        port=port,
        path=path,
        skip_ips=skip,
    )


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
