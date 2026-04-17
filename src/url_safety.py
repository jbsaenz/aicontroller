"""URL safety helpers for external ingestion sources."""

from __future__ import annotations

import ipaddress
import os
import socket
from urllib.parse import urlparse


ALLOWED_SCHEMES = {"http", "https"}
BLOCKED_HOSTNAMES = {
    "localhost",
    "localhost.localdomain",
    "metadata",
    "metadata.google.internal",
}
BLOCKED_METADATA_IPS = {
    ipaddress.ip_address("169.254.169.254"),  # AWS/Azure IMDS
    ipaddress.ip_address("100.100.100.200"),  # Alibaba metadata
}


class UnsafeURLError(ValueError):
    """Raised when a URL is not safe for outbound requests."""


def _parse_allowlist(raw_allowlist: str) -> list[str]:
    return [entry.strip().lower() for entry in raw_allowlist.split(",") if entry.strip()]


def get_source_allowlist() -> list[str]:
    """Return normalized host patterns from API_SOURCE_ALLOWLIST."""
    return _parse_allowlist(os.getenv("API_SOURCE_ALLOWLIST", ""))


def get_automator_allowlist() -> list[str]:
    """Return normalized host patterns from AUTOMATOR_ENDPOINT_ALLOWLIST."""
    return _parse_allowlist(os.getenv("AUTOMATOR_ENDPOINT_ALLOWLIST", ""))


def _host_in_allowlist(hostname: str, allowlist: list[str]) -> bool:
    if not allowlist:
        return True

    host = hostname.lower()
    for pattern in allowlist:
        if pattern.startswith("*."):
            base = pattern[2:]
            if host == base or host.endswith(f".{base}"):
                return True
            continue
        if host == pattern:
            return True
    return False


def _normalize_resolved_ip(ip_text: str) -> ipaddress._BaseAddress:
    # Drop scope suffix when present in IPv6 link-local addresses.
    return ipaddress.ip_address(ip_text.split("%", 1)[0])


def _is_blocked_ip(ip: ipaddress._BaseAddress) -> bool:
    if ip in BLOCKED_METADATA_IPS:
        return True
    # is_global excludes private, loopback, link-local, multicast, etc.
    return not ip.is_global


def validate_source_url(url: str) -> None:
    """Validate URL scheme, host allowlist, and DNS-resolved egress targets."""
    result = inspect_source_url(url)
    if not result["valid"]:
        raise UnsafeURLError("; ".join(result["errors"]))


def validate_automator_url(url: str) -> None:
    """Validate automator endpoint URL before command dispatch."""
    result = inspect_automator_url(url)
    if not result["valid"]:
        raise UnsafeURLError("; ".join(result["errors"]))


def _inspect_url(
    *,
    url: str,
    allowlist: list[str],
    allowlist_env_name: str,
) -> dict[str, object]:
    """Return detailed validation diagnostics for a candidate URL."""
    parsed = urlparse((url or "").strip())
    errors: list[str] = []

    result: dict[str, object] = {
        "url": (url or "").strip(),
        "scheme": parsed.scheme or None,
        "hostname": parsed.hostname.lower() if parsed.hostname else None,
        "port": parsed.port,
        "allowlist": allowlist,
        "allowlist_configured": bool(allowlist),
        "resolved_ips": [],
        "blocked_ips": [],
        "errors": errors,
        "valid": False,
    }

    if parsed.scheme not in ALLOWED_SCHEMES:
        errors.append("Only http/https source URLs are allowed")
    if parsed.hostname is None:
        errors.append("Source URL must include a hostname")

    host = (parsed.hostname or "").lower()
    if host and host in BLOCKED_HOSTNAMES:
        errors.append("Source hostname is blocked")

    if not allowlist:
        errors.append(
            f"{allowlist_env_name} is empty; outbound URLs are disabled"
        )
    elif "*" in allowlist:
        errors.append(
            f"{allowlist_env_name} cannot include wildcard '*'"
        )
    elif host and not _host_in_allowlist(host, allowlist):
        errors.append(f"Source hostname is not in {allowlist_env_name}")

    if host and parsed.scheme in ALLOWED_SCHEMES:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        result["port"] = port
        try:
            addr_info = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            errors.append(f"Could not resolve source hostname: {exc}")
            addr_info = []

        resolved_ips = sorted(
            {
                str(_normalize_resolved_ip(info[4][0]))
                for info in addr_info
                if info and len(info) >= 5 and info[4]
            }
        )
        result["resolved_ips"] = resolved_ips
        if not resolved_ips:
            errors.append("Source hostname resolved to no usable addresses")
        else:
            blocked_ips = [
                ip for ip in resolved_ips if _is_blocked_ip(ipaddress.ip_address(ip))
            ]
            result["blocked_ips"] = blocked_ips
            if blocked_ips:
                errors.append(
                    "Source hostname resolves to blocked address space: "
                    + ", ".join(blocked_ips)
                )

    result["valid"] = not errors
    return result


def inspect_source_url(url: str) -> dict[str, object]:
    """Return detailed validation diagnostics for a candidate source URL."""
    return _inspect_url(
        url=url,
        allowlist=get_source_allowlist(),
        allowlist_env_name="API_SOURCE_ALLOWLIST",
    )


def inspect_automator_url(url: str) -> dict[str, object]:
    """Return detailed validation diagnostics for automator endpoint URL."""
    return _inspect_url(
        url=url,
        allowlist=get_automator_allowlist(),
        allowlist_env_name="AUTOMATOR_ENDPOINT_ALLOWLIST",
    )
