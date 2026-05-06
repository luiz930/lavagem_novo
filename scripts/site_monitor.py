"""Monitora o site em producao e envia um resumo para o Telegram.

O script nao guarda token no repositorio. Configure por variaveis de ambiente:
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID e SITE_MONITOR_URL.
"""

from __future__ import annotations

import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


DEFAULT_SITE_URL = "https://wagenestetica.duckdns.org"
DEFAULT_TIMEOUT_SECONDS = 15


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    status: int | None
    elapsed_ms: int
    message: str


def normalize_base_url(url: str) -> str:
    return url.strip().rstrip("/")


def request_url(url: str, timeout: int) -> tuple[int | None, bytes, str]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "WagenEstetica-Monitor/1.0",
            "Accept": "text/html,application/json,*/*",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read(1024 * 256), response.geturl()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(1024 * 32), exc.geturl()


def run_check(name: str, url: str, timeout: int, expected_statuses: Iterable[int]) -> CheckResult:
    start = time.perf_counter()
    try:
        status, body, final_url = request_url(url, timeout)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        ok = status in set(expected_statuses)
        detail = f"HTTP {status}"
        if final_url and final_url != url:
            detail += f" -> {final_url}"
        if not body:
            detail += " sem corpo"
        return CheckResult(name=name, ok=ok, status=status, elapsed_ms=elapsed_ms, message=detail)
    except (TimeoutError, urllib.error.URLError, ssl.SSLError, OSError) as exc:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return CheckResult(name=name, ok=False, status=None, elapsed_ms=elapsed_ms, message=str(exc))


def run_site_checks(base_url: str, timeout: int) -> list[CheckResult]:
    base_url = normalize_base_url(base_url)
    return [
        run_check("Site HTTPS", base_url, timeout, {200, 302}),
        run_check("Login", f"{base_url}/login", timeout, {200}),
        run_check("Manifest PWA", f"{base_url}/site.webmanifest", timeout, {200}),
        run_check("Service Worker", f"{base_url}/sw.js", timeout, {200}),
        run_check("Status PWA", f"{base_url}/api/pwa/status", timeout, {200}),
    ]


def build_report(base_url: str, results: list[CheckResult]) -> str:
    now = datetime.now(timezone.utc).astimezone().strftime("%d/%m/%Y %H:%M:%S %Z")
    all_ok = all(result.ok for result in results)
    status_label = "OK" if all_ok else "FALHA"
    lines = [
        f"Monitor Wagen Estetica: {status_label}",
        f"Data: {now}",
        f"Site: {normalize_base_url(base_url)}",
        "",
    ]

    for result in results:
        icon = "OK" if result.ok else "FALHOU"
        lines.append(f"{icon} - {result.name}: {result.message} ({result.elapsed_ms} ms)")

    slow_results = [result for result in results if result.elapsed_ms >= 3000]
    if slow_results:
        lines.append("")
        lines.append("Atencao: resposta lenta em " + ", ".join(result.name for result in slow_results))

    return "\n".join(lines)


def send_telegram_message(token: str, chat_id: str, text: str, timeout: int) -> None:
    data = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=data,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        response.read()


def main() -> int:
    base_url = os.getenv("SITE_MONITOR_URL", DEFAULT_SITE_URL)
    timeout = int(os.getenv("SITE_MONITOR_TIMEOUT", str(DEFAULT_TIMEOUT_SECONDS)))
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    json_output = os.getenv("SITE_MONITOR_JSON", "").strip() == "1"

    results = run_site_checks(base_url, timeout)
    report = build_report(base_url, results)

    if json_output:
        print(
            json.dumps(
                {
                    "ok": all(result.ok for result in results),
                    "site": normalize_base_url(base_url),
                    "results": [result.__dict__ for result in results],
                    "report": report,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(report)

    if token and chat_id:
        send_telegram_message(token, chat_id, report, timeout)
    else:
        print("Telegram nao configurado: defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID.")

    return 0 if all(result.ok for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
