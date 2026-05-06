import importlib.util
import sys
from pathlib import Path
from unittest import TestCase


def carregar_monitor():
    caminho = Path(__file__).resolve().parents[1] / "scripts" / "site_monitor.py"
    spec = importlib.util.spec_from_file_location("site_monitor", caminho)
    modulo = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = modulo
    spec.loader.exec_module(modulo)
    return modulo


class SiteMonitorTests(TestCase):
    def test_normalize_base_url_remove_barra_final(self):
        monitor = carregar_monitor()

        self.assertEqual(
            monitor.normalize_base_url("https://wagenestetica.duckdns.org/"),
            "https://wagenestetica.duckdns.org",
        )

    def test_build_report_marca_falha_quando_algum_check_falha(self):
        monitor = carregar_monitor()

        report = monitor.build_report(
            "https://wagenestetica.duckdns.org/",
            [
                monitor.CheckResult("Login", True, 200, 120, "HTTP 200"),
                monitor.CheckResult("Status PWA", False, 500, 300, "HTTP 500"),
            ],
        )

        self.assertIn("Monitor Wagen Estetica: FALHA", report)
        self.assertIn("OK - Login", report)
        self.assertIn("FALHOU - Status PWA", report)
