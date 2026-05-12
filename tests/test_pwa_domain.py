import unittest

from domains.pwa import montar_manifesto_pwa, montar_status_pwa


class PwaDomainTests(unittest.TestCase):
    def test_montar_manifesto_pwa_usa_identidade_do_produto(self):
        manifest = montar_manifesto_pwa({
            "site_title": "Portal Wagen",
            "brand_name": "Wagen Estetica Premium",
            "brand_background_color": "#101010",
            "brand_primary_color": "#f5c542",
        })

        self.assertEqual(manifest["name"], "Portal Wagen")
        self.assertEqual(manifest["short_name"], "Wagen Estetica Premium")
        self.assertEqual(manifest["background_color"], "#101010")
        self.assertEqual(manifest["theme_color"], "#f5c542")
        self.assertEqual(manifest["icons"][0]["src"], "/static/icon-192.jpg")

    def test_montar_status_pwa_aceita_https_e_localhost(self):
        self.assertTrue(montar_status_pwa(request_is_secure=True, host="app.exemplo.com")["ok"])
        self.assertTrue(montar_status_pwa(request_is_secure=False, host="localhost:5000")["ok"])
        self.assertFalse(montar_status_pwa(request_is_secure=False, host="wagen.exemplo.com")["ok"])


if __name__ == "__main__":
    unittest.main()
