"""Tests for the platform file source node against a mock login + file server.

The mock follows the documented platform calls:
  POST /auth/.../token                     (password login -> access_token)
  POST /v1/databanks/{id}/files            (list, Bearer token)
  POST /v1/databanks/{id}/files/download   (-> data.presignedUrl, Bearer token)
  GET  /blob/{key}                         (the pre-signed link, no token)

Run inside the node's image:  python -m unittest discover -s tests -v
"""

import io
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.parse
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pipeline_sdk import util  # noqa: E402

import main  # noqa: E402

USER, PASSWORD = "tester@example.org", "s3cret-Pass!word"
DATABANK = "11111111-2222-3333-4444-555555555555"
CSV = b"id,name,age\n1,Asha,34\n2,Ravi,41\n3,Meera,29\n"
GEOJSON = json.dumps({"type": "FeatureCollection", "features": [
    {"type": "Feature", "properties": {"name": "Fort", "geometry": "x"}, "geometry": {"type": "Point", "coordinates": [78.04, 27.18]}},
    {"type": "Feature", "properties": {"name": "Gate", "height": 4}, "geometry": None},
]}).encode()
JSONL = b'{"id":1,"city":"Bengaluru"}\n{"id":2,"city":"Mysuru"}\n'


class State:
    def reset(self):
        self.tokens_issued = 0
        self.valid_token = None
        self.expire_next_token = False  # the first token answers 401 once, as an expired one would
        self.files = {"people.csv": CSV}
        self.requests = []
        self.link_expired = False


STATE = State()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _reply(self, code, payload):
        data = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        return raw.decode()

    def _authorised(self):
        return STATE.valid_token is not None and self.headers.get("Authorization") == f"Bearer {STATE.valid_token}"

    def do_POST(self):
        STATE.requests.append(self.path)
        body = self._body()
        if self.path.endswith("/protocol/openid-connect/token"):
            form = urllib.parse.parse_qs(body)
            ok = (
                form.get("grant_type") == ["password"]
                and form.get("client_id") == [main.CLIENT_ID]
                and form.get("username") == [USER]
                and form.get("password") == [PASSWORD]
            )
            if not ok:
                return self._reply(401, {"error": "invalid_grant", "error_description": "Invalid user credentials"})
            STATE.tokens_issued += 1
            STATE.valid_token = f"token-{STATE.tokens_issued}"
            return self._reply(200, {"access_token": STATE.valid_token, "expires_in": 300})
        if STATE.expire_next_token and STATE.tokens_issued == 1:
            STATE.expire_next_token = False
            STATE.valid_token = None  # the token the node holds is now stale
        if not self._authorised():
            return self._reply(401, {"success": False, "error": {"message": "Invalid token", "code": "UNAUTHORIZED"}})
        payload = json.loads(body or "{}")
        if self.path == f"/v1/databanks/{DATABANK}/files":
            files = [{"key": key, "size": len(data), "lastModified": "2026-10-26T00:00:00Z"} for key, data in STATE.files.items()]
            return self._reply(200, {"success": True, "data": {"files": files, "directories": []}})
        if self.path == f"/v1/databanks/{DATABANK}/files/download":
            key = payload.get("key")
            if key not in STATE.files:
                return self._reply(404, {"success": False, "error": {"message": "not found", "code": "RESOURCE_NOT_FOUND"}})
            port = self.server.server_port
            return self._reply(200, {"success": True, "data": {"presignedUrl": f"http://127.0.0.1:{port}/blob/{urllib.parse.quote(key)}", "expiresAt": "x"}})
        if f"/databanks/" in self.path and self.path.endswith("/files"):
            return self._reply(200, {"success": True, "data": {"files": [], "directories": []}})  # some other, empty databank
        return self._reply(404, {})

    def do_GET(self):
        if not self.path.startswith("/blob/"):
            return self._reply(404, {})
        key = urllib.parse.unquote(self.path[len("/blob/"):])
        if STATE.link_expired or key not in STATE.files:
            return self._reply(403, {"message": "expired"})
        data = STATE.files[key]
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class FakeOutput:
    path = "s3://artifacts/x/result.parquet"

    def __init__(self):
        self.uploaded = None

    def upload_file(self, path):
        with open(path, "rb") as handle:
            self.uploaded = handle.read()


class FakeContext:
    def __init__(self, config, output):
        self.config = config
        self.outputs = [output]


def make_gpkg(layers):
    """A real GeoPackage as bytes. layers: {name: (srs, point_sql)}; every extra layer is copied in with sqlite."""
    import sqlite3
    path = os.path.join(tempfile.mkdtemp(), "t.gpkg")
    connection = duckdb.connect()
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")
    names = list(layers)
    first = names[0]
    srs, point = layers[first]
    connection.execute(
        f"COPY (SELECT 1 AS id, 'Fort' AS name, {point} AS geom) TO '{path}' "
        f"(FORMAT GDAL, DRIVER 'GPKG', LAYER_NAME '{first}', SRS '{srs}')"
    )
    connection.close()
    if len(names) > 1:
        db = sqlite3.connect(path)
        for extra in names[1:]:
            db.execute(f'CREATE TABLE "{extra}" AS SELECT * FROM "{first}"')
            db.execute(
                "INSERT INTO gpkg_contents SELECT ?, data_type, ?, description, last_change, min_x, min_y, max_x, max_y, srs_id "
                "FROM gpkg_contents WHERE table_name = ?", (extra, extra, first))
            db.execute(
                "INSERT INTO gpkg_geometry_columns SELECT ?, column_name, geometry_type_name, srs_id, z, m "
                "FROM gpkg_geometry_columns WHERE table_name = ?", (extra, first))
        db.commit()
        db.close()
    with open(path, "rb") as handle:
        return handle.read()


def rows_of(parquet_bytes):
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        tmp.write(parquet_bytes)
        tmp.flush()
        return duckdb.sql(f"SELECT * FROM read_parquet('{tmp.name}') ORDER BY 1").fetchall()


class NodeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        STATE.reset()
        self.secrets = {"PLATFORM_USERNAME": USER, "PLATFORM_PASSWORD": PASSWORD}
        self.asked_for = []

        def fake_post_json(url, payload, **kwargs):
            self.asked_for.append(payload["name"])
            if payload["name"] not in self.secrets:
                raise util.ApiError("not found", status=404, detail="")
            return {"value": self.secrets[payload["name"]]}

        for patch in (
            mock.patch.object(util, "post_json", fake_post_json),
            mock.patch.dict(os.environ, {"BACKEND_URL": "http://backend.invalid", "API_KEY": "test-key"}),
        ):
            patch.start()
            self.addCleanup(patch.stop)
        self.workdir = tempfile.mkdtemp()

    def run_node(self, **config):
        config = {"databank_id": DATABANK, "file_server_url": f"{self.base}/v1", "token_url": f"{self.base}/auth/realms/iudx/protocol/openid-connect/token", **config}
        output = FakeOutput()
        captured = io.StringIO()
        try:
            with redirect_stdout(captured):
                result = main.handler(FakeContext(config, output), self.workdir)
        finally:
            self.output_text = captured.getvalue()
        return result, output

    # -- the normal path ---------------------------------------------------------
    def test_single_csv_becomes_parquet(self):
        result, output = self.run_node()
        self.assertEqual(rows_of(output.uploaded), [(1, "Asha", 34), (2, "Ravi", 41), (3, "Meera", 29)])
        self.assertEqual(result["rows"], 3)
        self.assertEqual(result["file"], "people.csv")

    def test_file_key_picks_one_of_several(self):
        STATE.files = {"a/people.csv": CSV, "b/places.jsonl": JSONL}
        result, output = self.run_node(file_key="b/places.jsonl")
        self.assertEqual(rows_of(output.uploaded), [(1, "Bengaluru"), (2, "Mysuru")])

    def test_a_parquet_file_is_kept_as_it_is(self):
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            duckdb.sql(f"COPY (SELECT 7 AS n) TO '{tmp.name}' (FORMAT PARQUET)")
        with open(tmp.name, "rb") as handle:
            STATE.files = {"nums.parquet": handle.read()}
        os.unlink(tmp.name)
        _, output = self.run_node()
        self.assertEqual(rows_of(output.uploaded), [(7,)])

    def test_a_geojson_file_becomes_rows_with_a_geometry_column(self):
        STATE.files = {"places.geojson": GEOJSON}
        _, output = self.run_node()
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            tmp.write(output.uploaded)
            tmp.flush()
            rows = duckdb.sql(f"SELECT name, height, properties_geometry, geometry FROM read_parquet('{tmp.name}') ORDER BY name").fetchall()
        self.assertEqual(rows, [("Fort", None, "x", '{"type":"Point","coordinates":[78.04,27.18]}'), ("Gate", 4, None, None)])

    def test_geojson_with_a_plain_json_name_is_read_as_geojson(self):
        STATE.files = {"places.json": GEOJSON}
        result, _ = self.run_node()
        self.assertEqual(result["rows"], 2)

    def test_ordinary_json_is_not_mistaken_for_geojson(self):
        STATE.files = {"rows.json": b'[{"id": 1, "kind": "Feature"}, {"id": 2, "kind": "Other"}]'}
        result, _ = self.run_node()
        self.assertEqual(result["rows"], 2)

    # -- all matching files ------------------------------------------------------
    def table(self, output):
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            tmp.write(output.uploaded)
            tmp.flush()
            return duckdb.sql(f"SELECT * FROM read_parquet('{tmp.name}') ORDER BY 1, 2").fetchall(), \
                [column[0] for column in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{tmp.name}')").fetchall()]

    def test_all_matching_files_are_stacked_with_their_source(self):
        STATE.files = {"d/a1.csv": CSV, "d/a2.csv": CSV, "d/other.jsonl": JSONL}
        result, output = self.run_node(all_files=True, file_key="d/a*.csv")
        rows, columns = self.table(output)
        self.assertEqual(result["rows"], 6)
        self.assertEqual(columns[-1], "_source_file")
        self.assertEqual({row[-1] for row in rows}, {"d/a1.csv", "d/a2.csv"})

    def test_parquet_files_can_be_stacked(self):
        files = {}
        for index in range(3):
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                duckdb.sql(f"COPY (SELECT {index} AS n, 'x' AS tag) TO '{tmp.name}' (FORMAT PARQUET)")
            with open(tmp.name, "rb") as handle:
                files[f"part{index}.parquet"] = handle.read()
            os.unlink(tmp.name)
        STATE.files = files
        result, output = self.run_node(all_files=True)
        rows, columns = self.table(output)
        self.assertEqual([row[0] for row in rows], [0, 1, 2])
        self.assertEqual(columns, ["n", "tag", "_source_file"])

    def test_a_folder_prefix_picks_the_files_in_it(self):
        STATE.files = {"in/a.csv": CSV, "out/b.csv": CSV}
        result, _ = self.run_node(all_files=True, file_key="in/")
        self.assertEqual(result["rows"], 3)

    def test_all_files_with_no_pattern_takes_everything_and_mixed_types_work(self):
        STATE.files = {"a.csv": CSV, "b.jsonl": JSONL}
        result, output = self.run_node(all_files=True)
        rows, columns = self.table(output)
        self.assertEqual(result["rows"], 5)
        self.assertEqual(set(columns), {"id", "name", "age", "city", "_source_file"})

    def test_many_files_are_loaded(self):
        STATE.files = {f"p{index:03}.csv": CSV for index in range(120)}
        result, _ = self.run_node(all_files=True)
        self.assertEqual(result["rows"], 360)

    def test_too_many_matching_files_stops_with_a_message(self):
        STATE.files = {f"p{index}.csv": CSV for index in range(5)}
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node(all_files=True, max_files=3)
        self.assertIn("more than the limit of 3", str(caught.exception))

    def test_a_pattern_that_matches_nothing_lists_the_files(self):
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node(all_files=True, file_key="zzz*")
        self.assertIn("people.csv", str(caught.exception))

    def test_a_pattern_without_the_switch_explains_itself(self):
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node(file_key="*.csv")
        self.assertIn("Load all matching files", str(caught.exception))

    def test_a_bad_file_among_many_is_named(self):
        STATE.files = {"a.csv": CSV, "b.gpkg": b"not a geopackage"}
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node(all_files=True)
        self.assertIn("'b.gpkg'", str(caught.exception))

    # -- GeoPackage ----------------------------------------------------------------
    def test_a_geopackage_becomes_rows_with_wgs84_geojson(self):
        STATE.files = {"places.gpkg": make_gpkg({"places": ("EPSG:4326", "ST_Point(78.04, 27.18)")})}
        result, output = self.run_node()
        rows, columns = self.table(output)
        self.assertEqual(rows, [(1, "Fort", '{"type":"Point","coordinates":[78.04,27.18]}')])
        self.assertEqual(columns, ["id", "name", "geometry"])

    def test_a_projected_geopackage_is_converted_to_longitude_latitude(self):
        STATE.files = {"utm.gpkg": make_gpkg({"places": ("EPSG:32643", "ST_Point(500000, 3000000)")})}
        _, output = self.run_node()
        rows, _ = self.table(output)
        longitude, latitude = json.loads(rows[0][2])["coordinates"]
        self.assertAlmostEqual(longitude, 75.0, places=1)  # the central meridian of zone 43
        self.assertAlmostEqual(latitude, 27.1, places=0)
        self.assertIn("EPSG:32643", self.output_text)

    def test_a_geopackage_with_several_layers_needs_a_choice(self):
        STATE.files = {"two.gpkg": make_gpkg({"places": ("EPSG:4326", "ST_Point(1, 2)"), "roads": ("EPSG:4326", "ST_Point(1, 2)")})}
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node()
        self.assertIn("places", str(caught.exception))
        self.assertIn("roads", str(caught.exception))
        result, _ = self.run_node(layer="roads")
        self.assertEqual(result["rows"], 1)

    def test_a_wrong_layer_name_lists_the_layers(self):
        STATE.files = {"one.gpkg": make_gpkg({"places": ("EPSG:4326", "ST_Point(1, 2)")})}
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node(layer="nope")
        self.assertIn("places", str(caught.exception))

    def test_a_file_that_is_not_geojson_is_refused(self):
        STATE.files = {"bad.geojson": b'{"type": "Banana"}'}
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node()
        self.assertIn("not a GeoJSON", str(caught.exception))

    def test_the_secret_names_can_be_changed(self):
        self.secrets = {"MY_USER": USER, "MY_PASS": PASSWORD}
        self.run_node(username_secret="MY_USER", password_secret="MY_PASS")
        self.assertEqual(self.asked_for, ["MY_USER", "MY_PASS"])

    def test_stray_whitespace_in_the_secrets_is_ignored(self):
        self.secrets = {"PLATFORM_USERNAME": f"  {USER}\n", "PLATFORM_PASSWORD": f"{PASSWORD}\r\n"}
        _, output = self.run_node()
        self.assertEqual(len(rows_of(output.uploaded)), 3)

    def test_a_username_that_is_not_an_email_is_flagged(self):
        self.secrets["PLATFORM_USERNAME"] = "someone"
        with self.assertRaises(main.PlatformError):
            self.run_node()
        self.assertIn("does not look like an email", self.output_text)

    # -- the token expires mid-run ------------------------------------------------
    def test_an_expired_token_triggers_one_new_login(self):
        STATE.expire_next_token = True
        _, output = self.run_node()
        self.assertEqual(STATE.tokens_issued, 2)
        self.assertEqual(len(rows_of(output.uploaded)), 3)

    # -- problems the user must be told about -------------------------------------
    def test_wrong_password_is_a_clear_error_without_leaking_it(self):
        self.secrets["PLATFORM_PASSWORD"] = "wrong-password-123"
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node()
        self.assertIn("Could not log in", str(caught.exception))
        self.assertNotIn("wrong-password-123", str(caught.exception) + self.output_text)

    def test_missing_secret_is_a_clear_error(self):
        del self.secrets["PLATFORM_USERNAME"]
        with self.assertRaises(Exception) as caught:
            self.run_node()
        self.assertIn("PLATFORM_USERNAME", str(caught.exception))

    def test_a_dataset_with_no_files_says_so(self):
        STATE.files = {}
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node()
        self.assertIn("no files", str(caught.exception))

    def test_several_files_without_a_choice_lists_them(self):
        STATE.files = {"a.csv": CSV, "b.csv": CSV}
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node()
        message = str(caught.exception)
        self.assertIn("a.csv", message)
        self.assertIn("b.csv", message)

    def test_a_wrong_file_key_lists_what_exists(self):
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node(file_key="nope.csv")
        self.assertIn("people.csv", str(caught.exception))

    def test_an_unsupported_file_type_is_refused(self):
        STATE.files = {"map.shp": b"not supported"}
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node()
        self.assertIn("not supported", str(caught.exception))

    def test_an_expired_download_link_is_explained(self):
        STATE.link_expired = True
        with self.assertRaises(main.PlatformError) as caught:
            self.run_node()
        self.assertIn("expired", str(caught.exception))

    def test_a_databank_id_is_required(self):
        with self.assertRaises(main.PlatformError):
            self.run_node(databank_id="")

    # -- nothing sensitive in the log -----------------------------------------------
    def test_neither_password_nor_token_is_logged(self):
        self.run_node()
        self.assertNotIn(PASSWORD, self.output_text)
        self.assertNotIn("token-1", self.output_text)


if __name__ == "__main__":
    unittest.main()
