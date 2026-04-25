import httpx


def test_api():
    BASE_URL = "http://127.0.0.1:8002"
    client = httpx.Client(timeout=30.0)

    print("Test 1: Health check")
    r = client.get(f"{BASE_URL}/health")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"
    print("  PASS")

    print("Test 2: Upload CSV")
    with open("tests/sample_data.csv", "rb") as f:
        files = {"file": ("sample_data.csv", f, "text/csv")}
        r = client.post(f"{BASE_URL}/upload_csv", files=files)
    assert r.status_code == 200
    upload_resp = r.json()
    session_id = upload_resp["session_id"]
    print(f"  Session: {session_id}")
    print("  PASS")

    print("Test 3: Get Schema")
    r = client.get(f"{BASE_URL}/schema/{session_id}")
    assert r.status_code == 200
    schema = r.json()
    print(f"  Columns: {schema['columns']}")
    print(f"  Types: {schema['inferred_types']}")
    print("  PASS")

    print("Test 4: Get Data Quality")
    r = client.get(f"{BASE_URL}/data_quality/{session_id}")
    assert r.status_code == 200
    quality = r.json()
    print(f"  Total rows: {quality['total_rows']}")
    print(f"  Valid rows: {quality['valid_rows']}")
    print(f"  Missingness: {quality['missingness']}")
    print(f"  Outliers: {quality['outliers']}")
    print("  PASS")

    print("Test 5: Query Data")
    query = {"session_id": session_id, "sql": "SELECT * FROM clean_dataset LIMIT 5"}
    r = client.post(f"{BASE_URL}/query", json=query)
    assert r.status_code == 200
    result = r.json()
    print(f"  Row count: {result['row_count']}")
    print("  PASS")

    print("\n=== API E2E TEST PASSED ===")


if __name__ == "__main__":
    test_api()