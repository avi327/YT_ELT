def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"


def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHEESE"


def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 5432
    assert conn.schema == "mock_db_name"

def test_dags_integrity(dagbag):
    assert dagbag.import_errors == {}, f"import errors found in DAGs: {dagbag.import_errors}"
    print("#############")
    print(dagbag.import_errors)

    
    expected_dags = ['produce_json', 'update_db', 'data_quality']
    loaded_dags = list(dagbag.dags.keys())
    print("#############")
    print(dagbag.dags.keys())
    for dag_id in expected_dags:
        assert dag_id in loaded_dags, f"DAG '{dag_id}' is not loaded in the DagBag"

    assert dagbag.size() == 3 
    print("#############")
    print(dagbag.size())
