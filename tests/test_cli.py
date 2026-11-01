import pytest

from dbt_hodor.cli import main


def test_ddl_delta(capsys):
    assert main(["ddl", "--database", "my_catalog", "--schema", "ops"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("CREATE TABLE IF NOT EXISTS `my_catalog`.`ops`.`hodor_state` (")
    assert out.rstrip().endswith("CLUSTER BY (target_name, node_key);")
    assert "node_hashes STRING" in out


def test_ddl_duckdb_defaults_to_main(capsys):
    main(["ddl", "--store", "duckdb"])
    assert '"main"."hodor_state"' in capsys.readouterr().out


def test_ddl_rejects_bad_names_and_missing_parts():
    with pytest.raises(SystemExit):
        main(["ddl", "--database", "c", "--schema", "s`; drop table x"])
    with pytest.raises(SystemExit):
        main(["ddl", "--store", "delta", "--schema", "s"])
    with pytest.raises(SystemExit):
        main(["ddl", "--store", "postgres"])  # Postgres has no schema "main"


def test_prune_rejects_negative_days():
    # -1 would move the cutoff into the future and delete today's records
    with pytest.raises(SystemExit):
        main(["prune", "--keep-days", "-1", "--yes"])


@pytest.mark.parametrize(
    ("store", "table"),
    [
        ("delta", "`iceberg`.`ops`.`hodor_state`"),
        ("trino", '"iceberg"."ops"."hodor_state"'),
        ("duckdb", '"ops"."hodor_state"'),
    ],
)
def test_ddl_quotes_the_table_per_store(capsys, store, table):
    args = ["ddl", "--store", store, "--database", "iceberg", "--schema", "ops"]
    assert main(args) == 0
    assert f"CREATE TABLE IF NOT EXISTS {table} (" in capsys.readouterr().out


def test_ddl_trino(capsys):
    assert main(["ddl", "--store", "trino", "--database", "iceberg", "--schema", "ops"]) == 0
    assert "record_id varchar" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        main(["ddl", "--store", "trino", "--schema", "ops"])


def test_explain_renders_env_var_in_a_new_process(tmp_path):
    # dbt renders env_var only inside an invocation context: a fresh `dbt-hodor explain`
    # process has none unless the CLI sets one
    import os
    import subprocess
    import sys

    pytest.importorskip("dbt.adapters.duckdb")
    from dbt_hodor.store.duckdb import DuckDBStore

    DuckDBStore(str(tmp_path / "state.duckdb")).close()
    (tmp_path / "dbt_project.yml").write_text('name: p\nversion: "1.0"\nprofile: p\n')
    (tmp_path / "profiles.yml").write_text(
        "p:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n"
        "      path: \"{{ env_var('CLI_WH') }}\"\n      hodor_store: duckdb\n"
        "      hodor_path: \"{{ env_var('CLI_STATE') }}\"\n"
    )
    env = dict(
        os.environ,
        CLI_WH=str(tmp_path / "wh.duckdb"),
        CLI_STATE=str(tmp_path / "state.duckdb"),
    )
    code = "import sys; from dbt_hodor.cli import main; sys.exit(main(sys.argv[1:]))"
    args = ["explain", "--project-dir", str(tmp_path), "--profiles-dir", str(tmp_path)]
    proc = subprocess.run(
        [sys.executable, "-c", code, *args], env=env, capture_output=True, text=True, timeout=120
    )
    # rendered: the store at CLI_STATE opens, and holds no records yet
    assert "no records for p/dev" in proc.stderr, proc.stderr[-1500:]
