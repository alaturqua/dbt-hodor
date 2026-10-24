import pytest

from dbt_hodor.cli import main


def test_ddl_delta(capsys):
    assert main(["ddl", "--database", "my_catalog", "--schema", "ops"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("CREATE TABLE IF NOT EXISTS `my_catalog`.`ops`.`hodor_state` (")
    assert out.rstrip().endswith("CLUSTER BY (target_name, node_key);")


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


def test_ddl_trino(capsys):
    assert main(["ddl", "--store", "trino", "--database", "iceberg", "--schema", "ops"]) == 0
    out = capsys.readouterr().out
    assert 'CREATE TABLE IF NOT EXISTS "iceberg"."ops"."hodor_state"' in out
    assert "record_id varchar" in out
    with pytest.raises(SystemExit):
        main(["ddl", "--store", "trino", "--schema", "ops"])
