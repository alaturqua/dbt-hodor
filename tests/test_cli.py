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
