from dbt_hodor.hashing import SqlAnalysis
from dbt_hodor.lineage import GraphNode, Lineage, external_inputs, normalize_relation

GRAPH = {
    "source.p.raw.orders": GraphNode("source.p.raw.orders", "source", relation="c.raw.orders"),
    "source.p.raw.users": GraphNode("source.p.raw.users", "source", relation="c.raw.users"),
    "model.p.stg_orders": GraphNode(
        "model.p.stg_orders", "model", "view", "c.s.stg_orders", ("source.p.raw.orders",)
    ),
    "model.p.eph_users": GraphNode(
        "model.p.eph_users", "model", "ephemeral", None, ("source.p.raw.users",)
    ),
    "model.p.both": GraphNode(
        "model.p.both", "model", "view", "c.s.both", ("model.p.stg_orders", "model.p.eph_users")
    ),
    "model.p.fct": GraphNode(
        "model.p.fct", "model", "incremental", "c.s.fct", ("model.p.both", "model.p.dim")
    ),
    "model.p.dim": GraphNode("model.p.dim", "model", "table", "c.s.dim", ("source.p.raw.users",)),
    "model.p.mv": GraphNode("model.p.mv", "model", "materialized_view", "c.s.mv", ()),
    "model.p.on_mv": GraphNode("model.p.on_mv", "model", "view", "c.s.on_mv", ("model.p.mv",)),
    "model.p.loop_a": GraphNode("model.p.loop_a", "model", "view", "c.s.a", ("model.p.loop_b",)),
    "model.p.loop_b": GraphNode("model.p.loop_b", "model", "view", "c.s.b", ("model.p.loop_a",)),
}


def test_normalize_relation():
    assert normalize_relation('`Cat`.`Sch`."T"') == "cat.sch.t"


def test_sources_and_own_data_models_are_their_own_tables():
    lineage = Lineage(GRAPH)
    assert lineage.base_tables("source.p.raw.orders") == {"c.raw.orders"}
    assert lineage.base_tables("model.p.dim") == {"c.s.dim"}


def test_views_and_ephemerals_expand_recursively():
    lineage = Lineage(GRAPH)
    assert lineage.base_tables("model.p.both") == {"c.raw.orders", "c.raw.users"}


def test_upstream_groups_per_direct_parent():
    assert Lineage(GRAPH).upstream_groups("model.p.fct") == {
        "model.p.both": {"c.raw.orders", "c.raw.users"},
        "model.p.dim": {"c.s.dim"},
    }


def test_unsupported_missing_and_cyclic_nodes_are_unknown():
    lineage = Lineage(GRAPH)
    assert lineage.base_tables("model.p.mv") is None
    assert lineage.base_tables("model.p.on_mv") is None
    assert lineage.base_tables("model.p.nope") is None
    assert lineage.base_tables("model.p.loop_a") is None


def test_base_tables_are_memoized():
    calls = []

    class Counting(Lineage):
        def _compute(self, unique_id):
            calls.append(unique_id)
            return super()._compute(unique_id)

    lineage = Counting(GRAPH)
    lineage.upstream_groups("model.p.fct")
    lineage.base_tables("model.p.both")
    assert calls.count("model.p.stg_orders") == 1


def test_external_inputs_are_what_lineage_does_not_cover():
    analysis = SqlAnalysis(
        "qualify:x", False, frozenset({"c.s.fct", "c.raw.orders", "c.ext.rates"}), frozenset({"/a"})
    )
    inputs = external_inputs(analysis, {"c.s.fct", "c.raw.orders"})
    assert inputs.tables == {"c.ext.rates"}
    assert inputs.delta_paths == {"/a"}
    assert inputs.unknown is False
    assert external_inputs(SqlAnalysis("x", False, opaque_inputs=True), set()).unknown is True
