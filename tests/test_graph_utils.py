import pytest
from orchestrator.graph_utils import parse_parents, build_full_graph, topological_sort

def test_parse_parents_various():
    assert parse_parents(None) == []
    assert parse_parents(1001) == ["1001"]
    assert parse_parents("1001,2002, 3003") == ["1001", "2002", "3003"]
    assert parse_parents("2001;2002") == ["2001", "2002"]
    assert parse_parents([1001, "2002"]) == ["1001", "2002"]
    assert parse_parents("garbage2003x") == ["2003"]

def test_build_and_topo_simple():
    objs = [
        {"IdObjet": 1001, "IdObjet_Parent": None, "NomObjet": "A"},
        {"IdObjet": 2001, "IdObjet_Parent": "1001", "NomObjet": "B"},
        {"IdObjet": 3001, "IdObjet_Parent": "2001", "NomObjet": "C"},
    ]
    parents_map, meta = build_full_graph(objs)
    order = topological_sort(parents_map)
    assert order.index("1001") < order.index("2001")
    assert order.index("2001") < order.index("3001")

def test_topo_cycle_detection():
    parents_map = {"a": set(["b"]), "b": set(["a"])}
    with pytest.raises(RuntimeError):
        topological_sort(parents_map)
