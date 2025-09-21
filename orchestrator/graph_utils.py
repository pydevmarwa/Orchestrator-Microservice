import json
from collections import defaultdict, deque
from typing import Dict, Set, List, Tuple, Any

def load_objects(path: str) -> List[dict]:
    """Load objects from a JSON file.

    Returns:
        A list of dictionaries parsed from the given JSON file path.

    Raises:
        Any IO / JSON decoding exceptions are propagated to the caller.
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def parse_parents(raw: Any) -> List[str]:
    """
    Normalize the IdObjet_Parent field into a list of canonical token strings.

    This handles several real-world input shapes:
      - None -> []
      - int -> ["<int>"]
      - list/tuple -> coerces each element to a string, extracts digits when present
      - string with separators (comma or semicolon) -> split into tokens, strip whitespace
      - mixed garbage (e.g. 'garbage2003x') -> extract digits and return them as string

    Returns:
        A list of normalized parent identifier strings. Empty list if there are no parents.
    """
    if raw is None:
        return []
    if isinstance(raw, int):
        return [str(raw)]
    if isinstance(raw, (list, tuple)):
        res = []
        for x in raw:
            if x is None:
                continue
            s = str(x).strip()
            if not s:
                continue
            digits = ''.join(ch for ch in s if ch.isdigit())
            if digits:
                res.append(str(int(digits)))
            else:
                res.append(s)
        return res
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.replace(';', ',').split(',') if p.strip()]
        res = []
        for p in parts:
            digits = ''.join(ch for ch in p if ch.isdigit())
            if digits:
                res.append(str(int(digits)))
            else:
                if p:
                    res.append(p)
        return res
    s = str(raw).strip()
    if not s:
        return []
    digits = ''.join(ch for ch in s if ch.isdigit())
    if digits:
        return [str(int(digits))]
    return [s]

def build_full_graph(objs: List[dict]) -> Tuple[Dict[str, Set[str]], Dict[str, dict]]:
    """
    Build a parents_map and metadata map from a list of object definitions.

    Canonical id selection rules:
      - prefer IdObjet when numeric-like
      - else prefer 'id' field if present
      - else fallback to NomObjet/NomFichier or object identity

    The returned parents_map maps canonical_id -> set(parent_canonical_ids).
    The meta dict maps canonical_id -> original object dict.

    This function also attempts to resolve parent tokens that might refer to either
    IdObjet or id fields in the original JSON.

    Returns:
        (parents_map, meta)
    """
    id_by_idobjet: Dict[str, str] = {}
    id_by_id: Dict[str, str] = {}
    meta: Dict[str, dict] = {}

    for o in objs:
        idobjet = o.get("IdObjet")
        altid = o.get("id")
        if idobjet is not None:
            try:
                canonical = str(int(idobjet)) if isinstance(idobjet, (int, float)) or (isinstance(idobjet, str) and str(idobjet).isdigit()) else str(idobjet)
            except Exception:
                canonical = str(idobjet)
        elif altid is not None:
            try:
                canonical = str(int(altid)) if isinstance(altid, (int, float)) or (isinstance(altid, str) and str(altid).isdigit()) else str(altid)
            except Exception:
                canonical = str(altid)
        else:
            canonical = str(o.get("NomObjet") or o.get("NomFichier") or id(o))

        meta[canonical] = o
        if idobjet is not None:
            try:
                id_by_idobjet[str(int(idobjet))] = canonical
            except Exception:
                id_by_idobjet[str(idobjet)] = canonical
        if altid is not None:
            id_by_id[str(altid)] = canonical

    def resolve_parent_token(token: str) -> str:
        """Resolve a parent token (possibly noisy) to a canonical id when possible.

        Resolution order:
          - exact numeric match on IdObjet
          - exact match on id
          - extract digits and try those
          - fallback to token string
        """
        if token is None:
            return None
        t = str(token).strip()
        if not t:
            return None
        if t.isdigit():
            if t in id_by_idobjet:
                return id_by_idobjet[t]
            if t in id_by_id:
                return id_by_id[t]
            return t
        digits = ''.join(ch for ch in t if ch.isdigit())
        if digits:
            if digits in id_by_idobjet:
                return id_by_idobjet[digits]
            if digits in id_by_id:
                return id_by_id[digits]
            return digits
        if t in id_by_id:
            return id_by_id[t]
        return t

    parents_map: Dict[str, Set[str]] = defaultdict(set)

    for canonical, o in list(meta.items()):
        raw_parents = o.get("IdObjet_Parent")
        tokens = parse_parents(raw_parents)
        for tk in tokens:
            resolved = resolve_parent_token(tk)
            if resolved:
                parents_map[canonical].add(str(resolved))
        if canonical not in parents_map:
            parents_map[canonical] = parents_map.get(canonical, set())

    # Ensure all referenced parents exist as keys (empty set if unknown)
    for node, parents in list(parents_map.items()):
        for p in parents:
            if p not in parents_map:
                parents_map[p] = set()

    return parents_map, meta

def collect_subgraph_for_terminals(parents_map: Dict[str, Set[str]], meta: Dict[str, dict]) -> Tuple[Dict[str, Set[str]], List[str]]:
    """
    Collect the subgraph reachable from terminal nodes (application layer).

    Terminals are identified by meta entries where Zone contains '3-Application'
    or by ids that start with '3'.

    Returns:
        (sub_parents_map, terminals_list) where sub_parents_map includes only nodes
        reachable to the terminals (parents restricted to the reachable set).
    """
    terminals = [nid for nid, m in meta.items() if (m.get("Zone") and "3-Application" in m.get("Zone")) or str(nid).startswith("3")]
    needed = set()
    stack = list(terminals)
    while stack:
        n = stack.pop()
        if n in needed:
            continue
        needed.add(n)
        for p in parents_map.get(n, []):
            if p not in needed:
                stack.append(p)
    sub = {n: set(parents_map.get(n, set())) & needed for n in needed}
    return sub, terminals

def topological_sort(parents_map: Dict[str, Set[str]]) -> List[str]:
    """
    Perform a topological sort on a graph represented by parents_map (node -> set(parents)).

    The returned order guarantees that all parents appear before their children.
    Raises RuntimeError if a cycle or unresolved dependency is detected.

    Returns:
        A list of node ids in topologically sorted order.
    """
    children = defaultdict(set)
    indeg = {n: len(parents) for n, parents in parents_map.items()}
    for n, parents in parents_map.items():
        for p in parents:
            children[p].add(n)
    q = deque([n for n, d in indeg.items() if d == 0])
    order = []
    while q:
        n = q.popleft()
        order.append(n)
        for c in list(children.get(n, [])):
            indeg[c] -= 1
            if indeg[c] == 0:
                q.append(c)
    if len(order) != len(indeg):
        remaining = set(indeg.keys()) - set(order)
        raise RuntimeError(f"Cycle or unresolved dependencies: {remaining}")
    return order
