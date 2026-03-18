import json
import math
from collections import Counter
from pathlib import Path
from typing import Any

# ---------------- CONFIG ----------------

LIST_DIR = Path("person_to_files/")
ME_PATH = LIST_DIR / "me.json"
JMAIL_PATH = LIST_DIR / "jmail.json"
TOMMY_PATH = LIST_DIR / "tommy.json"

OUTPUT_DIR = LIST_DIR / "consolidated"
TOP_K_CANDIDATES = 10

# Only consider unknowns in me.json whose key starts with this prefix.
UNKNOWN_PREFIX = "person_"

# Unknown→known candidate edge thresholds
# Allow smaller but very clean clusters while filtering noisy overlaps.
MIN_OVERLAP = 4
MIN_WEIGHTED_JACCARD = 0.40
# Identity-like matches should still explain a large share of the unknown's images.
MIN_UNKNOWN_PRECISION = 0.60  # overlap / |unknown|
AMBIGUOUS_DELTA = 0.03  # if top1 - top2 < delta => ambiguous

# --------------------------------------


def _normalize_name(name: str) -> str:
    # Conservative normalization: preserve most names, just trim/collapse whitespace.
    return " ".join((name or "").strip().split())


def _load_person_to_files(path: Path) -> dict[str, set[str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise TypeError(f"{path} must be a JSON object mapping person->list[filename]")
    out: dict[str, set[str]] = {}
    for k, v in data.items():
        if not isinstance(k, str):
            raise TypeError(f"{path}: key must be str, got {type(k).__name__}")
        if not isinstance(v, list):
            raise TypeError(f"{path}: value for {k!r} must be list, got {type(v).__name__}")
        out[_normalize_name(k)] = {str(x) for x in v if x}
    return out


def _weighted_jaccard(a: set[str], b: set[str], weights: dict[str, float]) -> tuple[float, int]:
    if not a or not b:
        return 0.0, 0
    inter = a & b
    union = a | b
    inter_w = sum(weights.get(x, 0.0) for x in inter)
    union_w = sum(weights.get(x, 0.0) for x in union)
    return (inter_w / union_w if union_w > 0 else 0.0), len(inter)


def _overlap_metrics(a: set[str], b: set[str]) -> tuple[int, float, float]:
    """Return (overlap, precision_a, recall_b) where precision_a = |A∩B|/|A|."""
    if not a or not b:
        return 0, 0.0, 0.0
    inter = len(a & b)
    return inter, inter / len(a), inter / len(b)


def main() -> None:
    me = _load_person_to_files(ME_PATH)
    jmail = _load_person_to_files(JMAIL_PATH)
    tommy = _load_person_to_files(TOMMY_PATH)

    print(f"Me: {len(me)} people")
    print(f"Jmail: {len(jmail)} people")
    print(f"Tommy: {len(tommy)} people")

    # Note: we intentionally do not incorporate faces.db moderation/age filtering here.

    # ---------------- A) Missing image suggestions for known names ----------------

    all_known_names = sorted({k for k in set(me) | set(jmail) | set(tommy) if not k.startswith(UNKNOWN_PREFIX)})
    missing_images: dict[str, dict[str, list[str]]] = {}
    for name in all_known_names:
        mine = me.get(name, set())
        j = jmail.get(name, set())
        t = tommy.get(name, set())
        other = j | t
        missing = sorted(other - mine)
        if not missing:
            continue
        missing_images[name] = {
            "missing_from_me": missing,
            "in_jmail_not_me": sorted(j - mine),
            "in_tommy_not_me": sorted(t - mine),
            "in_both_not_me": sorted((j & t) - mine),
        }

    missing_images_sorted = sorted(
        (
            {"name": name, **payload}
            for name, payload in missing_images.items()
        ),
        key=lambda r: (
            len(r.get("in_both_not_me") or []),
            len(r.get("missing_from_me") or []),
            r.get("name") or "",
        ),
        reverse=True,
    )

    # ---------------- B) Unknown name suggestions (unknown in me -> known names) ----------------

    unknowns = sorted([k for k in me.keys() if k.startswith(UNKNOWN_PREFIX)])

    # For known names, use only external sources (jmail/tommy) as the reference set.
    # Using `me.json` for known sets tends to reward co-occurrence in your clustered data.
    known_union: dict[str, set[str]] = {}
    for name in all_known_names:
        known_union[name] = (jmail.get(name, set()) | tommy.get(name, set()))

    # Build document frequency over known sets (IDF-ish weighting)
    df = Counter()
    for files in known_union.values():
        for f in files:
            df[f] += 1
    weights = {f: 1.0 / math.log(2.0 + c) for f, c in df.items()}

    # Score all unknown→known pairs, keep top-k candidates
    unknown_suggestions: dict[str, Any] = {}
    candidate_edges: list[tuple[str, str, float]] = []
    pair_score: dict[tuple[str, str], tuple[float, int]] = {}
    for unk in unknowns:
        a = me.get(unk, set())
        scored = []
        for name in all_known_names:
            b = known_union[name]
            score, overlap = _weighted_jaccard(a, b, weights)
            overlap2, precision_unk, recall_known = _overlap_metrics(a, b)
            if overlap2 != overlap:
                overlap = overlap2
            pair_score[(unk, name)] = (float(score), int(overlap))
            # Also record which external sources support this match.
            overlap_j = len(a & (jmail.get(name, set())))
            overlap_t = len(a & (tommy.get(name, set())))
            scored.append((score, overlap, precision_unk, recall_known, overlap_j, overlap_t, name))
            if (
                overlap >= MIN_OVERLAP
                and score >= MIN_WEIGHTED_JACCARD
                and precision_unk >= MIN_UNKNOWN_PRECISION
            ):
                candidate_edges.append((unk, name, score))
        scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
        top = scored[:TOP_K_CANDIDATES]
        top_candidates = [
            {
                "name": n,
                "score": float(s),
                "overlap": int(o),
                "precision_unknown": float(p),
                "recall_known": float(r),
                "overlap_jmail": int(oj),
                "overlap_tommy": int(ot),
                "source": ("both" if (oj > 0 and ot > 0) else ("jmail" if oj > 0 else ("tommy" if ot > 0 else "none"))),
            }
            for (s, o, p, r, oj, ot, n) in top
            if s > 0
        ]
        status = "weak"
        if (
            len(top) >= 1
            and top[0][1] >= MIN_OVERLAP
            and top[0][0] >= MIN_WEIGHTED_JACCARD
            and top[0][2] >= MIN_UNKNOWN_PRECISION
        ):
            status = "strong"
            if len(top) >= 2 and (top[0][0] - top[1][0]) < AMBIGUOUS_DELTA:
                status = "ambiguous"
        unknown_suggestions[unk] = {
            "status": status,
            "top_candidates": top_candidates,
        }

    status_rank = {"strong": 2, "ambiguous": 1, "weak": 0}
    unknown_suggestions_sorted = sorted(
        (
            {
                "unknown": unk,
                "status": payload["status"],
                "top_candidates": payload["top_candidates"],
            }
            for unk, payload in unknown_suggestions.items()
        ),
        key=lambda r: (
            status_rank.get(r.get("status") or "weak", 0),
            (r.get("top_candidates") or [{}])[0].get("score", 0.0),
            (r.get("top_candidates") or [{}])[0].get("overlap", 0),
            r.get("unknown") or "",
        ),
        reverse=True,
    )

    # Greedy 1:1 matching among strong edges (no external deps).
    # Sort by score desc, then overlap desc; take the first available unknown and name.
    sorted_edges = sorted(
        candidate_edges,
        key=lambda e: (
            float(e[2]),
            pair_score.get((e[0], e[1]), (0.0, 0))[1],
        ),
        reverse=True,
    )
    unknown_to_name: dict[str, str] = {}
    used_names: set[str] = set()
    for unk, name, score in sorted_edges:
        if unk in unknown_to_name:
            continue
        if name in used_names:
            continue
        unknown_to_name[unk] = name
        used_names.add(name)

    matching_rows = []
    for unk, name in unknown_to_name.items():
        score, overlap = pair_score.get((unk, name), (0.0, 0))
        matching_rows.append(
            {"unknown": unk, "name": name, "score": float(score), "overlap": int(overlap)}
        )
    matching_rows.sort(key=lambda r: (r["score"], r["overlap"], r["unknown"]), reverse=True)

    # ---------------- Write outputs ----------------

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "missing_images_suggestions.json").write_text(
        json.dumps(missing_images_sorted, indent=2),
        encoding="utf-8",
    )
    (OUTPUT_DIR / "unknown_name_suggestions.json").write_text(
        json.dumps(unknown_suggestions_sorted, indent=2),
        encoding="utf-8",
    )
    (OUTPUT_DIR / "unknown_name_matching.json").write_text(
        json.dumps(matching_rows, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote {OUTPUT_DIR / 'missing_images_suggestions.json'} ({len(missing_images)} names)")  # noqa: T201
    print(f"Wrote {OUTPUT_DIR / 'unknown_name_suggestions.json'} ({len(unknown_suggestions)} unknowns)")  # noqa: T201
    print(f"Wrote {OUTPUT_DIR / 'unknown_name_matching.json'} ({len(unknown_to_name)} matched)")  # noqa: T201


if __name__ == "__main__":
    main()