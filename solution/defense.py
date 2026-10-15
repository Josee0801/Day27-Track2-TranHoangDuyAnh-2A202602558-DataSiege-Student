"""Detect pipeline faults through the documented metered toolkit only.

Published limits protect cold starts. Robust history adds sensitivity to
persistent, smaller deviations without letting a few faults move the baseline.
"""
from api import Verdict


def median(values):
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


_MIN_HISTORY = 40
_HISTORY_LIMIT = 200


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _verdict(pillar, reasons):
    return Verdict(alert=bool(reasons), pillar=pillar,
                   reason="; ".join(reasons) or "within monitored limits")


def _unavailable(pillar, data):
    return Verdict(alert=False, pillar=pillar,
                   reason="check unavailable: " + str(data["error"]), confidence=0.0)


def _history(ctx, stream, metrics):
    """Score against preceding observations, then update bounded history.

    metrics maps each metric to (value, two_sided, scale_floor). The floor
    prevents rounded or nearly constant measurements producing huge scores.
    Median/MAD tolerate isolated faults; requiring forty samples limits
    cold-start noise. Histories are separated by pipeline entity.
    """
    histories = ctx.state.setdefault("history", {})
    history = histories.setdefault(stream, {})
    scores = []
    for name, (value, two_sided, floor) in metrics.items():
        values = history.setdefault(name, [])
        if len(values) >= _MIN_HISTORY:
            center = median(values)
            scale = max(1.4826 * median([abs(x - center) for x in values]), floor)
            delta = abs(value - center) if two_sided else value - center
            scores.append((name, delta / scale))
        values.append(value)
        if len(values) > _HISTORY_LIMIT:
            del values[0]
    reasons = [name + " robust outlier" for name, score in scores if score > 3.5]
    if not reasons and sum(score > 2.5 for _, score in scores) >= 2:
        reasons.append("multiple metrics shifted together")
    return reasons


def check_data_batch(payload, ctx):
    data = ctx.tools.batch_profile(payload["batch_id"])
    if "error" in data:
        return _unavailable("checks", data)
    b = ctx.baseline
    reasons = []
    if not b["row_count_min"] <= data["row_count"] <= b["row_count_max"]:
        reasons.append("volume outside baseline")
    if data["null_rate"]["customer_id"] > b["null_rate_max"]:
        reasons.append("customer IDs missing")
    if not b["mean_amount_min"] <= data["mean_amount"] <= b["mean_amount_max"]:
        reasons.append("amount distribution shifted")
    if data["staleness_min"] > b["staleness_min_max"]:
        reasons.append("stale batch")
    reasons.extend(_history(ctx, ("checks", payload.get("table", "orders")), {
        "volume": (data["row_count"], True, (b["row_count_max"] - b["row_count_min"]) / 12),
        "null rate": (data["null_rate"]["customer_id"], False, b["null_rate_max"] / 12),
        "amount": (data["mean_amount"], True, (b["mean_amount_max"] - b["mean_amount_min"]) / 12),
        "staleness": (data["staleness_min"], False, b["staleness_min_max"] / 12),
    }))
    return _verdict("checks", reasons)


def check_contract_checkpoint(payload, ctx):
    data = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if "error" in data:
        return _unavailable("contracts", data)
    reasons = list(data["violations"])
    limit = ctx.baseline["freshness_delay_max_min"]
    if data["freshness_delay_min"] > limit:
        reasons.append("contract freshness SLA exceeded")
    reasons.extend(_history(ctx, ("contracts", payload["contract_id"]), {
        "freshness delay": (data["freshness_delay_min"], False, limit / 12),
    }))
    return _verdict("contracts", reasons)


def check_lineage_run(payload, ctx):
    data = ctx.tools.lineage_graph_slice(payload["run_id"])
    if "error" in data:
        return _unavailable("lineage", data)
    reasons = []
    limit = ctx.baseline["lineage_duration_ms_max"]
    if data["duration_ms"] > limit:
        reasons.append("abnormal runtime")
    if "expected_upstream" in payload:
        if set(data["actual_upstream"]) != set(payload["expected_upstream"]):
            reasons.append("upstream lineage mismatch")
    elif "inputs" in payload:
        required = {item["name"] for item in payload["inputs"]}
        if not required.issubset(set(data["actual_upstream"])):
            reasons.append("declared upstream input missing")
    # Inputs can describe only part of the graph. Learn stable additional
    # dependencies per job, allowing occasional faulty observations.
    graphs = ctx.state.setdefault("lineage_graphs", {})
    graph = graphs.setdefault(payload.get("job", "default"), {"runs": 0, "edges": {}})
    actual = set(data["actual_upstream"])
    if graph["runs"] >= 3:
        stable = {edge for edge, count in graph["edges"].items()
                  if count / graph["runs"] >= 0.8}
        if not stable.issubset(actual):
            reasons.append("stable upstream dependency missing")
    graph["runs"] += 1
    for edge in actual:
        graph["edges"][edge] = graph["edges"].get(edge, 0) + 1
    if data["actual_downstream_count"] == 0:
        reasons.append("orphan output")
    elif "expected_downstream_count" in payload:
        if data["actual_downstream_count"] != payload["expected_downstream_count"]:
            reasons.append("downstream lineage mismatch")
    reasons.extend(_history(ctx, ("lineage", payload.get("job", "default")), {
        "runtime": (data["duration_ms"], False, limit / 12),
    }))
    return _verdict("lineage", reasons)


def check_feature_materialization(payload, ctx):
    data = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if "error" in data:
        return _unavailable("ai_infra", data)
    reasons = []
    limit = ctx.baseline["feature_mean_shift_sigma_max"]
    if data["mean_shift_sigma"] > limit:
        reasons.append("training-serving skew")
    reasons.extend(_history(ctx, ("feature", payload["feature_view"]), {
        "feature skew": (data["mean_shift_sigma"], False, limit / 12),
    }))
    return _verdict("ai_infra", reasons)


def check_embedding_batch(payload, ctx):
    data = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if "error" in data:
        return _unavailable("ai_infra", data)
    reasons = []
    b = ctx.baseline
    if data["centroid_shift"] > b["embedding_centroid_shift_max"]:
        reasons.append("embedding drift")
    if data["avg_doc_age_days"] > b["corpus_avg_doc_age_days_max"]:
        reasons.append("stale retrieval corpus")
    reasons.extend(_history(ctx, ("embedding", payload["corpus"]), {
        "centroid shift": (data["centroid_shift"], False, b["embedding_centroid_shift_max"] / 12),
        "corpus age": (data["avg_doc_age_days"], False, b["corpus_avg_doc_age_days_max"] / 12),
    }))
    return _verdict("ai_infra", reasons)
