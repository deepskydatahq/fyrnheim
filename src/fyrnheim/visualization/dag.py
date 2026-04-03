"""Generate a self-contained HTML page showing the pipeline DAG."""

from __future__ import annotations

import html
import json

from fyrnheim.core.activity import ActivityDefinition
from fyrnheim.core.analytics_entity import AnalyticsEntity
from fyrnheim.core.identity import IdentityGraph
from fyrnheim.core.metrics_model import MetricsModel
from fyrnheim.core.source import EventSource, StateSource


def _esc(text: str) -> str:
    """HTML-escape a string."""
    return html.escape(str(text))


def _source_tooltip(s: StateSource | EventSource) -> str:
    kind = "STATE" if isinstance(s, StateSource) else "EVENT"
    lines = [
        f"Type: {kind}",
        f"Project: {s.project}",
        f"Dataset: {s.dataset}",
        f"Table: {s.table}",
    ]
    if isinstance(s, StateSource):
        lines.append(f"ID Field: {s.id_field}")
        lines.append(f"Snapshot Grain: {s.snapshot_grain}")
        lines.append(f"Computed Columns: {len(s.computed_columns)}")
    else:
        lines.append(f"Entity ID Field: {s.entity_id_field}")
        lines.append(f"Timestamp Field: {s.timestamp_field}")
        if s.event_type:
            lines.append(f"Event Type: {s.event_type}")
        lines.append(f"Computed Columns: {len(s.computed_columns)}")
    return "\n".join(lines)


def _activity_tooltip(a: ActivityDefinition) -> str:
    lines = [
        f"Trigger: {a.trigger.trigger_type}",
        f"Source: {a.source}",
        f"Entity ID Field: {a.entity_id_field}",
    ]
    if a.include_fields:
        lines.append(f"Include Fields: {', '.join(a.include_fields)}")
    return "\n".join(lines)


def _identity_tooltip(ig: IdentityGraph) -> str:
    lines = [
        f"Canonical ID: {ig.canonical_id}",
        f"Strategy: {ig.resolution_strategy}",
        "Sources:",
    ]
    for src in ig.sources:
        lines.append(
            f"  - {src.source} (id: {src.id_field}, match: {src.match_key_field})"
        )
    return "\n".join(lines)


def _analytics_entity_tooltip(ae: AnalyticsEntity) -> str:
    lines = [
        f"Identity Graph: {ae.identity_graph or 'none'}",
        f"State Fields ({len(ae.state_fields)}):",
    ]
    for sf in ae.state_fields:
        lines.append(f"  - {sf.name} ({sf.strategy} from {sf.source}.{sf.field})")
    if ae.measures:
        lines.append(f"Measures ({len(ae.measures)}):")
        for m in ae.measures:
            lines.append(f"  - {m.name} ({m.aggregation})")
    if ae.computed_fields:
        lines.append(f"Computed Fields: {len(ae.computed_fields)}")
    if ae.quality_checks:
        lines.append(f"Quality Checks: {len(ae.quality_checks)}")
    return "\n".join(lines)


def _metrics_tooltip(mm: MetricsModel) -> str:
    lines = [
        f"Sources: {', '.join(mm.sources)}",
        f"Grain: {mm.grain}",
    ]
    if mm.dimensions:
        lines.append(f"Dimensions: {', '.join(mm.dimensions)}")
    lines.append(f"Metric Fields ({len(mm.metric_fields)}):")
    for mf in mm.metric_fields:
        lines.append(f"  - {mf.field_name} ({mf.aggregation})")
    return "\n".join(lines)


def _build_edges(
    sources: list[StateSource | EventSource],
    activities: list[ActivityDefinition],
    identity_graphs: list[IdentityGraph],
    analytics_entities: list[AnalyticsEntity],
    metrics_models: list[MetricsModel],
) -> list[dict[str, str]]:
    """Build edge list as [{from_id, to_id}, ...]."""
    source_names = {s.name for s in sources}
    ig_names = {ig.name for ig in identity_graphs}
    edges: list[dict[str, str]] = []

    # Activity -> Source
    for act in activities:
        if act.source in source_names:
            edges.append(
                {"from": f"source-{act.source}", "to": f"activity-{act.name}"}
            )

    # IdentityGraph -> Source (via ig.sources[].source)
    for ig in identity_graphs:
        for ig_src in ig.sources:
            if ig_src.source in source_names:
                edges.append(
                    {"from": f"source-{ig_src.source}", "to": f"identity-{ig.name}"}
                )

    # AnalyticsEntity -> IdentityGraph (when identity_graph is set)
    for ae in analytics_entities:
        if ae.identity_graph and ae.identity_graph in ig_names:
            edges.append(
                {
                    "from": f"identity-{ae.identity_graph}",
                    "to": f"entity-{ae.name}",
                }
            )
        elif ae.identity_graph is None:
            # Connect to sources via state_fields[].source
            connected_sources = {sf.source for sf in ae.state_fields}
            for sf_source in connected_sources:
                if sf_source in source_names:
                    edges.append(
                        {
                            "from": f"source-{sf_source}",
                            "to": f"entity-{ae.name}",
                        }
                    )

    # MetricsModel -> Sources
    for mm in metrics_models:
        for ms in mm.sources:
            if ms in source_names:
                edges.append(
                    {"from": f"source-{ms}", "to": f"metrics-{mm.name}"}
                )

    return edges


def generate_dag_html(
    sources: list[StateSource | EventSource] | None = None,
    activities: list[ActivityDefinition] | None = None,
    identity_graphs: list[IdentityGraph] | None = None,
    analytics_entities: list[AnalyticsEntity] | None = None,
    metrics_models: list[MetricsModel] | None = None,
) -> str:
    """Generate a self-contained HTML page showing the pipeline DAG.

    Args:
        sources: State and event sources.
        activities: Activity definitions.
        identity_graphs: Identity graph definitions.
        analytics_entities: Analytics entity definitions.
        metrics_models: Metrics model definitions.

    Returns:
        A string of self-contained HTML.
    """
    sources = sources or []
    activities = activities or []
    identity_graphs = identity_graphs or []
    analytics_entities = analytics_entities or []
    metrics_models = metrics_models or []

    edges = _build_edges(
        sources, activities, identity_graphs, analytics_entities,
        metrics_models,
    )

    # Build node HTML for each layer
    source_nodes = ""
    for s in sources:
        kind = "STATE" if isinstance(s, StateSource) else "EVENT"
        grain = s.snapshot_grain if isinstance(s, StateSource) else ""
        grain_html = f'<span class="node-detail">{_esc(grain)}</span>' if grain else ""
        tooltip = _source_tooltip(s)
        source_nodes += (
            f'<div class="node source-node" id="source-{_esc(s.name)}" '
            f'title="{_esc(tooltip)}">'
            f'<span class="node-badge badge-{kind.lower()}">{kind}</span>'
            f'<span class="node-name">{_esc(s.name)}</span>'
            f"{grain_html}"
            f"</div>\n"
        )

    activity_nodes = ""
    for a in activities:
        tooltip = _activity_tooltip(a)
        activity_nodes += (
            f'<div class="node activity-node" id="activity-{_esc(a.name)}" '
            f'title="{_esc(tooltip)}">'
            f'<span class="node-name">{_esc(a.name)}</span>'
            f'<span class="node-detail">{_esc(a.trigger.trigger_type)}</span>'
            f'<span class="node-detail">{_esc(a.source)}</span>'
            f"</div>\n"
        )

    identity_nodes = ""
    for ig in identity_graphs:
        tooltip = _identity_tooltip(ig)
        identity_nodes += (
            f'<div class="node identity-node" id="identity-{_esc(ig.name)}" '
            f'title="{_esc(tooltip)}">'
            f'<span class="node-name">{_esc(ig.name)}</span>'
            f'<span class="node-detail">{len(ig.sources)} sources</span>'
            f"</div>\n"
        )

    entity_nodes = ""
    for ae in analytics_entities:
        tooltip = _analytics_entity_tooltip(ae)
        field_count = len(ae.state_fields)
        measure_count = len(ae.measures)
        entity_nodes += (
            f'<div class="node entity-node" id="entity-{_esc(ae.name)}" '
            f'title="{_esc(tooltip)}">'
            f'<span class="node-badge badge-entity">ANALYTICS ENTITY</span>'
            f'<span class="node-name">{_esc(ae.name)}</span>'
            f'<span class="node-detail">{field_count} state fields, {measure_count} measures</span>'
            f"</div>\n"
        )

    bottom_nodes = ""
    for mm in metrics_models:
        tooltip = _metrics_tooltip(mm)
        bottom_nodes += (
            f'<div class="node metrics-node" id="metrics-{_esc(mm.name)}" '
            f'title="{_esc(tooltip)}">'
            f'<span class="node-name">{_esc(mm.name)}</span>'
            f'<span class="node-detail">{len(mm.metric_fields)} metrics</span>'
            f"</div>\n"
        )

    edges_json = json.dumps(edges)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>fyrnheim pipeline</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  background: #0a0a0a;
  color: #e5e5e5;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
    Helvetica, Arial, sans-serif;
  min-height: 100vh;
  padding: 40px 20px;
}}
h1 {{
  text-align: center;
  font-size: 2rem;
  font-weight: 900;
  letter-spacing: 0.05em;
  margin-bottom: 48px;
  color: #fff;
}}
.dag-container {{
  position: relative;
  max-width: 1200px;
  margin: 0 auto;
}}
svg#edges {{
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  pointer-events: none;
  z-index: 0;
}}
svg#edges path {{
  fill: none;
  stroke: #333;
  stroke-width: 1.5;
  opacity: 0.6;
}}
.layer {{
  position: relative;
  z-index: 1;
  margin-bottom: 48px;
}}
.layer-label {{
  font-size: 0.7rem;
  letter-spacing: 0.15em;
  text-transform: uppercase;
  color: #555;
  margin-bottom: 12px;
  padding-left: 4px;
}}
.layer-nodes {{
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  justify-content: center;
}}
.node {{
  background: #141414;
  border: 1.5px solid #333;
  border-radius: 8px;
  padding: 14px 20px;
  min-width: 160px;
  max-width: 280px;
  display: flex;
  flex-direction: column;
  gap: 6px;
  cursor: pointer;
  transition: border-color 0.15s, box-shadow 0.15s, opacity 0.2s;
}}
.node:hover {{
  box-shadow: 0 0 16px rgba(255,255,255,0.05);
}}
.source-node {{ border-color: #3b82f6; }}
.source-node:hover {{ border-color: #60a5fa; box-shadow: 0 0 16px rgba(59,130,246,0.15); }}
.activity-node {{ border-color: #22c55e; }}
.activity-node:hover {{ border-color: #4ade80; box-shadow: 0 0 16px rgba(34,197,94,0.15); }}
.identity-node {{ border-color: #a855f7; }}
.identity-node:hover {{ border-color: #c084fc; box-shadow: 0 0 16px rgba(168,85,247,0.15); }}
.entity-node {{ border-color: #f59e0b; }}
.entity-node:hover {{ border-color: #fbbf24; box-shadow: 0 0 16px rgba(245,158,11,0.15); }}
.metrics-node {{ border-color: #ef4444; }}
.metrics-node:hover {{ border-color: #f87171; box-shadow: 0 0 16px rgba(239,68,68,0.15); }}
.node-badge {{
  font-size: 0.6rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  padding: 2px 8px;
  border-radius: 4px;
  width: fit-content;
}}
.badge-state {{ background: rgba(59,130,246,0.15); color: #60a5fa; }}
.badge-event {{ background: rgba(59,130,246,0.15); color: #93c5fd; }}
.badge-entity {{ background: rgba(245,158,11,0.15); color: #fbbf24; }}
.node-name {{
  font-weight: 600;
  font-size: 0.95rem;
  color: #fff;
}}
.node-detail {{
  font-size: 0.75rem;
  color: #888;
}}
</style>
</head>
<body>
<h1>fyrnheim pipeline</h1>
<div class="dag-container">
  <svg id="edges"></svg>

  <div class="layer" id="layer-sources">
    <div class="layer-label">SOURCES</div>
    <div class="layer-nodes">{source_nodes}</div>
  </div>

  <div class="layer" id="layer-activities">
    <div class="layer-label">ACTIVITIES</div>
    <div class="layer-nodes">{activity_nodes}</div>
  </div>

  <div class="layer" id="layer-identity">
    <div class="layer-label">IDENTITY</div>
    <div class="layer-nodes">{identity_nodes}</div>
  </div>

  <div class="layer" id="layer-entities">
    <div class="layer-label">ANALYTICS ENTITIES</div>
    <div class="layer-nodes">{entity_nodes}</div>
  </div>

  <div class="layer" id="layer-metrics">
    <div class="layer-label">METRICS MODELS</div>
    <div class="layer-nodes">{bottom_nodes}</div>
  </div>
</div>

<script>
(function() {{
  var edges = {edges_json};
  var svg = document.getElementById("edges");
  var container = document.querySelector(".dag-container");

  var selectedNode = null;

  function drawEdges() {{
    var rect = container.getBoundingClientRect();
    svg.setAttribute("width", rect.width);
    svg.setAttribute("height", rect.height);
    svg.setAttribute("viewBox", "0 0 " + rect.width + " " + rect.height);
    svg.innerHTML = "";

    edges.forEach(function(e) {{
      var fromEl = document.getElementById(e.from);
      var toEl = document.getElementById(e.to);
      if (!fromEl || !toEl) return;

      var fromRect = fromEl.getBoundingClientRect();
      var toRect = toEl.getBoundingClientRect();
      var cRect = container.getBoundingClientRect();

      var x1 = fromRect.left + fromRect.width / 2 - cRect.left;
      var y1 = fromRect.bottom - cRect.top;
      var x2 = toRect.left + toRect.width / 2 - cRect.left;
      var y2 = toRect.top - cRect.top;

      var midY = (y1 + y2) / 2;
      var d = "M " + x1 + " " + y1 +
              " C " + x1 + " " + midY + ", " + x2 + " " + midY + ", " + x2 + " " + y2;

      var path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", d);
      path.setAttribute("data-from", e.from);
      path.setAttribute("data-to", e.to);
      svg.appendChild(path);
    }});

    applyHighlight();
  }}

  function getConnected(nodeId) {{
    var connected = new Set();
    connected.add(nodeId);
    // Direct neighbors only (one hop in both directions)
    edges.forEach(function(e) {{
      if (e.from === nodeId) connected.add(e.to);
      if (e.to === nodeId) connected.add(e.from);
    }});
    return connected;
  }}

  function applyHighlight() {{
    var allNodes = document.querySelectorAll(".node");
    var allPaths = svg.querySelectorAll("path");

    if (!selectedNode) {{
      allNodes.forEach(function(n) {{ n.style.opacity = "1"; }});
      allPaths.forEach(function(p) {{ p.style.opacity = "0.15"; p.style.stroke = "#404040"; p.style.strokeWidth = "1.5"; }});
      return;
    }}

    var connected = getConnected(selectedNode);

    allNodes.forEach(function(n) {{
      n.style.opacity = connected.has(n.id) ? "1" : "0.15";
    }});

    allPaths.forEach(function(p) {{
      var from = p.getAttribute("data-from");
      var to = p.getAttribute("data-to");
      if (connected.has(from) && connected.has(to)) {{
        p.style.opacity = "1";
        p.style.stroke = "#f4442e";
        p.style.strokeWidth = "2.5";
      }} else {{
        p.style.opacity = "0.05";
        p.style.stroke = "#404040";
        p.style.strokeWidth = "1.5";
      }}
    }});
  }}

  document.addEventListener("click", function(evt) {{
    var node = evt.target.closest(".node");
    if (node) {{
      if (selectedNode === node.id) {{
        selectedNode = null;
      }} else {{
        selectedNode = node.id;
      }}
    }} else {{
      selectedNode = null;
    }}
    applyHighlight();
  }});

  window.addEventListener("load", drawEdges);
  window.addEventListener("resize", drawEdges);
}})();
</script>
</body>
</html>"""
