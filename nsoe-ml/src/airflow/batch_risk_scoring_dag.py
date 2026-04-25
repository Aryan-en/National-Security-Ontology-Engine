"""
Airflow DAG: nightly-gnn-risk-scoring — 2.5.6
Runs nightly GraphSAGE inference on the full graph,
updates risk_score on all Person nodes,
and logs score distribution to Grafana via Prometheus pushgateway.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "nsoe-ml",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [os.environ.get("NSOE_ALERT_EMAIL", "ops@nsoe.gov.in")],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

NEO4J_URI      = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.environ.get("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")
MODEL_PATH     = os.environ.get("GRAPHSAGE_MODEL_PATH", "/opt/nsoe/models/graphsage_risk.pt")
PUSHGW_URL     = os.environ.get("PROMETHEUS_PUSHGATEWAY", "http://pushgateway:9091")
MLFLOW_URI     = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")


def _extract_features(**kwargs) -> str:
    """Extract node features from Neo4j. Returns serialised path."""
    import tempfile, torch
    from nsoe_ml.features.graph_feature_extractor import GraphFeatureExtractor

    extractor = GraphFeatureExtractor(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    data = extractor.extract(include_labels=False)
    extractor.close()

    path = tempfile.mktemp(suffix=".pt")
    torch.save(data, path)
    kwargs["ti"].xcom_push(key="data_path", value=path)
    return path


def _run_inference(**kwargs) -> str:
    """Load model, run inference, push results to XCom."""
    import torch
    from nsoe_ml.models.graph_sage_risk import GraphSAGERiskModel

    data_path = kwargs["ti"].xcom_pull(key="data_path", task_ids="extract_features")
    data = torch.load(data_path)

    model = GraphSAGERiskModel()
    model.load_state_dict(torch.load(MODEL_PATH, map_location="cpu"))
    scores = model.predict(data)  # shape: [N]

    results = {
        eid: float(score)
        for eid, score in zip(data.entity_ids, scores.tolist())
    }

    import tempfile, json
    out_path = tempfile.mktemp(suffix=".json")
    with open(out_path, "w") as f:
        json.dump(results, f)
    kwargs["ti"].xcom_push(key="scores_path", value=out_path)
    return out_path


def _write_scores_to_neo4j(**kwargs) -> int:
    """Batch-update risk_score on all Person nodes."""
    import json
    from neo4j import GraphDatabase
    from datetime import timezone

    scores_path = kwargs["ti"].xcom_pull(key="scores_path", task_ids="run_inference")
    with open(scores_path) as f:
        scores: dict = json.load(f)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    now_iso = datetime.now(timezone.utc).isoformat()
    batch_size = 500
    items = list(scores.items())

    with driver.session() as session:
        for i in range(0, len(items), batch_size):
            batch = [{"entity_id": k, "risk_score": v}
                     for k, v in items[i:i + batch_size]]
            session.run(
                """
                UNWIND $batch AS item
                MATCH (p:Person {entity_id: item.entity_id})
                SET p.risk_score = item.risk_score,
                    p.risk_score_updated_at = $now
                """,
                batch=batch, now=now_iso,
            )
    driver.close()
    return len(scores)


def _push_metrics(**kwargs) -> None:
    """Log score distribution to Prometheus pushgateway."""
    import json
    from prometheus_client import CollectorRegistry, Histogram, push_to_gateway

    scores_path = kwargs["ti"].xcom_pull(key="scores_path", task_ids="run_inference")
    with open(scores_path) as f:
        scores: dict = json.load(f)

    registry = CollectorRegistry()
    hist = Histogram(
        "nsoe_risk_score_distribution",
        "Distribution of GNN risk scores across all Person nodes",
        buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
        registry=registry,
    )
    for score in scores.values():
        hist.observe(score)
    push_to_gateway(PUSHGW_URL, job="nsoe-gnn-scoring", registry=registry)


with DAG(
    dag_id="nightly_gnn_risk_scoring",
    default_args=default_args,
    description="Nightly GraphSAGE risk score update for all Person nodes",
    schedule_interval="0 2 * * *",   # 02:00 IST nightly
    start_date=days_ago(1),
    catchup=False,
    tags=["nsoe", "ml", "risk-scoring"],
) as dag:

    extract = PythonOperator(
        task_id="extract_features",
        python_callable=_extract_features,
    )
    infer = PythonOperator(
        task_id="run_inference",
        python_callable=_run_inference,
    )
    write = PythonOperator(
        task_id="write_scores_to_neo4j",
        python_callable=_write_scores_to_neo4j,
    )
    metrics = PythonOperator(
        task_id="push_metrics",
        python_callable=_push_metrics,
    )

    extract >> infer >> write >> metrics
