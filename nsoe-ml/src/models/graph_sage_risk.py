"""
GraphSAGE Node Classifier for Risk Scoring — 2.5.2 / 2.5.3
Output: risk_score ∈ [0, 1] per Person entity node.
Training data: labeled dataset of known threat actors (label=1) vs civilians (label=0).

Architecture:
  Input features (8-dim):
    - entity_type_encoded  (one-hot: Person=0, ThreatActor=1, Org=2)
    - source_credibility   (float)
    - connection_count     (int, log-normalized)
    - historical_incident_count (int, log-normalized)
    - geographic_centrality (float, betweenness centrality)
    - days_since_last_seen  (float, log-normalized)
    - watchlist_flag        (0/1)
    - cross_domain_appearance_count (CCTV + CYBER + SOCMINT)

  Model: 3-layer GraphSAGE → binary classification
"""

from __future__ import annotations

import math
import os
from typing import Optional, Tuple

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv
from torch_geometric.data import Data


INPUT_DIM    = 8
HIDDEN_DIM   = 64
OUTPUT_DIM   = 1   # binary risk score via sigmoid
DROPOUT_P    = 0.3
NUM_LAYERS   = 3


class GraphSAGERiskModel(nn.Module):
    """
    3-layer GraphSAGE risk scoring model.
    Forward pass returns risk_score ∈ [0, 1] for each node.
    """

    def __init__(
        self,
        input_dim: int = INPUT_DIM,
        hidden_dim: int = HIDDEN_DIM,
        dropout: float = DROPOUT_P,
    ) -> None:
        super().__init__()
        self.conv1 = SAGEConv(input_dim, hidden_dim)
        self.conv2 = SAGEConv(hidden_dim, hidden_dim)
        self.conv3 = SAGEConv(hidden_dim, hidden_dim // 2)
        self.fc    = nn.Linear(hidden_dim // 2, OUTPUT_DIM)
        self.dropout = nn.Dropout(dropout)
        self.bn1 = nn.BatchNorm1d(hidden_dim)
        self.bn2 = nn.BatchNorm1d(hidden_dim)

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        x = F.relu(self.bn1(self.conv1(x, edge_index)))
        x = self.dropout(x)
        x = F.relu(self.bn2(self.conv2(x, edge_index)))
        x = self.dropout(x)
        x = F.relu(self.conv3(x, edge_index))
        x = self.fc(x)
        return torch.sigmoid(x).squeeze(-1)   # shape: [N]

    def predict(self, data: Data) -> torch.Tensor:
        self.eval()
        with torch.no_grad():
            return self.forward(data.x, data.edge_index)


class GraphSAGETrainer:
    """Training wrapper with early stopping and MLflow logging."""

    def __init__(
        self,
        model: GraphSAGERiskModel,
        lr: float = 1e-3,
        weight_decay: float = 1e-4,
        pos_weight: float = 10.0,   # class imbalance: few threats vs many civilians
    ) -> None:
        self.model = model
        self.optimizer = torch.optim.Adam(
            model.parameters(), lr=lr, weight_decay=weight_decay
        )
        # BCEWithLogitsLoss for numerical stability (pass logits, not sigmoid output)
        self.criterion = nn.BCELoss(
            weight=None,
            reduction="mean",
        )
        # Separate loss for positive class weighting
        self.pos_weight = torch.tensor([pos_weight])

    def train_epoch(self, data: Data) -> float:
        self.model.train()
        self.optimizer.zero_grad()

        pred = self.model(data.x, data.edge_index)
        # Weighted BCE for class imbalance
        loss = F.binary_cross_entropy(
            pred[data.train_mask],
            data.y[data.train_mask].float(),
            weight=torch.where(
                data.y[data.train_mask].bool(),
                self.pos_weight,
                torch.ones(data.train_mask.sum()),
            ),
        )
        loss.backward()
        self.optimizer.step()
        return float(loss)

    @torch.no_grad()
    def evaluate(self, data: Data, mask_attr: str = "val_mask") -> Tuple[float, float]:
        """Returns (auc, accuracy) on the given mask split."""
        from sklearn.metrics import roc_auc_score, accuracy_score
        self.model.eval()
        mask = getattr(data, mask_attr)
        pred = self.model(data.x, data.edge_index)[mask].numpy()
        labels = data.y[mask].numpy()
        auc = roc_auc_score(labels, pred) if labels.sum() > 0 else 0.0
        acc = accuracy_score(labels, (pred >= 0.5).astype(int))
        return auc, acc

    def fit(
        self,
        data: Data,
        epochs: int = 200,
        patience: int = 20,
        mlflow_run_id: Optional[str] = None,
    ) -> dict:
        """Full training loop with early stopping."""
        import mlflow

        best_val_auc = 0.0
        no_improve = 0
        history = {"train_loss": [], "val_auc": []}

        run = mlflow.active_run()

        for epoch in range(1, epochs + 1):
            loss = self.train_epoch(data)
            val_auc, val_acc = self.evaluate(data, "val_mask")
            history["train_loss"].append(loss)
            history["val_auc"].append(val_auc)

            if run:
                mlflow.log_metrics({
                    "train_loss": loss,
                    "val_auc": val_auc,
                    "val_acc": val_acc,
                }, step=epoch)

            if val_auc > best_val_auc:
                best_val_auc = val_auc
                no_improve = 0
                torch.save(self.model.state_dict(), "/tmp/nsoe_graphsage_best.pt")
            else:
                no_improve += 1

            if no_improve >= patience:
                break

        # Restore best weights
        self.model.load_state_dict(torch.load("/tmp/nsoe_graphsage_best.pt"))
        return {"best_val_auc": best_val_auc, "epochs": epoch}
