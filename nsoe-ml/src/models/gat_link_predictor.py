"""
GAT Link Predictor for Hidden Connection Discovery — 2.5.4
Output: P(relationship exists between entity pairs) ∈ [0, 1]
Used to surface hidden connections not explicitly in the knowledge graph.

Architecture:
  2-layer GAT encoder → dot-product decoder
  Multi-head attention (8 heads) on layer 1, 1 head on layer 2.
"""

from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GATConv
from torch_geometric.data import Data
from torch_geometric.utils import negative_sampling


INPUT_DIM  = 8
HIDDEN_DIM = 64
HEADS_L1   = 8
HEADS_L2   = 1
DROPOUT    = 0.3


class GATEncoder(nn.Module):
    def __init__(self, input_dim: int = INPUT_DIM, hidden_dim: int = HIDDEN_DIM) -> None:
        super().__init__()
        self.conv1 = GATConv(input_dim, hidden_dim // HEADS_L1, heads=HEADS_L1, dropout=DROPOUT)
        self.conv2 = GATConv(hidden_dim, hidden_dim, heads=HEADS_L2, concat=False, dropout=DROPOUT)

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        x = F.elu(self.conv1(x, edge_index))
        x = self.conv2(x, edge_index)
        return x


class GATLinkPredictor(nn.Module):
    """
    Encodes node features with GAT, then predicts edge existence
    via dot-product between source and target node embeddings.
    """

    def __init__(self, input_dim: int = INPUT_DIM, hidden_dim: int = HIDDEN_DIM) -> None:
        super().__init__()
        self.encoder = GATEncoder(input_dim, hidden_dim)

    def encode(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        return self.encoder(x, edge_index)

    def decode(self, z: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        """Dot-product decoder. edge_index shape: [2, E]."""
        return (z[edge_index[0]] * z[edge_index[1]]).sum(dim=-1)

    def forward(
        self,
        x: torch.Tensor,
        edge_index: torch.Tensor,
        query_edges: torch.Tensor,
    ) -> torch.Tensor:
        """Returns edge probability ∈ [0,1] for query_edges."""
        z = self.encode(x, edge_index)
        raw = self.decode(z, query_edges)
        return torch.sigmoid(raw)

    def predict_top_k(
        self,
        data: Data,
        k: int = 50,
        threshold: float = 0.7,
    ) -> list[tuple[int, int, float]]:
        """
        Predict top-k hidden connections not already in the graph.
        Returns list of (node_i, node_j, probability) sorted by probability desc.
        """
        self.eval()
        n = data.x.size(0)
        neg_edges = negative_sampling(
            data.edge_index, num_nodes=n, num_neg_samples=min(n * 10, 100_000),
        )
        with torch.no_grad():
            z = self.encode(data.x, data.edge_index)
            probs = torch.sigmoid(self.decode(z, neg_edges))
        top_idx = probs.argsort(descending=True)[:k]
        results = []
        for idx in top_idx:
            p = float(probs[idx])
            if p < threshold:
                break
            i = int(neg_edges[0, idx])
            j = int(neg_edges[1, idx])
            results.append((i, j, p))
        return results


class GATLinkTrainer:
    def __init__(self, model: GATLinkPredictor, lr: float = 1e-3) -> None:
        self.model = model
        self.optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    def train_epoch(self, data: Data) -> float:
        self.model.train()
        self.optimizer.zero_grad()
        n = data.x.size(0)
        neg_edge = negative_sampling(
            data.edge_index, num_nodes=n,
            num_neg_samples=data.edge_index.size(1),
        )
        pos_pred = self.model(data.x, data.train_edge_index, data.train_edge_index)
        neg_pred = self.model(data.x, data.train_edge_index, neg_edge)
        pos_loss = F.binary_cross_entropy(pos_pred, torch.ones_like(pos_pred))
        neg_loss = F.binary_cross_entropy(neg_pred, torch.zeros_like(neg_pred))
        loss = pos_loss + neg_loss
        loss.backward()
        self.optimizer.step()
        return float(loss)

    @torch.no_grad()
    def evaluate(self, data: Data) -> float:
        """Returns ROC AUC on validation edge set."""
        from sklearn.metrics import roc_auc_score
        self.model.eval()
        n = data.x.size(0)
        neg_val = negative_sampling(
            data.edge_index, num_nodes=n,
            num_neg_samples=data.val_edge_index.size(1),
        )
        pos_pred = self.model(data.x, data.train_edge_index, data.val_edge_index).numpy()
        neg_pred = self.model(data.x, data.train_edge_index, neg_val).numpy()
        import numpy as np
        preds  = np.concatenate([pos_pred, neg_pred])
        labels = np.concatenate([np.ones(len(pos_pred)), np.zeros(len(neg_pred))])
        return roc_auc_score(labels, preds) if labels.sum() > 0 else 0.0
