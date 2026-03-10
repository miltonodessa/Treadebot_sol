"""
wallet_graph.py — Wallet Coordination Graph
=============================================
Builds a session-persistent graph of wallet relationships.

Nodes: wallet addresses
Edges: two wallets share an edge when they both participated
       (bought or sold) on the SAME token launch within the same session.
Edge weight: number of co-participations across different launches.

Coordination score for a group of wallets:
  high edge density + high weights → strongly coordinated cluster.

Used in two ways:
  1. Entry signal: given the set of early buyers of a new token,
     compute their inter-coordination score.
     If they have repeatedly appeared together → likely insider cluster.

  2. Long-term blacklisting: add known rug/dump wallets to graph,
     use label propagation to flag their likely associates.

Dependencies:
  networkx (graph operations)
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from typing import Optional

log = logging.getLogger("wallet_graph")

try:
    import networkx as nx
    HAS_NX = True
except ImportError:
    HAS_NX = False
    log.warning("networkx not installed — wallet_graph coordination scores disabled")


class WalletGraph:
    """
    Session-persistent wallet coordination graph.

    Usage:
        graph = WalletGraph()
        # After a token launch, register its early buyers:
        graph.register_launch("MINT123", {"wallet_A", "wallet_B", "wallet_C"})
        # For a NEW token with these buyers, get coordination score:
        score = graph.coordination_score({"wallet_A", "wallet_B"})
        # → high score because they co-participated before
    """

    def __init__(self, min_coparticipations: int = 2):
        """
        min_coparticipations: minimum times two wallets must appear together
        before they are considered "coordinated" (not just coincidence).
        """
        self.min_co = min_coparticipations
        self._G = nx.DiGraph() if HAS_NX else None
        # mint → set of wallets (for dedup)
        self._launches: dict[str, set[str]] = {}
        # (wallet_a, wallet_b) → co-participation count
        self._co_count: dict[tuple, int] = defaultdict(int)
        # Wallet first seen timestamp
        self._wallet_first_seen: dict[str, float] = {}

    # ── Public API ────────────────────────────────────────────────────────

    def register_launch(self, mint: str, buyer_wallets: set[str]):
        """
        Called after a token is bought.
        Adds all buyer-pairs to the co-participation counter.
        """
        if not buyer_wallets or mint in self._launches:
            return

        self._launches[mint] = buyer_wallets
        now = time.time()

        wallets = list(buyer_wallets)
        for i, w in enumerate(wallets):
            if w not in self._wallet_first_seen:
                self._wallet_first_seen[w] = now
            if HAS_NX and self._G is not None:
                if not self._G.has_node(w):
                    self._G.add_node(w, first_seen=now, launch_count=0)
                self._G.nodes[w]["launch_count"] = self._G.nodes[w].get("launch_count", 0) + 1

            for j in range(i + 1, len(wallets)):
                w2 = wallets[j]
                key = (min(w, w2), max(w, w2))
                self._co_count[key] += 1
                if HAS_NX and self._G is not None:
                    if self._G.has_edge(w, w2):
                        self._G[w][w2]["weight"] += 1
                    else:
                        self._G.add_edge(w, w2, weight=1)
                    if self._G.has_edge(w2, w):
                        self._G[w2][w]["weight"] += 1
                    else:
                        self._G.add_edge(w2, w, weight=1)

    def coordination_score(self, wallets: set[str]) -> float:
        """
        Returns 0.0–1.0 coordination score for a set of wallets.

        High score: these wallets repeatedly appeared together in past launches.
        Low score:  new/unrelated wallets.

        Algorithm:
          1. For each pair (wi, wj) in wallets, get co-participation count.
          2. Pairs with count >= min_co are "coordinated".
          3. Score = coordinated_pairs / total_possible_pairs.
          4. Bonus: average co-participation strength of coordinated pairs.
        """
        if len(wallets) < 2:
            return 0.0

        wallets = list(wallets)
        total_pairs = len(wallets) * (len(wallets) - 1) // 2
        if total_pairs == 0:
            return 0.0

        coordinated = 0
        strength_sum = 0.0

        for i in range(len(wallets)):
            for j in range(i + 1, len(wallets)):
                key = (min(wallets[i], wallets[j]), max(wallets[i], wallets[j]))
                count = self._co_count.get(key, 0)
                if count >= self.min_co:
                    coordinated += 1
                    strength_sum += count

        if coordinated == 0:
            return 0.0

        pair_ratio  = coordinated / total_pairs
        avg_strength = strength_sum / coordinated
        # Normalize strength: 2 co-participations = baseline, cap at 10
        strength_norm = min(avg_strength / 10.0, 1.0)

        score = 0.7 * pair_ratio + 0.3 * strength_norm
        return round(min(score, 1.0), 3)

    def known_coordination(self, wallet: str) -> float:
        """
        Single-wallet coordination: how connected is this wallet
        in the co-participation graph?
        Returns 0-1 (normalized degree centrality).
        """
        if not HAS_NX or self._G is None:
            return 0.0
        if not self._G.has_node(wallet):
            return 0.0
        deg = self._G.degree(wallet, weight="weight")
        total_wallets = max(len(self._G.nodes) - 1, 1)
        return min(deg / (total_wallets * 3), 1.0)  # 3 = typical avg weight

    def get_clusters(self, min_size: int = 3) -> list[list[str]]:
        """
        Return wallet clusters (connected components with weight >= min_co).
        Useful for blacklisting entire rug clusters.
        """
        if not HAS_NX or self._G is None:
            return []

        # Build undirected graph of coordinated pairs only
        G2 = nx.Graph()
        for (w1, w2), count in self._co_count.items():
            if count >= self.min_co:
                G2.add_edge(w1, w2, weight=count)

        clusters = [
            list(c) for c in nx.connected_components(G2)
            if len(c) >= min_size
        ]
        clusters.sort(key=len, reverse=True)
        return clusters

    def stats(self) -> dict:
        return {
            "wallets":       len(self._wallet_first_seen),
            "launches":      len(self._launches),
            "pairs_tracked": len(self._co_count),
            "coordinated_pairs": sum(
                1 for v in self._co_count.values() if v >= self.min_co
            ),
            "clusters": len(self.get_clusters()),
        }
