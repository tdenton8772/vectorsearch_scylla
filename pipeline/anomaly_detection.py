#!/usr/bin/env python3
"""
Shared anomaly detection logic for IoT monitoring.

Provides 3-path anomaly detection:
- Path 1: Statistical outliers (rules-based)
- Path 2: Profile fingerprint similarity
- Path 3: Vector search (ANN queries) - optional, expensive
"""

import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


# Detection thresholds
OUTLIER_SIGMA_THRESHOLD = 6.0  # Z-score for Path 1
OUTLIER_COUNT_THRESHOLD = 4    # Min outliers for Path 1
PROFILE_SIMILARITY_THRESHOLD = 0.90  # Cosine similarity for Path 2
PATH3_MIN_MATCHES = 7          # Min similar snapshots for Path 3
PATH3_SIMILARITY_THRESHOLD = 0.90  # Similarity threshold for Path 3


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    is_anomalous: bool
    anomaly_score: float
    path1_triggered: bool
    path2_triggered: bool
    path3_triggered: bool
    detection_details: Optional[str]
    outliers: Dict[str, float]  # metric -> z-score
    similarity_to_profile: float


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    va = np.array(a)
    vb = np.array(b)
    denom = (np.linalg.norm(va) * np.linalg.norm(vb))
    if denom == 0:
        return 0.0
    return float(np.dot(va, vb) / denom)


def check_metric_outliers(
    metrics: Dict[str, float],
    profile_stats: Dict[str, Dict[str, float]],
    sigma_threshold: float = OUTLIER_SIGMA_THRESHOLD
) -> Dict[str, float]:
    """
    Path 1: Check for statistical outliers.
    
    Returns: Dict of {metric_name: z_score} for outliers
    """
    outliers = {}
    for metric_name, value in metrics.items():
        if metric_name not in profile_stats:
            continue
        
        stats = profile_stats[metric_name]
        mean = stats.get('mean')
        std = stats.get('std') or 0.0
        
        if std <= 0:
            continue
        
        z_score = abs((value - mean) / std)
        if z_score > sigma_threshold:
            outliers[metric_name] = z_score
    
    return outliers


def check_profile_similarity(
    snapshot_embedding: List[float],
    profile_embedding: List[float],
    threshold: float = PROFILE_SIMILARITY_THRESHOLD
) -> Tuple[float, bool]:
    """
    Path 2: Check similarity to device profile fingerprint.
    
    Returns: (similarity_score, is_anomalous)
    """
    similarity = cosine_similarity(snapshot_embedding, profile_embedding)
    is_anomalous = similarity < threshold
    return similarity, is_anomalous


def detect_anomaly_paths_1_2(
    metrics: Dict[str, float],
    snapshot_embedding: List[float],
    profile: Optional[Dict]
) -> AnomalyResult:
    """
    Run Path 1 (Rules) and Path 2 (Fingerprint) anomaly detection.
    
    Args:
        metrics: Current snapshot metrics
        snapshot_embedding: Embedding vector for current snapshot
        profile: Device profile dict with 'metric_stats' and 'embedding'
    
    Returns:
        AnomalyResult with detection details
    """
    # Defaults
    path1_triggered = False
    path2_triggered = False
    outliers = {}
    similarity = 1.0
    anomaly_score = 0.0
    
    if not profile:
        # No profile - can't detect anomalies
        return AnomalyResult(
            is_anomalous=False,
            anomaly_score=0.0,
            path1_triggered=False,
            path2_triggered=False,
            path3_triggered=False,
            detection_details=None,
            outliers={},
            similarity_to_profile=1.0
        )
    
    # Path 1: Statistical outliers
    if 'metric_stats' in profile:
        outliers = check_metric_outliers(metrics, profile['metric_stats'])
        if len(outliers) >= OUTLIER_COUNT_THRESHOLD:
            path1_triggered = True
    
    # Path 2: Profile fingerprint similarity
    if 'embedding' in profile and profile['embedding']:
        similarity, path2_anomalous = check_profile_similarity(
            snapshot_embedding,
            profile['embedding']
        )
        if path2_anomalous:
            path2_triggered = True
    
    # Compute aggregate anomaly score
    # Score based on: (1 - similarity) + normalized outlier magnitude
    anomaly_score = max(0.0, 1.0 - similarity)
    if outliers:
        # Add outlier contribution (normalize by dividing by 40 to keep in 0-1 range)
        outlier_magnitude = sum(min(10.0, z) for z in outliers.values())
        anomaly_score += min(1.0, outlier_magnitude / 40.0)
    
    # Build detection details
    is_anomalous = path1_triggered or path2_triggered
    detection_details = None
    
    if is_anomalous:
        parts = []
        if path1_triggered:
            top_outliers = sorted(outliers.items(), key=lambda x: x[1], reverse=True)[:3]
            labels = ", ".join(f"{m} (Z: {z:.1f})" for m, z in top_outliers)
            parts.append(f"PATH 1 (Rules): {len(outliers)} outliers - {labels}")
        if path2_triggered:
            parts.append(f"PATH 2 (Fingerprint): similarity={similarity:.3f} (< {PROFILE_SIMILARITY_THRESHOLD:.2f})")
        detection_details = " | ".join(parts)
    
    return AnomalyResult(
        is_anomalous=is_anomalous,
        anomaly_score=anomaly_score,
        path1_triggered=path1_triggered,
        path2_triggered=path2_triggered,
        path3_triggered=False,
        detection_details=detection_details,
        outliers=outliers,
        similarity_to_profile=similarity
    )
