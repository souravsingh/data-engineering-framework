    {
  "request_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "entity_id": "cust_0042abc",
  "status": "success",
  "score": {
    "score": 0.7312,
    "band": "high",
    "will_convert": true,
    "confidence": 0.8801,
    "model_version": "xgb-v2.1.0",
    "top_drivers": [
      "recency_days",
      "product_page_views",
      "cart_additions",
      "ltv_score",
      "tenure_months"
    ]
  },
  "provenance": {
    "redshift_features_used": [
      "ltv_score",
      "tenure_months",
      "age_band",
      "num_products_held",
      "churn_risk_score",
      "avg_monthly_transactions",
      "total_spend_90d",
      "email_open_rate_30d"
    ],
    "json_features_used": [
      "session_duration_seconds",
      "pages_viewed",
      "product_page_views",
      "cart_additions",
      "channel",
      "is_returning_visitor",
      "recency_days",
      "target_product",
      "campaign_id",
      "customer_segment"
    ],
    "missing_features": [],
    "feature_snapshot_timestamp": "2024-06-01T08:00:00Z"
  },
  "error_detail": null,
  "scored_at": "2024-06-01T09:15:30Z",
  "latency_ms": 38
}
