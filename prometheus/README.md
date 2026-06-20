# OpenFactory OPCUA Connector – Prometheus Integration Guide

This guide explains how to integrate the OPCUA connector metrics into Prometheus using the **provided files**, either by adding them to an **existing Prometheus setup** or by using the **provided Docker Compose project**.

---

## 📦 1 – Provided Files

The OPCUA connector provides the following files:

```
├── docker-compose.prometheus.yml      # Docker Compose project for Prometheus
├── opcua_connector_rules.yml          # Recording rules for latency, device counts, etc.
└── prometheus.yml                     # Prometheus configuration for Docker Compose
```

These files are **ready to use** in either scenario.

---

## 🔗 2 – Option 1: Integrate into an Existing Prometheus Setup

If you already have Prometheus running:

* **📝 Step 1:** Copy `opcua_connector_rules.yml` to a folder accessible by Prometheus, e.g., `/etc/prometheus/`.

* **⚙️ Step 2:** Edit your `prometheus.yml` to include 

    * the new scrape targets:

        ```yaml
        scrape_configs:
          - job_name: openfactory
            http_sd_configs:
              - url: http://prometheus-metrics-registry:4000/prometheus/targets
                refresh_interval: 30s
        ```

    * and the rules file:

        ```yaml
        rule_files:
        - "/etc/prometheus/opcua_connector_rules.yml"
        ```

* **🔄 Step 3:** Reload Prometheus:

    ```bash
    # systemd example
    sudo systemctl reload prometheus

    # Or HTTP reload endpoint
    curl -X POST http://<PROMETHEUS_HOST>:9090/-/reload
    ```

* **👀 Step 4:** Verify in Prometheus:

  * **Targets:** Status → Targets
  * **Rules:** Status → Rule groups

* **📊 Step 5:** Example PromQL queries:

    ```promql
    # Average Kafka send latency for OPCUA gateways
    avg(gateway:opcua_kafka_latency_avg{component="opcua_connector", role="gateway"})

    # Total devices across all OPCUA coordinator gateways
    coordinator:total_devices{component="opcua_connector", role="coordinator"}
    ```

---

## 🐳 3 – Option 2: Run with the Provided Docker Compose Project

This option is **fully self-contained** and requires no manual Prometheus configuration.

* **🚀 Step 1:** Start the entire Prometheus stack using Docker Compose:

    ```bash
    docker compose -f prometheus/docker-compose.prometheus.yml up -d
    ```

    The stack automatically uses:

    - `prometheus.yml` as configuration
    - `opcua_connector_rules.yml` for rules

* **👀 Step 2:** Access Prometheus:

  * Default port: `http://localhost:9090`
  * Verify **Targets** and **Rule groups** as above.

* **📊 Step 3:** Example queries are the same as in Option 1.
