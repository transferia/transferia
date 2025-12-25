---
title: "Connect {{ PRS }} to {{ data-transfer-name }}"
description: "Connect {{ PRS }} to {{ data-transfer-name }} and monitor your transfer details in {{ PRS }}"
---

# Connect {{ PRS }} to {{ data-transfer-name }}

By default, transfer pods will expose metrics on `9091` ports, and add a default prometheus annotations:

```yaml
annotations:
  prometheus.io/path: /metrics
  prometheus.io/port: "9091"
  prometheus.io/scrape: "true"
```

This will expose metrics scrapper for k8s scrapper. You can re-use our grafana dashboard template from Grafana template [here](https://github.com/transferia/transferia/blob/main/assets/grafana.tmpl.json).

This template will generate for you something like this:

![demo_grafana_dashboard.png](../_assets/demo_grafana_dashboard.png)

Be caution: in template you must replace **<Your-Prometheus-source-ID>** before deployement to grafana.

## Add a prefix to metrics

To add a prefix to metrics, use the `--metrics-prefix` flag. For example, to add a prefix of `transfer` to all metrics, use the following command:

```
trcli replicate --metrics-prefix transfer ...
```

Note: `--metrics-prefix` flag is only available for `activate`, `replicate` and `upload` commands.
