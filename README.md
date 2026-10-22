# Service Health Checker

Probe HTTP, Redis, and TCP endpoints on a schedule, store latency and status in SQLite, and track incidents after three consecutive failures. A recovered service closes its incident. Repeated failures during one outage do not create duplicate incidents.

```sh
python health.py --config examples/services.json --database health.db --once
python health.py --config examples/services.json --database health.db --interval 10
python -m unittest discover -s tests -v
```

The configuration gives each service a name, type, host and port, with an optional HTTP path and timeout. Redis checks use a PING command and require a PONG response. The database retains probe and incident history for reporting.

