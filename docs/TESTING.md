# Testing

The project was built test-first: each test was written and seen failing before the code or config it covers.

## Suites

| Suite | Type | What it proves |
|---|---|---|
| `tests/test_classifier.py`, `tests/test_classifier_delivery.py` | pytest, no services | severity mapping for every input format; ordering send → acknowledge → commit; key format; failed send raises and does not commit |
| `tests/test_spark_transform.py` | pytest + local Spark | parsed columns and their order, `raw_key` decoding, rsyslog and JSON formats |
| `tests/test_curated.py`, `tests/test_aggregates.py` | pytest + local Spark | noise removal, de-duplication by `raw_key` and by Kafka coordinates, hourly/daily numbers |
| `tests/test_stack.sh` | acceptance, live stack | systemd-managed processes, ports, topics, end-to-end records appearing in Hive, `raw_key` in Hive |
| `tests/test_curated.sh`, `tests/test_aggregates.sh` | acceptance, live stack | the batch jobs and their Hive tables end to end |
| `tests/test_yarn_memory.sh` | acceptance | YARN/Tez/Spark memory settings stay inside the VM budget |
| `tests/test_systemd_units.sh` | static + behavioural | HDFS unit cannot be killed by a stuck safe mode; Spark/Hive guards behave correctly |
| `tests/test_install.sh` | behavioural, stubbed | `install.sh` survives a failing unit, resets failed state, reports safe mode |
| `tests/test_repo.sh` | repository hygiene | files tracked, no data/logs/binaries/backups/caches tracked, none over 512 KB, tree clean |

Last recorded results (2026-10-04): unit tests 76 pass; stack suite 79 pass; curated 8; aggregates 12; YARN memory 10;
`test_systemd_units` 15; `test_install` 23; `test_repo` 33.

## Running

```bash
cd /home/hadoop/kafka
python3 -m pytest tests -q                       # unit tests (no services needed)
bash tests/test_systemd_units.sh                 # static, repo copy of the units
bash tests/test_install.sh                       # stubbed, no root
bash tests/test_repo.sh                          # run as your normal user (it calls sudo -u hadoop itself)
bash tests/test_stack.sh                         # needs the stack installed under systemd
```
The bash suites print `PASS`/`FAIL` per check and `RESULT: n passed, m failed`, and exit non-zero on any failure.

## Do the tests actually bite? (mutation checks)
A test that passes on broken code is worthless, so the infrastructure tests were run against deliberately broken copies:

- `test_systemd_units.sh`: over a dozen broken units (no-op guard, guard ending `exit 0`, wrong string, `-` prefix, wrong
  position, too-short timeout, commented-out or duplicated guard, fatal safe-mode wait, the original unit): all rejected.
- `test_install.sh`: 8 broken `install.sh` variants (warning deleted, `|| true` dropped, every unit marked failed,
  start order changed, `reset-failed` after start, unconditional warning, never exiting non-zero, aborting on failure):
  all rejected.

The Spark guard test even runs the guard's real command against a fake `hdfs` that reports "Safe mode is ON/OFF" or
fails, rather than only grepping the unit text.

## Validation tooling
`scripts/validate_system_logs.sh` consumes each severity topic and reports, per topic, the first/last records and any
record whose severity does not belong there. It writes a report under `validation/` (not tracked).
