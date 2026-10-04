# Deployment verification: systemd install and reboot test (2026-10-04)

This document records how the pipeline was moved from hand-started processes to systemd supervision, and the evidence
that the whole stack recovers by itself after a reboot.

## 1. Starting point
After the [incident](INCIDENT-2026-10-04.md) the stack was running, but HDFS, YARN, HiveServer2 and the Spark job had
been started by hand. systemd showed `hadoop-hdfs` as failed and the other three as inactive, so none of them would
have returned after a reboot. Kafka, Kafka Connect, the classifier and rsyslog were already systemd-managed.

## 2. Preparation: making the reboot safe
A reboot could not simply be tried. Seven corrupt HDFS blocks from the incident were still present (in a quarantine
directory), so HDFS held 783 blocks with only 776 (99.1 %) minimally replicated. The NameNode needs 99.9 % to leave
safe mode, so it would have stuck in safe mode again at boot, and the Spark and Hive start guards would have waited
300 seconds and then failed.

Steps taken:
1. Listed the quarantine directory: exactly 7 files, all of them the corrupt ones, and no corrupt file elsewhere.
2. Deleted it with `hdfs dfs -rm -r -skipTrash` (skipping trash, because a trash copy would keep the corrupt blocks).
   The data those files held had already been re-read from Kafka after the Spark checkpoint rollback, and Hive counts
   matched Kafka.
3. `hdfs fsck /`: **HEALTHY**, 801 of 801 blocks minimally replicated (100 %), 0 missing, 0 corrupt.

## 3. Installation
Run by an administrator in a normal terminal (sudo needs a password):

```bash
sudo bash /home/hadoop/kafka/systemd/install.sh
```

What `install.sh` does:

| Step | Action |
|---|---|
| 1 | copies the unit files, `/etc/hadoop-stack.env` and the tmpfiles rule; creates `/var/log/hadoop-stack`; `systemctl daemon-reload`; enables every unit |
| 2 | stops any previously installed units, in reverse dependency order |
| 3 | stops hand-started processes cleanly (Spark, classifier, Connect, Kafka, HiveServer2, YARN, HDFS) |
| 4 | starts the units in dependency order, running `reset-failed` before each; a failing start is reported but does not stop the script |
| 5 | prints each unit's state and the real HDFS safe-mode state; exits 1 if any unit failed to start |

The installed `hadoop-hdfs.service` is the fixed version (1450 bytes) with the bounded, non-fatal safe-mode wait.

## 4. Reboot test
`sudo reboot`, then checks about three minutes after boot.

| Check | Result |
|---|---|
| Shutdown | clean: `last -x reboot` shows the previous boot with an end time (02:39 – 03:47), unlike the three unclean boots earlier that night |
| Units (`rsyslog`, `kafka`, `kafka-connect`, `system-log-classifier`, `hadoop-hdfs`, `hadoop-yarn`, `hiveserver2`, `spark-system-logs`) | all 8 `active` three minutes after boot |
| HDFS safe mode | `OFF` |
| `hdfs fsck /` | HEALTHY, 812 of 812 blocks minimally replicated |
| Spark checkpoint | newest commit batch 269, written after boot (03:51) |
| Memory | 2.5 GB available of 7.6 GB (474 MB free) |

### Hive against Kafka after the reboot (`default.system_logs`)

| severity | Hive rows | Kafka end offset | gap |
|---|---|---|---|
| critical | 100 | 100 | 0 |
| debug | 582 | 582 | 0 |
| error | 88 | 88 | 0 |
| normal | 40,245 | 40,258 | 13 |
| warning | 1,635 | 1,640 | 5 |

The gaps are micro-batch lag on live topics: the Kafka offsets were read slightly after the Hive query. In every
severity the number of distinct `raw_key` values equals the number of keyed rows, so there are **no duplicates**, and
no rows were lost across the reboot.

## 5. What this proves, and what it does not
- **Proves:** a clean reboot brings the whole stack back in dependency order without manual steps; HDFS leaves safe
  mode by itself on a healthy filesystem; the Spark job resumes from its checkpoint; end-to-end counts match Kafka.
- **Does not prove:** behaviour after a *hard* reset (power loss). The only hard-reset evidence is the incident itself,
  which the new unit design is meant to survive but which has not been re-run on purpose. Also not proven: behaviour
  with a stuck safe mode in the live units; that path is covered by the stubbed tests (`test_systemd_units.sh`,
  `test_install.sh`), not by a live failure.

## 6. Remaining recommendations
1. Set `dfs.datanode.synconclose=true`, so closed HDFS blocks are fsynced and a hard reset cannot leave zero-byte blocks.
2. Reduce NameNode/DataNode heap; memory margin is about 2.5 GB with the full stack up.
3. Find the cause of the unexplained 02:36 reset (`journalctl -b -1` from the boot before the incident, hypervisor logs).
4. Investigate the small, growing difference between the raw topic and the sum of the five severity topics (8 messages
   at the last check). The classifier skips records it cannot classify; this is the likely cause but was not confirmed.
5. Repeat this test after any change to the units or `install.sh`.
