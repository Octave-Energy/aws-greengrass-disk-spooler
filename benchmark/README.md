# Disk spooler benchmark

A black-box benchmark of the disk spooler plugin, run inside a real AWS IoT Greengrass Nucleus the way IBEMS uses it
in the field. It establishes a baseline and compares builds (branches, tags, commits) so that you can see what a
change improves and what it breaks.

```
 IBEMS simulator ──IPC──▶ Nucleus 2.18.3 + disk spooler ──MQTT/mTLS──▶ network proxy ──▶ Mosquitto ──▶ cloud subscriber
 (Greengrass component,   (released distribution,                       (up / black hole /            (decodes every
  protobuf batches,        plugin from the component                     reset: "the internet          message, checks
  publish_to_iot_core)     store, production config)                     breaks")                       sequence numbers)
```

Nothing in the plugin or the Nucleus is mocked or instrumented. Everything is measured from the outside:

| what | how |
|---|---|
| ingest rate, IPC publish latency, rejected publishes | the simulator, through `publish_to_iot_core_async` (QoS 1), as IBEMS publishes |
| delivered, lost, duplicated, out-of-order messages, end-to-end latency | the cloud subscriber; every message carries a sequence number and its creation time |
| disk used | size of `spooler.db` (+ `-journal`/`-wal`), every second |
| bytes written to storage | `/proc/<pid>/io` of the Nucleus (Linux only) |
| RAM used | RSS of the Nucleus; JVM heap (`jstat`), every second; at checkpoints the live heap after a full GC and the number of `SpoolMessage` objects in it (`jcmd GC.class_histogram`) |
| CPU | CPU time of the Nucleus |
| errors | WARN/ERROR events and known spooler events in the Nucleus logs, e.g. *Disk Spooler failed to add Message* (fallback to memory) or *Message spool is full* |

## Messages

The simulator publishes IBEMS `MeasurementMessage` batches (protobuf, schema copied from `octave-ibems`, see
`proto/SOURCE.md`) on `ibems/<thing>/measurements/protobuf`. They come from a pool generated from a fixed seed, so
every run publishes the same bytes. The mix is about the one of IBEMS: 85% small low-latency batches (a few devices,
a few values), 13% larger batches, 2% BMS cell dumps (string → module → cell) of up to 125 KB, the IBEMS cap. Mean
size is about 2 KB.

Like IBEMS, the simulator waits for every publish to complete before it sends the next one (`max_outstanding: 1`).
If the Nucleus cannot accept messages at the target rate, the shortfall is reported (`ingest_shortfall_pct`).

## Scenarios

Every scenario simulates an **internet outage** while IBEMS keeps publishing. A scenario name says what happens:

| scenario | what happens |
|---|---|
| `r<rate>-spill<threshold>-outage` | **internet outage**: the link to AWS IoT Core is down, the device keeps running and the spooler takes over |
| `…-outage-devicecrash` | **internet outage + device crash**: during the outage the whole device goes down (Nucleus, spooler, IBEMS) and boots again while the internet is still down |
| `…-outage-reset` | **internet outage** where the connections are reset (closed at once) instead of black-holed |

`<rate>` is the IBEMS publish rate in messages per second, `<threshold>` the plugin's `diskSpillThresholdBytes`
(`spill0`: everything goes to disk, no RAM; `spill300k`: production, the first 300 KiB of backlog stays in RAM).

Each scenario runs through these phases:

1. **startup**: the Nucleus starts and sets up the persistent spooler (the run fails if it falls back to memory)
2. **connect**, **warmup**: until messages reach the cloud, then a few seconds of steady state
3. **online**: steady state, link up
4. **outage**: the internet (the link between the device and AWS IoT Core) breaks: black-holed (packets vanish, as
   on a dead uplink: the Nucleus only notices through its MQTT keep-alive) or reset (connections closed). The device
   itself keeps running: IBEMS keeps publishing and the spooler takes over, first memory, up to
   `diskSpillThresholdBytes`, then disk
5. **device crash** (optional, `device_crash: true`, scenarios `…-outage-devicecrash`): during the internet outage the
   whole device goes down. The Nucleus, the spooler and IBEMS are killed at once (SIGKILL: no shutdown, nothing is
   flushed), whatever was only in memory is lost, and the connections the device had open die with it. The device
   then boots again (**offline_after_reboot**): the Nucleus starts, the spooler reopens its database and IBEMS
   publishes again, while the internet is still down.
   This is a crash of the processes, not a cut of the power supply: the operating system keeps running, so data
   the spooler wrote but the operating system had not yet flushed to storage survives here, where a real power cut
   could lose it. It shows what the spooler keeps across a crash or reboot, not whether its writes are durable.
6. **recovery**: the internet is back; the simulator keeps publishing; until the backlog of the outage is delivered
7. **cooldown**: the simulator stops; until every accepted message is delivered, or nothing more arrives
   (then the rest is lost); then a graceful stop and the final database size

Profiles in `scenarios/`:

- `quick.yaml` (every pull request): 10, 50 and 100 msg/s × spill threshold 0 (disk only, no RAM) and 300 KiB
  (production), 15 s online, 30 s outage, plus two device-crash scenarios.
- `full.yaml` (on demand): same matrix, 60 s online, 180 s outage (longer than the MQTT keep-alive, so the Nucleus
  drops the connection and reconnects), more device-crash scenarios, a reset.

The rates stop at 100 msg/s: AWS IoT Core accepts at most 100 publish requests per second per MQTT connection
(and 512 KB/s), and the Nucleus throttles its publishes to stay within that. The cloud-side delivery rate is
therefore capped around 100 msg/s, and a backlog drains at about 100 msg/s minus the rate IBEMS keeps publishing
at: at 100 msg/s it does not drain at all.

The Nucleus and spooler settings are those of production (`octave-ibems/deployment/generate_greengrass_deployment.py`):
Nucleus 2.18.3, `mqtt.spooler.storageType: Disk`, `maxSizeInBytes: 62914560`, JSON logs at INFO.

## The report

`report.md`, `report.json` and `charts/*.png`; `summary.md` is the same report with Mermaid line charts instead of the
PNGs, shown as the job summary in GitHub Actions (which renders Mermaid but no images):

- **Key results**: one row per scenario and build. These answer the questions:
  - *Can we ingest at 10 / 50 / 100 msg/s?* `accepted msg/s`, `IPC p99`; and get it to the cloud? `delivered msg/s`,
    `e2e p50`, `capacity msg/s`
  - *How much disk space without RAM?* the `spill0` scenarios: `db growth` during the outage, `disk B / payload B`,
    `db after drain` (SQLite does not give space back)
  - *If RAM is used, how much?* `heap for backlog` (live heap at the end of the outage minus online, both after a full
    GC), `RSS peak`; in detail the number of `SpoolMessage` objects in the heap per checkpoint
  - *Do we lose messages?* `lost`, `dup`, `rejected`
- **Disk needed for longer outages**: per scenario, the spooler.db size needed to keep every message of a 1 h, 6 h,
  12 h, 1 d and 7 d outage at that rate, extrapolated from the measured outage: *disk B / payload B* × (payload rate ×
  duration − `diskSpillThresholdBytes`). Also when the spool is full: the Nucleus caps it at
  `mqtt.spooler.maxSizeInBytes` of payload (60 MiB in production) and then rejects new publishes (*Message spool is
  full*), which at 10 msg/s happens after about 50 minutes, at 100 msg/s after about 5. Outages longer than that need
  a larger `maxSizeInBytes` as well as the disk.
- **Comparison** (two builds): every headline metric with its direction and a noise tolerance; changes within the
  tolerance are shown as ≈.
- **Scenarios**: charts (throughput, backlog, database size, heap, RSS; the builds overlaid, aligned on the start of
  the outage), and per build the phases, memory checkpoints and most frequent log events.

## Running it

### In GitHub Actions

- Every pull request that touches the plugin runs the quick profile: PR head (candidate) against its base (baseline).
  Every commit gets its own complete run: a follow-up commit does not cancel the run of the previous one.
- *Actions → Spooler benchmark → Run workflow* benchmarks any two refs, e.g. baseline `main-octave` and candidate
  `sync-upstream-and-fix-spool-races`, with the quick or full profile. Leave the candidate empty to record a baseline.

Every scenario runs in its own job, both builds on the same runner one after the other (alternating which goes
first), so their comparison does not depend on the runner. Download the `spooler-benchmark-report` artifact for the
charts, `results-<scenario>` for the raw time series and all logs.

To compare against an earlier run, download both `report.json` files:

```bash
python -m spoolbench compare baseline/report.json candidate/report.json
```

Reports from different machines are flagged: compare runs of the same workflow (same runner type) only.

### Locally

Requires Python 3.11+, a JDK 11+ to run the Nucleus (`JAVA_HOME`), Docker (or a local `mosquitto`) and `openssl`.

```bash
cd benchmark
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
(cd .. && mvn -B package -DskipTests -Dmaven.test.skip=true)   # JDK 8, as in CI
.venv/bin/python -m spoolbench run --profile scenarios/quick.yaml \
    --jar main=../target/aws.greengrass.DiskSpooler.jar --out results/local
```

- `--jar LABEL=PATH` can be repeated to compare builds in one run; `--ref LABEL=TEXT` describes a build in the report.
- `--only REGEX` selects scenarios (`python -m spoolbench list --profile …` lists them), `--repeat N` repeats them.
- `python -m spoolbench report results/local` rebuilds the report from the results.
- The Nucleus distribution is downloaded once to `~/.cache/spoolbench`.

## Layout

| file | |
|---|---|
| `spoolbench/runner.py` | the scenario phases and the metrics |
| `spoolbench/nucleus.py` | installs and runs the Nucleus with the plugin and the simulator component |
| `spoolbench/simulator.py` | the IBEMS simulator component |
| `spoolbench/payloads.py` | the protobuf message generator |
| `spoolbench/cloud.py` | the cloud subscriber and loss accounting |
| `spoolbench/netproxy.py` | the network proxy |
| `spoolbench/broker.py`, `certs.py` | Mosquitto and its test PKI |
| `spoolbench/probe.py` | process, JVM and database metrics |
| `spoolbench/report.py`, `compare.py` | report, charts and comparison (metrics, directions, tolerances) |
| `spoolbench/projection.py` | disk needed for longer outages, extrapolated from the measured one |
| `scenarios/*.yaml` | the profiles |
| `proto/` | the IBEMS measurement schema |
