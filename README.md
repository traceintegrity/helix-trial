# Helix Trial: Community Edition

**Know in minutes whether your AI traces are audit-ready.**

Helix ingests your existing trace exports, generates a cryptographic archive bundle, replays it for verification, and tells you plainly whether your workload is a fit — all on your own machine, no account required.

This is the public open-source trial. Run it once to answer three questions:

- Can Helix ingest your trace export cleanly?
- Does the archive replay correctly?
- Does your workload beat `source + gzip`?

> Enterprise audit, compliance, and drift modules are not included in this package.

---

## Quick Start

```bash
python -m pip install -e .
helix-trial ./my-traces.jsonl
```

**Windows:**
```bash
py -3.12 run_helix_trial.py .\my-traces.jsonl
```

Output lands in `./helix_trial_output/` by default.

---

## Try the Included Sample

```bash
helix-trial ./examples/langfuse_test.jsonl
```

Compatibility path:
```bash
helix-trial ./data/partner_test_corpus/langfuse_test.jsonl
```

---

## Supported Inputs

- Langfuse JSONL
- OTLP JSON
- OpenInference-style JSONL
- Flat JSONL span exports

---

## What You Get

A full local trial run produces:

| File | Purpose |
|------|---------|
| `helix_trial_output/trial_box/latest_summary.html` | Primary review surface — start here |
| `helix_trial_output/trial_box/latest_summary.json` | Machine-readable summary |
| `helix_trial_output/trial_box/latest_metrics.json` | Workload metrics |
| `helix_trial_output/trial_box/latest_trace_pack_manifest.json` | Archive bundle manifest |

Open `latest_summary.html` first.

---

## Verdicts

Helix returns one of three verdicts after a trial run:

| Verdict | Meaning |
|---------|---------|
| `pilot_now` | Your trace data ingested cleanly, the archive replayed correctly, and compression is favourable. You're ready to run a real pilot. |
| `narrow_pilot` | Helix can work with your data, but there are format gaps or workload characteristics worth discussing before a full rollout. |
| `not_fit_yet` | Something in your trace structure or volume profile doesn't align with what Helix expects. The summary report will tell you why. |

If you land on `pilot_now` or `narrow_pilot`, the logical next step is [Helix](https://traceintegrity.org/helix).

---

## Docker

```bash
docker build -t helix-trial-community .
docker run --rm -v "$(pwd)/traces:/data" helix-trial-community /data/my-traces.jsonl
```

The Docker flow writes machine-readable outputs next to your mounted export:

- `my-traces.epl_trial_summary.json`
- `my-traces.epl_trial_summary.md`
- `my-traces.epl_trial_metrics.json`
- `my-traces.epl_trial_manifest.json`

---

## What's Not Included

This trial does not include:

- Enterprise audit verdicts
- Compliance checks and metadata
- Drift detection modules
- Internal dashboards

These are part of [Helix](https://traceintegrity.org/helix).

---

## License

Apache-2.0

---

## Learn More

- Product: [traceintegrity.org/helix](https://traceintegrity.org/helix)
- Community & updates: [traceintegrity.org](https://traceintegrity.org)
- Enterprise enquiries: [office@traceintegrity.org](mailto:office@traceintegrity.org)

---

`helix-trial` is the public entry point. Start there.
