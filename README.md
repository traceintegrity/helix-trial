# Helix Trial: Community Edition

**Find out in minutes whether your AI traces are audit-ready.**

Helix ingests your existing trace exports, builds a cryptographic archive bundle, replays it to verify integrity, and returns a plain verdict on whether your workload fits — all locally, no account required.

Built for teams preparing for **EU AI Act Article 12** logging requirements and anyone who needs verifiable, tamper-evident trace archives.

### What the trial answers

1. Can Helix ingest your trace format cleanly?
2. Does the archive replay bit-for-bit?
3. Does your workload compress better than `source + gzip`?

> Enterprise audit, compliance, and drift modules are not included in this package.

---

## Quick start

```bash
pip install -e .
helix-trial ./my-traces.jsonl
```

**Windows:**
```bash
py -3.12 run_helix_trial.py .\my-traces.jsonl
```

Output lands in `./helix_trial_output/` by default.

---

## Try the included samples

```bash
helix-trial ./examples/langfuse_test.jsonl
```

Alternate path (same data):
```bash
helix-trial ./data/partner_test_corpus/langfuse_test.jsonl
```

---

## Supported input formats

| Format | File type |
|--------|-----------|
| Langfuse | `.jsonl` |
| OTLP / OpenTelemetry | `.json` |
| OpenInference | `.jsonl` |
| Flat span exports | `.jsonl` |

Pass a single file, a directory of files, or a `.zip` archive.

---

## Output

| File | What it is |
|------|------------|
| `trial_box/latest_summary.html` | Visual report — open this first |
| `trial_box/latest_summary.json` | Machine-readable summary |
| `trial_box/latest_metrics.json` | Detailed workload metrics |
| `trial_box/latest_trace_pack_manifest.json` | Archive bundle manifest |

All output is written to `./helix_trial_output/` (override with `--output-dir`).

---

## Verdicts

| Verdict | Meaning |
|---------|---------|
| `pilot_now` | Traces ingested cleanly, archive replayed correctly, compression is favourable. Ready for a real pilot. |
| `narrow_pilot` | Helix works with your data, but format gaps or workload characteristics are worth reviewing before a full rollout. |
| `not_fit_yet` | Something in your trace structure or volume profile does not match what Helix expects. The report explains why. |

---

## Docker

```bash
docker build -t helix-trial-community .
docker run --rm -v "$(pwd)/traces:/data" helix-trial-community /data/my-traces.jsonl
```

Docker writes machine-readable outputs next to your mounted export:

- `my-traces.epl_trial_summary.json`
- `my-traces.epl_trial_summary.md`
- `my-traces.epl_trial_metrics.json`
- `my-traces.epl_trial_manifest.json`

Or use the convenience script:
```bash
./docker-trial.sh ./traces/my-traces.jsonl
```

---

## What is not included

This trial does not include enterprise audit verdicts, compliance metadata, drift detection, or internal dashboards. Those are part of [Helix](https://traceintegrity.org/helix).

---

## License

Apache-2.0 — see [LICENSE](LICENSE).

---

## Learn more

- Product: [traceintegrity.org/helix](https://traceintegrity.org/helix)
- Community: [traceintegrity.org](https://traceintegrity.org)
- Enterprise enquiries: [office@traceintegrity.org](mailto:office@traceintegrity.org)
