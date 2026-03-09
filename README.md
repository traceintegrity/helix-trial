# Helix Trial Community

This package is the **public open source Helix trial**.

Recommended public repo:

- `https://github.com/traceintegrity/helix-trial`

Use it to answer three questions on your own machine:

- can Helix ingest this export cleanly?
- does the archive replay correctly?
- does the workload beat `source + gzip`?

It does **not** include enterprise audit, compliance, or drift modules.

## Quick start

```bash
python -m pip install -e .
helix-trial ./my-traces.jsonl
```

Windows:

```powershell
py -3.12 run_helix_trial.py .\my-traces.jsonl
```

Default output directory:

- `./helix_trial_output/`

## Included sample

```bash
helix-trial ./examples/langfuse_test.jsonl
```

Compatibility sample path:

```bash
helix-trial ./data/partner_test_corpus/langfuse_test.jsonl
```

## What you get

- local format detection through the normal trial path
- archive bundle generation
- replay verification
- workload-fit verdict
- HTML summary report

Supported inputs:

- Langfuse JSONL
- OTLP JSON
- OpenInference-style JSONL
- flat JSONL span exports

## Output files

After a run, the important files are:

- `helix_trial_output/trial_box/latest_summary.html`
- `helix_trial_output/trial_box/latest_summary.json`
- `helix_trial_output/trial_box/latest_metrics.json`
- `helix_trial_output/trial_box/latest_trace_pack_manifest.json`

Primary review surface:

- `latest_summary.html`

## Verdicts

- `pilot_now`
- `narrow_pilot`
- `not_fit_yet`

## Docker path

```bash
docker build -t helix-trial-community .
docker run --rm -v "$(pwd)/traces:/data" helix-trial-community /data/my-traces.jsonl
```

The Docker flow writes machine-readable outputs next to the mounted export:

- `my-traces.epl_trial_summary.json`
- `my-traces.epl_trial_summary.md`
- `my-traces.epl_trial_metrics.json`
- `my-traces.epl_trial_manifest.json`

## Not included

- enterprise audit verdicts
- enterprise compliance checks
- enterprise compliance metadata
- internal dashboards
- benchmark marketing pages

## License

- Apache-2.0

Start with `helix-trial`. That is the public entry point.
