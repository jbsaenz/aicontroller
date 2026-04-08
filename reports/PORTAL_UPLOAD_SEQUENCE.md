# Class Portal Upload Sequence

## Recommended Upload Package
Use the **light bundle** for class portal upload unless your professor explicitly asks for full datasets.

## Step-by-Step Sequence
1. Generate the ZIP bundles:

```bash
cd /Users/saenz/Applications/aicontroller
./scripts/create_submission_bundle.sh
```

2. Upload this file first (primary package):
- `submission/aicontroller_submission_light_<timestamp>.zip`

3. If the portal allows/requests separate key files, upload in this order:
1. `reports/technical_report_final.md`
2. `reports/architecture_diagram.mmd`
3. `reports/FINAL_DELIVERABLES.md`
4. `reports/submission_checklist.md`

4. If instructor requests full reproducibility artifacts, upload additionally:
- `submission/aicontroller_submission_full_<timestamp>.zip`

## Notes for Grader Convenience
- Main execution command inside bundle:

```bash
python src/pipeline.py --phase phase2-5
```

- Docker execution command:

```bash
docker run --rm -v "$(pwd):/app" aicontroller:latest
```

- Deliverables map:
- `reports/FINAL_DELIVERABLES.md`
