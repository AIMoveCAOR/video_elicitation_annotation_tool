# Integration checks — knowledge silo

Live checks for the "knowledge silo" access control: a project's organisation
must persist, propagate to CraftPilot, and actually gate retrieval.

This codebase has no pytest harness, and these need a live MySQL plus both
running services, so they are plain scripts. They are deliberately **not**
named `test_*.py` — that prefix would make pytest collect them next to real
unit tests and fail without a live environment.

Each creates and deletes its own data. `check_silo_end_to_end.py` also asserts
that partner content stays invisible to a user outside the organisation.

Run as root (they read Moodle's DB credentials):

```bash
cd /opt/video_elicitation_annotation_tool
./.venv/bin/python scripts/integration/check_project_cohort_roundtrip.py
./.venv/bin/python scripts/integration/check_compat_layer_cohort.py
./.venv/bin/python scripts/integration/check_cohort_validation.py
./.venv/bin/python scripts/integration/check_managed_cohorts.py

# this one imports CraftPilot, so use its conda interpreter instead
PYTHONNOUSERSITE=1 \
  /root/miniconda3/envs/moodle_backend/bin/python \
  /opt/video_elicitation_annotation_tool/scripts/integration/check_silo_end_to_end.py
```

Each exits non-zero on failure and prints PASS/FAIL per assertion.

## What each one caught

| Check | The bug it exists for |
|-------|----------------------|
| `check_project_cohort_roundtrip` | `allowed_cohort_id` was never written to or read from the projects table |
| `check_compat_layer_cohort` | `model_dump(exclude_none=True)` silently dropped an explicit `null`, so "Open access" could never be restored once a project was locked |
| `check_cohort_validation` | Any cohort id was accepted; a typo made content retrievable by nobody |
| `check_managed_cohorts` | The organisation picker was empty for every user, forever |
| `check_silo_end_to_end` | Ties it together: explicit cohort, craft safety net, and the filter actually hiding restricted docs |
