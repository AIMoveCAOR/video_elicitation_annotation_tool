"""A project may only be locked to a cohort that actually exists.

Without this, a typo'd id is accepted and the project's annotations become
retrievable by nobody at all (build_cohort_filter matches membership, and no
user is a member of a non-existent cohort). That fails safe but silently, and
is very hard to diagnose from the UI.
"""
import sys
sys.path.insert(0, '/opt/video_elicitation_annotation_tool/backend')
from moodle_db import moodle_db

FAILURES = []
def check(label, actual, expected):
    ok = actual == expected
    print(f"  {'PASS' if ok else 'FAIL'}  {label}: got {actual!r}, expected {expected!r}")
    if not ok: FAILURES.append(label)

check("real cohort (30 = Louis Vuitton)", moodle_db.cohort_exists_sync(30), True)
check("real cohort (29 = souffleurs)", moodle_db.cohort_exists_sync(29), True)
check("non-existent cohort", moodle_db.cohort_exists_sync(999999), False)
check("None is allowed (open access)", moodle_db.cohort_exists_sync(None), True)

print("\nRESULT:", "ALL PASS" if not FAILURES else f"{len(FAILURES)} FAILED: {FAILURES}")
sys.exit(1 if FAILURES else 0)
