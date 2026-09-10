"""Integration test: a project's knowledge-silo cohort must round-trip.

Covers the two breaks that made silo-ing impossible:
  1. create/update never persisted allowed_cohort_id at all;
  2. an explicit None ("Open access") was dropped by exclude_none, so a
     restricted project could never be reopened.

Creates and deletes its own project row; touches nothing else.
"""
import sys
sys.path.insert(0, '/opt/video_elicitation_annotation_tool/backend')

from moodle_db import moodle_db

FAILURES = []

def check(label, actual, expected):
    ok = actual == expected
    print(f"  {'PASS' if ok else 'FAIL'}  {label}: got {actual!r}, expected {expected!r}")
    if not ok:
        FAILURES.append(label)

project_id = None
try:
    created = moodle_db.create_project_sync({
        'name': 'ZZ silo roundtrip test',
        'description': 'temporary - safe to delete',
        'userid': 2,
        'allowed_cohort_id': 30,
    })
    project_id = created['id']
    print(f"created project id={project_id}")

    check("create persists allowed_cohort_id", created.get('allowed_cohort_id'), 30)

    fetched = moodle_db.get_project_sync(project_id)
    check("re-read after create", fetched.get('allowed_cohort_id'), 30)

    updated = moodle_db.update_project_sync(project_id, {'allowed_cohort_id': None})
    check("update to None (open access)", updated.get('allowed_cohort_id'), None)

    updated = moodle_db.update_project_sync(project_id, {'allowed_cohort_id': 30})
    check("update back to a cohort", updated.get('allowed_cohort_id'), 30)

    updated = moodle_db.update_project_sync(project_id, {'name': 'ZZ renamed'})
    check("unrelated update preserves cohort", updated.get('allowed_cohort_id'), 30)
finally:
    if project_id:
        with moodle_db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"DELETE FROM {moodle_db._table('projects')} WHERE id = %s", (project_id,))
            conn.commit()
        print(f"cleaned up project id={project_id}")

print("\nRESULT:", "ALL PASS" if not FAILURES else f"{len(FAILURES)} FAILED: {FAILURES}")
sys.exit(1 if FAILURES else 0)
