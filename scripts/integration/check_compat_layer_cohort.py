"""The API/compat layer must be able to CLEAR a project's cohort.

Selecting "Open access" in the UI sends allowed_cohort_id = null. If the compat
layer strips None values, that choice silently does nothing and a project can
never be un-restricted once locked.
"""
import asyncio, sys
sys.path.insert(0, '/opt/video_elicitation_annotation_tool/backend')

import database_compat as db
from models import ProjectCreate, ProjectUpdate

FAILURES = []

def check(label, actual, expected):
    ok = actual == expected
    print(f"  {'PASS' if ok else 'FAIL'}  {label}: got {actual!r}, expected {expected!r}")
    if not ok:
        FAILURES.append(label)

async def main():
    project = await db.create_project(None, ProjectCreate(
        name='ZZ compat cohort test', description='temporary',
        allowed_cohort_id=30,
    ))
    pid = project.id
    print(f"created project id={pid}")
    try:
        check("create through compat layer", project.allowed_cohort_id, 30)

        cleared = await db.update_project(None, pid, ProjectUpdate(allowed_cohort_id=None))
        check("clear cohort -> open access", cleared.allowed_cohort_id, None)

        relocked = await db.update_project(None, pid, ProjectUpdate(allowed_cohort_id=30))
        check("re-lock to cohort", relocked.allowed_cohort_id, 30)

        renamed = await db.update_project(None, pid, ProjectUpdate(name='ZZ renamed'))
        check("rename preserves cohort", renamed.allowed_cohort_id, 30)
    finally:
        await db.delete_project(None, pid)
        print(f"cleaned up project id={pid}")

asyncio.run(main())
print("\nRESULT:", "ALL PASS" if not FAILURES else f"{len(FAILURES)} FAILED: {FAILURES}")
sys.exit(1 if FAILURES else 0)
