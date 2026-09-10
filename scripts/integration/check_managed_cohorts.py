"""The elicitor's organisation dropdown must actually be populated.

It was empty for every user, forever, because the old query required a
cohort-sync enrolment instance and no course has ever had one.
"""
import sys
sys.path.insert(0, '/opt/video_elicitation_annotation_tool/backend')
import pymysql.cursors, os
from dotenv import dotenv_values

env = dotenv_values('/opt/video_elicitation_annotation_tool/.env')
import main as m

FAILURES = []
def check(label, actual, expected):
    ok = actual == expected
    print(f"  {'PASS' if ok else 'FAIL'}  {label}: got {actual!r}, expected {expected!r}")
    if not ok: FAILURES.append(label)

conn = pymysql.connect(host='localhost', user='moodleuser',
                       password=env.get('MOODLE_DB_PASSWORD') or os.getenv('MOODLE_DB_PASSWORD',''),
                       database='moodle', cursorclass=pymysql.cursors.DictCursor)

with conn:
    with conn.cursor() as cur:
        # Staff path: every published organisation.
        cur.execute(m._ALL_ORGS_QUERY)
        allorgs = cur.fetchall()
        names = [r['name'] for r in allorgs]
        check("staff see Louis Vuitton", 'Louis Vuitton' in names, True)
        check("staff do NOT see unpublished cohorts", '20-21' in names, False)

        # Named-manager path: nobody is a manager yet, so it must be empty.
        cur.execute(m._MANAGED_COHORTS_QUERY, (2,))
        check("non-manager gets empty list", len(cur.fetchall()), 0)

        # Name a manager, then it should appear.
        orgid = None
        with conn.cursor() as c2:
            c2.execute("SELECT o.id FROM mdl_local_orgrequest_org o JOIN mdl_cohort c ON c.id=o.cohortid WHERE c.idnumber='org-lv'")
            orgid = c2.fetchone()['id']
            c2.execute("INSERT INTO mdl_local_orgrequest_manager (orgid, userid, timecreated) VALUES (%s, %s, %s)",
                       (orgid, 296, 1))
            conn.commit()
        try:
            cur.execute(m._MANAGED_COHORTS_QUERY, (296,))
            rows = cur.fetchall()
            check("named manager sees their org", [r['name'] for r in rows], ['Louis Vuitton'])
        finally:
            with conn.cursor() as c2:
                c2.execute("DELETE FROM mdl_local_orgrequest_manager WHERE orgid=%s AND userid=296", (orgid,))
                conn.commit()
            print("  cleanup: removed test manager row")

print("\nRESULT:", "ALL PASS" if not FAILURES else f"{len(FAILURES)} FAILED: {FAILURES}")
sys.exit(1 if FAILURES else 0)
