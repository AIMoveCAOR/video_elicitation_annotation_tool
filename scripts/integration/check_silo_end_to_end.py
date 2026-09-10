"""Phase 1 end-to-end: a project's cohort must reach ChromaDB metadata.

Simulates the full live path: project with a cohort -> annotation ingested via
CraftPilot's /api/ingest-annotation -> document carries the right silo labels.
"""
import json, sys, urllib.request
sys.path.insert(0, '/opt/video_elicitation_annotation_tool/backend')
sys.path.insert(0, '/opt/craftpilot_backend')

FAILURES = []
def check(label, actual, expected):
    ok = actual == expected
    print(f"  {'PASS' if ok else 'FAIL'}  {label}: got {actual!r}, expected {expected!r}")
    if not ok: FAILURES.append(label)

token = ''
for line in open('/opt/craftpilot_backend/.env'):
    if line.startswith('INTERNAL_API_TOKEN='):
        token = line.split('=', 1)[1].strip()

def ingest(annotation_id, project_name, cohort, craft):
    payload = json.dumps({
        "annotation_id": annotation_id, "video_id": 9999,
        "transcription": "Technique de pose du rivet sur la malle en cuir.",
        "start_time": 0.0, "end_time": 4.0,
        "video_filename": "ZZ_e2e_test.mp4", "video_filepath": "/tmp/ZZ_e2e_test.mp4",
        "source_type": "local", "project_name": project_name,
        "audio_filepath": "", "allowed_cohort_id": cohort,
        "craft": craft, "language": "fr",
    }).encode()
    req = urllib.request.Request("http://127.0.0.1:8000/api/ingest-annotation", data=payload,
        headers={"Content-Type": "application/json", "X-Internal-Token": token})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())

import chromadb
col = chromadb.PersistentClient(path='/opt/craftpilot_backend/chroma_langchain_db').get_collection('moodle_assistant_collection')

def meta_for(annotation_id):
    got = col.get(where={"annotation_id": annotation_id}, include=["metadatas"])
    return got["metadatas"][0] if got["metadatas"] else None

created = []
try:
    # A: project explicitly locked to the LV cohort
    ingest(999001, "ZZ E2E Locked", 30, "lv_rivetage_maletterie"); created.append(999001)
    m = meta_for(999001)
    check("explicit project cohort -> cohort_id", m and m.get("cohort_id"), 30)
    check("explicit project cohort -> open_access", m and m.get("open_access"), False)

    # B: project with NO cohort, but a mapped craft -> safety net must fire
    ingest(999002, "ZZ E2E Unset", None, "lv_rivetage_maletterie"); created.append(999002)
    m = meta_for(999002)
    check("safety net catches unset project", m and m.get("cohort_id"), 30)
    check("safety net -> open_access False", m and m.get("open_access"), False)

    # C: unmapped craft with no cohort -> stays open
    ingest(999003, "ZZ E2E Open", None, "glassblowing"); created.append(999003)
    m = meta_for(999003)
    check("unmapped craft stays open", m and m.get("open_access"), True)
    check("unmapped craft cohort_id", m and m.get("cohort_id"), -1)

    # D: the filter must actually hide A and B from a cohort-less user
    from services.rag_service import build_cohort_filter
    got = col.get(where=build_cohort_filter([]), include=["metadatas"])
    ids = {m.get("annotation_id") for m in got["metadatas"]}
    check("locked docs hidden from non-member", 999001 in ids or 999002 in ids, False)
    check("open doc visible to everyone", 999003 in ids, True)
finally:
    for aid in created:
        got = col.get(where={"annotation_id": aid})
        if got["ids"]:
            col.delete(ids=got["ids"])
    print(f"cleaned up {len(created)} test annotations")

print("\nRESULT:", "ALL PASS" if not FAILURES else f"{len(FAILURES)} FAILED: {FAILURES}")
sys.exit(1 if FAILURES else 0)
