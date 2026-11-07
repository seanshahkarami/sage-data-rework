"""Create isolated baseline/candidate source snapshots; never edit the API checkout.

Archived exp108 tool: --api must point to a pre-adoption source snapshot.
Only the candidate snapshot replaces standalone selection. Module pins,
aggregation, staging and the rest of the application are copied unchanged.
"""
import argparse
import difflib
import hashlib
import json
from pathlib import Path
import shutil
import subprocess


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(api,output):
    api,output=api.resolve(),output.resolve()
    if output.exists() or output.is_relative_to(api) or any('node-data' in p.parts for p in (api,output)):raise ValueError('fresh isolated build root required')
    if not (api/'internal/backend/cold/selection_reduce_probe_test.go').is_file():
        raise ValueError('exp108 is archived; use its retained pre-adoption baseline snapshot, not the current API checkout')
    files=[Path(name) for name in ('go.mod','go.sum','Dockerfile','.dockerignore','main.go')]
    # The full suite reads these assets relative to internal/httpapi. They are
    # identical in both copies and excluded from the runtime Docker context.
    files += [Path('observability/grafana/beehive-data-api.json'),
              Path('observability/prometheus/alerts.yaml')]
    files+=sorted(p.relative_to(api) for p in (api/'internal').rglob('*.go'))
    if any(p.is_symlink() for p in (api/'internal').rglob('*')):raise ValueError('source symlinks are not copied')
    for name in files:
        if not (api/name).is_file() or (api/name).is_symlink():raise ValueError('source file missing or symlink: '+str(name))
    production=[api/name for name in files if name.suffix=='.go' and not name.name.endswith('_test.go')]
    if any('//go:embed' in p.read_text() for p in production):raise ValueError('embedded assets require an explicit snapshot update')
    output.mkdir(parents=True,mode=0o700)
    for role in ('baseline','candidate'):
        root=output/role;root.mkdir(mode=0o700)
        for name in files:
            target=root/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(api/name,target)
    candidate=output/'candidate';baseline=output/'baseline'
    name=Path('internal/backend/cold/selection_sql.go');path=candidate/name;source=path.read_text()
    declaration='func buildSelectionSQL('
    if source.count(declaration)!=1:raise ValueError('expected retained standalone selector declaration')
    path.write_text(source.replace(declaration,'func buildCanonicalSelectionSQL(',1))
    probe=(api/'internal/backend/cold/selection_reduce_probe_test.go').read_text()
    begin=probe.index('func buildRawCandidateSelection(');end=probe.index('\nfunc TestRawCandidateSelectionParity',begin)
    body=probe[begin:end]
    call='buildSelectionSQL(q, "SELECT * FROM raw_candidates", now)'
    if body.count(call)!=1:raise ValueError('expected retained final selection call')
    body=body.replace('func buildRawCandidateSelection(','func buildSelectionSQL(',1).replace(call,'buildCanonicalSelectionSQL(q, "SELECT * FROM raw_candidates", now)')
    wrapper=candidate/'internal/backend/cold/selection_candidate.go'
    wrapper.write_text('// Candidate build snapshot only; never written to the API checkout.\npackage cold\nimport(model "beehive-data-api/internal/query";"fmt";"strings";"time")\n'+body)
    # Retain the experimental/diagnostic test baselines as the direct plan;
    # HTTP tests still call the actual candidate through Backend.Query.
    for filename in ('selection_reduce_probe_test.go','selection_isolation_test.go'):
        path=candidate/'internal/backend/cold'/filename;text=path.read_text()
        if 'buildSelectionSQL(' not in text:raise ValueError('expected test baseline references')
        path.write_text(text.replace('buildSelectionSQL(','buildCanonicalSelectionSQL('))
    subprocess.run(['gofmt','-w',str(candidate/name),str(wrapper),
        str(candidate/'internal/backend/cold/selection_reduce_probe_test.go'),str(candidate/'internal/backend/cold/selection_isolation_test.go')],check=True)
    originals={str(name):digest(api/name) for name in files}
    copies={role:{str(p.relative_to(output/role)):digest(p) for p in sorted((output/role).rglob('*')) if p.is_file()} for role in ('baseline','candidate')}
    changed=sorted(k for k in set(copies['baseline'])|set(copies['candidate']) if copies['baseline'].get(k)!=copies['candidate'].get(k))
    expected=sorted('internal/backend/cold/'+n for n in ('selection_sql.go','selection_candidate.go','selection_reduce_probe_test.go','selection_isolation_test.go'))
    if changed!=expected or copies['baseline']!=originals:raise ValueError('unexpected snapshot changes')
    delta=[]
    for name in changed:
        before=(baseline/name).read_text().splitlines(True) if (baseline/name).exists() else []
        delta+=difflib.unified_diff(before,(candidate/name).read_text().splitlines(True),fromfile='baseline/'+name,tofile='candidate/'+name)
    (output/'candidate.patch').write_text(''.join(delta))
    manifest=dict(status='prepared',scope=__doc__,source=str(api),files=copies,changed=changed,
        notes=['Production changes are one builder rename plus the exact experimental wrapper; only two diagnostic test files redirect their baselines.',
               'Candidate correctness must run in the candidate snapshot, not the API checkout.',
               'The retained direct SQL byte-shape capture is a baseline-only test; disable HEAD_TAIL_VALIDATED_REPORT in the candidate suite.',
               'Native/HTTP/fleet results and adoption remain pending. No runtime flags or main-checkout changes.'])
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    if any(digest(api/Path(name))!=value for name,value in originals.items()):raise ValueError('API checkout changed during snapshot preparation')
    return manifest


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--api',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args()
    result=prepare(a.api,a.output);print(json.dumps({'status':result['status'],'changed':result['changed']},indent=2))

if __name__=='__main__':main()
