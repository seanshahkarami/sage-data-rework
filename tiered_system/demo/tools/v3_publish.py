"""Export a committed, append-only Iceberg v3 snapshot to a fresh warehouse.

DuckDB cannot PUT plain HTTPS files. Keep its REST warehouse local, then copy
the committed table graph, rebase file references and publish the hint last.
PyIceberg is used only as an Avro container codec, never as a v3 table writer.
"""
import json
import os
import re
from pathlib import Path
import shutil
from urllib.parse import quote, unquote, urlparse

import requests
from pyiceberg.avro.file import AvroFile, AvroOutputFile, AvroFileHeader, MAGIC, META_SCHEMA
from pyiceberg.avro.resolver import construct_writer
from pyiceberg.io.pyarrow import PyArrowFileIO


def catalog_base(endpoint, warehouse='warehouse', session=requests):
    endpoint = endpoint.rstrip('/')
    response = session.get(endpoint+'/v1/config', params={'warehouse':warehouse}, timeout=30)
    response.raise_for_status()
    config = response.json()
    settings = {**config.get('defaults', {}), **config.get('overrides', {})}
    prefix = settings.get('prefix', '').strip('/')
    return endpoint+'/v1'+('/'+quote(prefix,safe='/') if prefix else '')


def catalog_head(endpoint, namespace='sage', table='obs', warehouse='warehouse', session=requests):
    """REST LoadTable is authoritative, including when orphan files exist."""
    base=catalog_base(endpoint,warehouse,session)
    response = session.get(base+'/namespaces/'+quote(namespace,safe='')+'/tables/'+quote(table,safe=''), timeout=30)
    response.raise_for_status()
    result = response.json()
    if result['metadata']['format-version'] != 3:
        raise ValueError('publisher requires an Iceberg v3 committed head')
    return result['metadata-location']


def restore_registration(endpoint, publication, root, session=requests):
    """Register a known published source head after a fixture catalog restart.

    Never discovers a head by directory traversal, replaces a catalog entry or
    registers the rebased read-only replica as the writable source table.
    """
    saved=json.loads(Path(publication).read_text())
    root=Path(root).resolve()
    if (root/'seed-v3-pending.json').exists():
        raise ValueError('an INSERT has unresolved commit status; inspect the pending journal before restoring registration')
    progress=root/'seed-v3-progress.json'
    # A known later acknowledged commit supersedes the original March 1
    # publication. Never silently roll the source catalog back to that head.
    source=json.loads(progress.read_text())['source_metadata'] if progress.exists() else saved['source_metadata']
    head=local_path(source)
    if root not in head.parents:
        raise ValueError('saved source metadata is outside the shared warehouse')
    metadata=json.loads(head.read_text())
    if metadata.get('format-version')!=3 or metadata['table-uuid']!=saved['table_uuid']:
        raise ValueError('saved publication does not identify this v3 source table')
    base=catalog_base(endpoint,session=session)
    url=base+'/namespaces/sage/tables/obs'
    response=session.get(url,timeout=30)
    if response.status_code==404:
        response=session.post(base+'/namespaces',json={'namespace':['sage']},timeout=30)
        if response.status_code!=409:response.raise_for_status()
        response=session.post(base+'/namespaces/sage/register',
                              json={'name':'obs','metadata-location':str(head)},timeout=30)
        response.raise_for_status()
        response=session.get(url,timeout=30)
    response.raise_for_status()
    loaded=response.json()
    if loaded['metadata']['table-uuid']!=saved['table_uuid']:
        raise ValueError('catalog already contains a different sage.obs; refusing replacement')
    current=local_path(loaded['metadata-location'])
    if root not in current.parents:raise ValueError('catalog head is outside the source warehouse')
    return str(current)


def local_path(location):
    parsed = urlparse(str(location))
    if parsed.scheme not in ('', 'file') or parsed.netloc not in ('', 'localhost'):
        raise ValueError(f'expected a local REST warehouse file: {location}')
    path=Path(unquote(parsed.path)).resolve()
    if 'node-data' in path.parts:
        raise ValueError('/node-data is not a v3 staging warehouse')
    return path


class PreservedAvro(AvroOutputFile):
    """Preserve original field IDs, schema and v3 metadata in the Avro header."""
    def _write_header(self):
        construct_writer(META_SCHEMA).write(self.encoder, AvroFileHeader(MAGIC, self.metadata, self.sync_bytes))


def rewrite_avro(source, target, transform):
    io = PyArrowFileIO()
    with AvroFile(io.new_input(str(source))) as reader:
        with PreservedAvro(io.new_output(str(target)), reader.schema,
                           json.loads(reader.header.meta['avro.schema'])['name'],
                           metadata=dict(reader.header.meta)) as writer:
            # One manifest is bounded independently of the size of the table.
            chunk = []
            for record in reader:
                transform(record, {field.name:i for i,field in enumerate(reader.schema.fields)})
                chunk.append(record)
                if len(chunk) == 1024:
                    writer.write_block(chunk)
                    chunk = []
            if chunk:
                writer.write_block(chunk)


def export_snapshot(metadata_location, destination, published_table_uri=None):
    """Copy all reachable snapshots; destination must be a new empty path.

    published_table_uri names the *table* (…/sage/obs), while destination is a
    local staging warehouse. Files are immutable after this call. No catalog
    commit is fabricated: the published tree is a read-only snapshot replica.
    """
    head = local_path(metadata_location)
    metadata = json.loads(head.read_text())
    if metadata['format-version'] != 3:
        raise ValueError('only v3 snapshot export is supported')
    source = local_path(metadata['location'])
    destination = Path(destination).resolve()
    if 'node-data' in destination.parts or destination == source or source in destination.parents:
        raise ValueError('destination must be isolated from the source and /node-data')
    destination.mkdir(parents=True, exist_ok=False)
    target = destination/'sage'/'obs'
    uri = (published_table_uri or str(target)).rstrip('/')
    copied = {}
    visiting = set()

    def paths(location):
        path = local_path(location)
        relative = path.relative_to(source)  # Reject references outside this table.
        out = target/relative
        out.parent.mkdir(parents=True,exist_ok=True)
        return path,out,uri+'/'+relative.as_posix()

    def copy(location, kind):
        src,out,mapped = paths(location)
        if src in copied:
            return mapped,out.stat().st_size
        if src in visiting:
            raise ValueError('cycle in snapshot metadata graph')
        visiting.add(src)
        if kind == 'data':
            shutil.copyfile(src,out)
        elif kind == 'manifest':
            with AvroFile(PyArrowFileIO().new_input(str(src))) as header:
                pos = next(i for i,f in enumerate(header.schema.fields) if f.name == 'data_file')
                names = {f.name:i for i,f in enumerate(header.schema.fields[pos].field_type.fields)}
            def entry(record, fields):
                data = record[pos]
                if data[names['content']] != 0:
                    raise ValueError('snapshot exporter supports append-only data, not delete files')
                data[names['file_path']],_ = copy(data[names['file_path']], 'data')
            rewrite_avro(src,out,entry)
        elif kind == 'list':
            def manifest(record, fields):
                record[fields['manifest_path']],record[fields['manifest_length']] = copy(record[fields['manifest_path']], 'manifest')
            rewrite_avro(src,out,manifest)
        elif kind == 'metadata':
            doc = json.loads(src.read_text())
            if doc.get('statistics') or doc.get('partition-statistics'):
                raise ValueError('statistics files need explicit replication support')
            for snapshot in doc.get('snapshots', []):
                if snapshot.get('summary',{}).get('operation') not in ('append',None):
                    raise ValueError('only append snapshots are supported')
                snapshot['manifest-list'],_ = copy(snapshot['manifest-list'], 'list')
            for previous in doc.get('metadata-log', []):
                previous['metadata-file'],_ = copy(previous['metadata-file'], 'metadata')
            # Rewrite only storage paths, never arbitrary user values.
            doc['location'] = uri
            for key in ('write.data.path','write.metadata.path'):
                if key in doc.get('properties',{}):
                    doc['properties'][key] = paths(doc['properties'][key])[2]
            out.write_text(json.dumps(doc,separators=(',',':')))
        else:
            raise ValueError(kind)
        visiting.remove(src)
        copied[src] = mapped
        return mapped,out.stat().st_size

    mapped,_ = copy(head,'metadata')
    stem = head.name.removesuffix('.metadata.json')
    if stem == head.name or not stem or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._' for c in stem):
        raise ValueError('unsupported metadata filename for version-hint.text')
    # No newline. Atomic local hint replacement, only after dependencies exist.
    hint = target/'metadata'/'version-hint.text'
    temporary = hint.with_suffix('.pending')
    temporary.write_text(stem)
    os.replace(temporary,hint)
    summary = dict(format_version=3, source_metadata=str(head), metadata_location=mapped,
                   table_uuid=metadata['table-uuid'], snapshot_id=metadata.get('current-snapshot-id'),
                   files=len(copied), hint=stem)
    (destination/'publication.json').write_text(json.dumps(summary,indent=2)+'\n')
    return summary


def upload_snapshot(staging, prefix, transport):
    """Fresh prefix only. Hint is the final PUT; failures leave it undiscoverable."""
    if not re.fullmatch(r'/cold-storage-demo-v3-[A-Za-z0-9_-]+',prefix):
        raise ValueError('use a fresh /cold-storage-demo-v3-<unique> prefix')
    status,_ = transport.propfind(prefix)
    if status != 404:
        raise ValueError(f'prefix must not exist (PROPFIND {status})')
    staging = Path(staging)
    hint = staging/'sage'/'obs'/'metadata'/'version-hint.text'
    publication=json.loads((staging/'publication.json').read_text())
    expected=prefix+'/warehouse/sage/obs/metadata/'+hint.read_text()+'.metadata.json'
    location=urlparse(publication['metadata_location'])
    if location.scheme!='https' or not location.path.endswith(expected):
        raise ValueError('staged replica references do not match the target Pelican prefix')
    files = sorted(p for p in staging.rglob('*') if p.is_file() and p != hint)
    for path in files+[hint]:
        # Pelican transport accepts file-like request bodies; do not read a
        # multi-GB Parquet object into Python memory.
        with path.open('rb') as stream:
            status,_,body = transport._request(prefix+'/warehouse/'+path.relative_to(staging).as_posix(), 'PUT', data=stream,
                                             headers={'Content-Length':str(path.stat().st_size)})
        if status not in (200,201,204):
            raise IOError(f'PUT failed ({status}) for {path.name}: {body[:100]!r}')
