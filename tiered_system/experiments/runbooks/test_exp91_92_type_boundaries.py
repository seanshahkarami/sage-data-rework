"""Guards for opt-in unsigned encoding and metadata-driven hot reads."""
import importlib.util
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent


def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


encoding = load('exp91_variant_encoding')
reads = load('exp92_complete_hot_reads')
base = reads.base


def group(start, end, **extra):
    return dict(id=1, startTime=start, endTime=end, **extra)


def manifest(*groups):
    return dict(bucketName='fixture', bucketID='id', organizationName='org',
                retentionPolicies=[dict(name='autogen', shardGroups=list(groups))])


class TypeBoundaryTests(unittest.TestCase):
    def test_unsigned_encoding_requires_an_explicit_matching_marker(self):
        for metadata in ({}, {'properties': {encoding.PROPERTY: 'unknown'}}):
            with self.assertRaises(ValueError): encoding.check_marker(metadata, 'decimal20')
        marked = {'properties': {encoding.PROPERTY: encoding.ENCODING}}
        encoding.check_marker(marked, 'decimal20')
        with self.assertRaises(ValueError): encoding.check_marker(marked, 'native')
        encoding.check_marker({}, 'native')

    def test_boundary_fixture_distinguishes_signed_unsigned_and_float(self):
        rows = encoding.boundary_rows()
        self.assertIn(('uint', 2**64 - 1), [(r['kind'], r['value']) for r in rows])
        self.assertEqual({r['kind'] for r in rows if r['value'] == 6}, {'int', 'uint', 'float'})
        self.assertEqual(len({r['time'] for r in rows}), len(rows))

    def test_non_aligned_groups_are_clipped_without_day_or_hour_assumptions(self):
        data = manifest(group('2025-01-01T00:17:00Z', '2025-01-01T01:47:00Z'),
                        group('2025-01-01T01:47:00Z', '2025-01-01T02:47:00Z'))
        start = base.ns('2025-01-01T00:30:00.000000194Z')
        stop = base.ns('2025-01-01T02:00:00.000000195Z')
        intervals = reads.shard_ranges(data, start, stop)
        self.assertEqual([(x['start'], x['stop']) for x in intervals],
                         [(start, base.ns('2025-01-01T01:47:00Z')),
                          (base.ns('2025-01-01T01:47:00Z'), stop)])

    def test_gaps_deleted_groups_and_truncation_are_respected(self):
        data = manifest(group('2025-01-01T00:00:00Z', '2025-01-02T00:00:00Z',
                              truncatedAt='2025-01-01T01:00:00Z'),
                        group('2025-01-01T01:00:00Z', '2025-01-02T00:00:00Z',
                              deletedAt='2025-01-03T00:00:00Z'))
        result = reads.shard_ranges(data, base.ns('2025-01-01T00:00:00Z'), base.ns('2025-01-02T00:00:00Z'))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['stop'], base.ns('2025-01-01T01:00:00Z'))

    def test_overlapping_groups_fail_instead_of_claiming_complete_reads(self):
        data = manifest(group('2025-01-01T00:00:00Z', '2025-01-02T00:00:00Z'),
                        group('2025-01-01T01:00:00Z', '2025-01-03T00:00:00Z'))
        with self.assertRaisesRegex(ValueError, 'overlapping'):
            reads.shard_ranges(data, base.ns('2025-01-01T00:00:00Z'), base.ns('2025-01-03T00:00:00Z'))

    def test_multipart_backup_keeps_only_the_buckets_part(self):
        import json
        selected = manifest(group('2025-01-01T00:00:00Z', '2025-01-02T00:00:00Z'))
        boundary = 'exp92boundary'
        body = (
            f'--{boundary}\r\nContent-Disposition: attachment; name="sql"\r\n'
            'Content-Type: application/octet-stream\r\n\r\n').encode() + b'\xedCANARY-SECRET-SQL\r\n' + (
            f'--{boundary}\r\nContent-Disposition: attachment; name="buckets"\r\n'
            'Content-Type: application/json; charset=utf-8\r\n\r\n').encode() + json.dumps([selected]).encode() + (
            f'\r\n--{boundary}--\r\n').encode()
        parsed = reads.bucket_manifests_from_backup(f'multipart/mixed; boundary={boundary}', body)
        result = reads.selected_manifest(parsed, 'fixture', 'id', 'org')
        self.assertNotIn(b'CANARY', json.dumps(result).encode())
        self.assertEqual(len(result['retentionPolicies'][0]['shardGroups']), 1)

    def test_metadata_allowlist_does_not_persist_credentials_or_other_buckets(self):
        selected = manifest(group('2025-01-01T00:00:00Z', '2025-01-02T00:00:00Z', secret='CANARY'))
        selected['secret'] = 'CANARY'
        raw = {'kv': 'CANARY', 'sql': 'CANARY', 'buckets': [dict(selected, bucketID='other'), selected]}
        result = reads.selected_manifest(raw, 'fixture', 'id', 'org')
        self.assertNotIn('CANARY', repr(result))
        self.assertEqual(len(result['retentionPolicies'][0]['shardGroups']), 1)
        with self.assertRaises(ValueError): reads.selected_manifest(raw, 'fixture', 'wrong', 'org')

    def test_half_open_boundary_does_not_requery_previous_group(self):
        border = '2025-01-02T00:00:00Z'
        data = manifest(group('2025-01-01T00:00:00Z', border), group(border, '2025-01-03T00:00:00Z'))
        intervals = reads.shard_ranges(data, base.ns(border), base.ns(border) + 1)
        self.assertEqual(len(intervals), 1)
        self.assertEqual(intervals[0]['start'], base.ns(border))

    def test_live_reader_rejects_writes_and_bucket_creation_before_network(self):
        reader = reads.FixtureReader('http://localhost:8086', 'unused', 'org', 'agg-exp87-' + 'a'*32)
        for endpoint in ('/api/v2/write', '/api/v2/delete', '/api/v2/buckets'):
            with self.assertRaises(ValueError): reader.request(endpoint, {'not': 'authorized'})


if __name__ == '__main__':
    unittest.main()
