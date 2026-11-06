"""Opt-in research convention; no production exporter or reader uses this yet."""
ENCODING = 'variant-uint64-decimal-v1'
PROPERTY = 'sage.value.encoding'


def boundary_rows():
    values = [('uint', 0), ('int', 0), ('uint', 6), ('int', 6),
              ('uint', 2**53 + 1), ('int', 2**53 + 1),
              ('uint', 2**63 - 1), ('int', 2**63 - 1),
              ('uint', 2**63), ('uint', 2**64 - 1), ('int', -(2**63)),
              ('float', 6.0), ('bool', True), ('bool', False), ('string', '6')]
    return [dict(time=1740873600000000194 + i, name='exp91.boundary', kind=kind, value=value,
                 meta={'plugin': 'exp91', 'vsn': 'EXP91', 'sensor': 'encoding'})
            for i, (kind, value) in enumerate(values)]


def check_invalid_values(duck):
    """Range failures must precede any narrowing or unsigned reinterpretation."""
    for label, value in [('negative', -1), ('overflow', 2**64)]:
        sql = "SELECT " + decode_value() + " FROM (SELECT '" + str(value) + "'::DECIMAL(20,0)::VARIANT AS value)"
        try:
            duck.run(sql, 'invalid-uint-' + label)
        except RuntimeError as error:
            if 'invalid encoded uint64' in str(error):
                continue
            raise
        raise AssertionError('invalid reserved decimal accepted: ' + label)


def encode_value(value='value'):
    """Reserve DECIMAL(20,0) only for source uint64; preserve other scalars."""
    return ("CASE WHEN variant_typeof(" + value + ") = variant_typeof(0::UBIGINT::VARIANT) "
            "THEN " + value + "::UBIGINT::DECIMAL(20,0)::VARIANT ELSE " + value + " END")


def decode_value(value='value'):
    """Use only after checking the table marker; fail on invalid reserved values."""
    # Parquet may normalize a 128-bit decimal's declared precision to 38.
    return ("CASE WHEN variant_typeof(" + value + ") IN ('DECIMAL(20, 0)', 'DECIMAL(38, 0)') THEN "
            "CASE WHEN " + value + "::DECIMAL(38,0) BETWEEN 0 AND 18446744073709551615 "
            "THEN " + value + "::UBIGINT::VARIANT ELSE error('invalid encoded uint64') END "
            "ELSE " + value + " END")


def check_marker(metadata, encoding):
    marker = metadata.get('properties', {}).get(PROPERTY)
    if encoding == 'decimal20' and marker != ENCODING:
        raise ValueError('missing or wrong unsigned encoding marker')
    if encoding == 'native' and marker is not None:
        raise ValueError('encoded table requires an explicit reader convention')
