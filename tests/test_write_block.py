
import fastavro
from fastavro.six import MemoryIO
from tempfile import NamedTemporaryFile

schema = {
    "type": "record",
    "name": "test_block_iteration",
    "fields": [
        {
            "name": "nullable_str",
            "type": ["string", "null"]
        }, {
            "name": "str_field",
            "type": "string"
        }, {
            "name": "int_field",
            "type": "int"
        }
    ]
}


def make_records(num_records=2000):
    return [
        {
            "nullable_str": None if i % 3 == 0 else "%d-%d" % (i, i),
            "str_field": "%d %d %d" % (i, i, i),
            "int_field": i * 10
        }
        for i in range(num_records)
    ]


def make_blocks(num_records=2000, codec='null', write_to_disk=False):
    records = make_records(num_records)

    new_file = NamedTemporaryFile() if write_to_disk else MemoryIO()
    fastavro.writer(new_file, schema, records, codec=codec)
    bytes = new_file.tell()

    new_file.seek(0)
    block_reader = fastavro.block_reader(new_file, schema)

    blocks = list(block_reader)

    new_file.close()

    return blocks, records, bytes


def check_round_trip(write_to_disk):

    blocks, records, bytes = make_blocks(write_to_disk=write_to_disk)

    assert bytes == 46007

    new_file = MemoryIO()
    w = fastavro.write.Writer(new_file, schema)
    for block in blocks:
        w.write_block(block)

    new_bytes = new_file.tell()
    assert new_bytes == bytes

    # Read the file back to make sure we get back the same stuff
    new_file.seek(0)
    new_records = list(fastavro.reader(new_file, schema))
    assert len(records) == len(new_records)


def test_block_iteration_disk():
    check_round_trip(write_to_disk=True)


def test_block_iteration_memory():
    check_round_trip(write_to_disk=False)
