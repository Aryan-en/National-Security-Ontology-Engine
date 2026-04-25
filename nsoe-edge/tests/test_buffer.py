"""Unit tests for RocksDB event buffer (using in-memory fallback)."""

import pytest
from nsoe_edge.buffer.rocksdb_buffer import RocksDBEventBuffer


@pytest.fixture
def buf(tmp_path):
    b = RocksDBEventBuffer(str(tmp_path / "testbuf"))
    b.open()
    yield b
    b.close()


class TestRocksDBEventBuffer:
    def test_put_assigns_sequential_seqs(self, buf):
        seq1 = buf.put({"event_id": "a"})
        seq2 = buf.put({"event_id": "b"})
        assert seq2 == seq1 + 1

    def test_unacknowledged_returns_all_when_none_acked(self, buf):
        buf.put({"event_id": "a"})
        buf.put({"event_id": "b"})
        items = list(buf.get_unacknowledged())
        assert len(items) == 2

    def test_acknowledged_events_excluded(self, buf):
        seq1 = buf.put({"event_id": "a"})
        seq2 = buf.put({"event_id": "b"})
        buf.acknowledge(seq1)
        items = list(buf.get_unacknowledged())
        seqs = [s for s, _ in items]
        assert seq1 not in seqs
        assert seq2 in seqs

    def test_acknowledge_all(self, buf):
        seq1 = buf.put({"event_id": "a"})
        seq2 = buf.put({"event_id": "b"})
        buf.acknowledge(seq2)
        items = list(buf.get_unacknowledged())
        assert len(items) == 0

    def test_event_data_preserved(self, buf):
        event = {"event_id": "test-123", "camera_id": "cam01", "track_id": "t001"}
        seq = buf.put(event)
        items = list(buf.get_unacknowledged())
        assert len(items) == 1
        returned_seq, returned_event = items[0]
        assert returned_seq == seq
        assert returned_event["event_id"] == "test-123"
        assert returned_event["camera_id"] == "cam01"
