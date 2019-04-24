"""Partitions test module."""

from splitio.models.grammar import partitions

class PartitionTests(object):
    """Partition model tests."""

    raw = {
        'treatment': 'on',
        'size': 50
    }

    def test_parse(self):
        """Test that the partition is parsed correctly."""
        p = partitions.from_raw(self.raw)
        assert isinstance(p, partitions.Partition)
        assert p.treatment == 'on'
        assert p.size == 50

    def test_to_json(self):
        """Test the JSON representation."""
        as_json = partitions.from_raw(self.raw).to_json()
        assert as_json['treatment'] == 'on'
        assert as_json['size'] == 50
