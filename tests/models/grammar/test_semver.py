"""Condition model tests module."""
import csv
import os

from splitio.models.grammar.matchers.semver import build_semver_or_none

valid_versions = os.path.join(os.path.dirname(__file__), 'files', 'valid-semantic-versions.csv')
invalid_versions = os.path.join(os.path.dirname(__file__), 'files', 'invalid-semantic-versions.csv')
equalto_versions = os.path.join(os.path.dirname(__file__), 'files', 'equal-to-semver.csv')
between_versions = os.path.join(os.path.dirname(__file__), 'files', 'between-semver.csv')

class SemverTests(object):
    """Test the semver object model."""

    def test_valid_versions(self):
        with open(valid_versions) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                assert build_semver_or_none(row['higher']) is not None
                assert build_semver_or_none(row['lower']) is not None

    def test_invalid_versions(self):
        with open(invalid_versions) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                assert build_semver_or_none(row['invalid']) is None

    def test_compare(self):
        with open(valid_versions) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                assert build_semver_or_none(row['higher']).compare(build_semver_or_none(row['lower'])) == 1
                assert build_semver_or_none(row['lower']).compare(build_semver_or_none(row['higher'])) == -1

        with open(equalto_versions) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                version1 = build_semver_or_none(row['version1'])
                version2 = build_semver_or_none(row['version2'])
                if row['equals'] == "true":
                    assert version1.version == version2.version
                else:
                    assert version1.version != version2.version

        with open(between_versions) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                version1 = build_semver_or_none(row['version1'])
                version2 = build_semver_or_none(row['version2'])
                version3 = build_semver_or_none(row['version3'])
                if row['expected'] == "true":
                    assert version2.compare(version1) >= 0 and version3.compare(version2) >= 0
                else:
                    assert version2.compare(version1) < 0 or version3.compare(version2) < 0

    def test_leading_zeros(self):
        assert build_semver_or_none('1.01.2').version == '1.1.2'
        assert build_semver_or_none('1.01.2-rc.01').version == '1.1.2-rc.1'
