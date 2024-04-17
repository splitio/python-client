"""Condition model tests module."""
import pytest
import csv
import os

from splitio.models.grammar.matchers.semver import Semver

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
                assert Semver.build(row['higher']) is not None
                assert Semver.build(row['lower']) is not None

    def test_invalid_versions(self):
        with open(invalid_versions) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                assert Semver.build(row['invalid']) is None

    def test_compare(self):
        with open(valid_versions) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                assert Semver.build(row['higher']).compare(Semver.build(row['lower'])) == 1
                assert Semver.build(row['lower']).compare(Semver.build(row['higher'])) == -1

        with open(equalto_versions) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                version1 = Semver.build(row['version1'])
                version2 = Semver.build(row['version2'])
                if row['equals'] == "true":
                    assert version1.version == version2.version
                else:
                    assert version1.version != version2.version

        with open(between_versions) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                version1 = Semver.build(row['version1'])
                version2 = Semver.build(row['version2'])
                version3 = Semver.build(row['version3'])
                if row['expected'] == "true":
                    assert version2.compare(version1) >= 0 and version3.compare(version2) >= 0
                else:
                    assert version2.compare(version1) < 0 or version3.compare(version2) < 0
