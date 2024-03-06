"""Condition model tests module."""
import pytest

from splitio.models.grammar.matchers.semver import Semver

class SemverTests(object):
    """Test the semver object model."""

    valid_versions = ["1.1.2", "1.1.1", "1.0.0", "1.0.0-rc.1", "1.0.0-beta.11", "1.0.0-beta.2",
        "1.0.0-beta", "1.0.0-alpha.beta", "1.0.0-alpha.1", "1.0.0-alpha", "2.2.2-rc.2+metadata-lalala", "2.2.2-rc.1.2",
        "1.2.3", "0.0.4", "1.1.2+meta", "1.1.2-prerelease+meta", "1.0.0-beta", "1.0.0-alpha", "1.0.0-alpha0.valid",
        "1.0.0-alpha.0valid", "1.0.0-rc.1+build.1", "1.0.0-alpha-a.b-c-somethinglong+build.1-aef.1-its-okay",
        "10.2.3-DEV-SNAPSHOT", "1.2.3-SNAPSHOT-123", "1.1.1-rc2", "1.0.0-0A.is.legal", "1.2.3----RC-SNAPSHOT.12.9.1--.12+788",
        "1.2.3----R-S.12.9.1--.12+meta", "1.2.3----RC-SNAPSHOT.12.9.1--.12.88", "1.2.3----RC-SNAPSHOT.12.9.1--.12",
        "9223372036854775807.9223372036854775807.9223372036854775807", "9223372036854775807.9223372036854775807.9223372036854775806",
        "1.1.1-alpha.beta.rc.build.java.pr.support.10", "1.1.1-alpha.beta.rc.build.java.pr.support"]

    def test_valid_versions(self):
        major = [1, 1, 1, 1, 1, 1,
                 1, 1, 1, 1, 2, 2,
                 1, 0, 1, 1, 1, 1, 1,
                 1, 1, 1,
                 10, 1, 1, 1, 1,
                 1, 1, 1,
                 9223372036854775807, 9223372036854775807,
                 1,1]
        minor = [1, 1, 0, 0, 0, 0,
                 0, 0, 0, 0, 2, 2,
                 2, 0, 1, 1, 0, 0, 0,
                 0, 0, 0,
                 2, 2, 1, 0, 2,
                 2, 2, 2,
                 9223372036854775807, 9223372036854775807,
                 1, 1]
        patch = [2, 1, 0, 0, 0, 0,
                 0, 0, 0, 0, 2, 2,
                 3, 4, 2, 2, 0, 0, 0,
                 0, 0, 0,
                 3, 3, 1, 0, 3,
                 3, 3, 3,
                 9223372036854775807, 9223372036854775806,
                 1, 1]
        pre_release = [[], [], [], ["rc","1"], ["beta","11"],["beta","2"],
        ["beta"], ["alpha","beta"], ["alpha","1"], ["alpha"], ["rc","2"], ["rc","1","2"],
        [], [], [], ["prerelease"], ["beta"], ["alpha"], ["alpha0","valid"],
        ["alpha","0valid"], ["rc","1"], ["alpha-a","b-c-somethinglong"],
        ["DEV-SNAPSHOT"], ["SNAPSHOT-123"], ["rc2"], ["0A","is","legal"], ["---RC-SNAPSHOT","12","9","1--","12"],
        ["---R-S","12","9","1--","12"], ["---RC-SNAPSHOT","12","9","1--","12","88"], ["---RC-SNAPSHOT","12","9","1--","12"],
        [], [],
        ["alpha","beta","rc","build","java","pr","support","10"], ["alpha","beta","rc","build","java","pr","support"]]

        for i in range(len(major)-1):
            semver = Semver(self.valid_versions[i])
            self._verify_version(semver, major[i], minor[i], patch[i], pre_release[i], pre_release[i]==[])

    def test_invalid_versions(self):
        """Test parsing invalid versions."""
        invalid_versions = [
        "1", "1.2", "1.alpha.2", "+invalid", "-invalid", "-invalid+invalid", "+justmeta",
        "-invalid.01", "alpha", "alpha.beta", "alpha.beta.1", "alpha.1", "alpha+beta",
        "alpha_beta", "alpha.", "alpha..", "beta", "-alpha.", "1.2", "1.2.3.DEV", "-1.0.3-gamma+b7718",
        "1.2-SNAPSHOT", "1.2.31.2.3----RC-SNAPSHOT.12.09.1--..12+788", "1.2-RC-SNAPSHOT"]
#        "99999999999999999999999.999999999999999999.99999999999999999----RC-SNAPSHOT.12.09.1--------------------------------..12"]

        for version in invalid_versions:
            with pytest.raises(RuntimeError):
                semver = Semver(version)
                pass

    def test_compare(self):
        cnt = 0
        for i in range(int(len(self.valid_versions)/2)):
            assert Semver(self.valid_versions[cnt]).compare(Semver(self.valid_versions[cnt+1])) == 1
            assert Semver(self.valid_versions[cnt+1]).compare(Semver(self.valid_versions[cnt])) == -1
            assert Semver(self.valid_versions[cnt]).compare(Semver(self.valid_versions[cnt])) == 0
            assert Semver(self.valid_versions[cnt+1]).compare(Semver(self.valid_versions[cnt+1])) == 0
            cnt = cnt + 2

        assert Semver("1.1.1").compare(Semver("1.1.1")) == 0
        assert Semver("1.1.1").compare(Semver("1.1.1+metadata")) == 0
        assert Semver("1.1.1").compare(Semver("1.1.1-rc.1")) == 1
        assert Semver("88.88.88").compare(Semver("88.88.88")) == 0
        assert Semver("1.2.3----RC-SNAPSHOT.12.9.1--.12").compare(Semver("1.2.3----RC-SNAPSHOT.12.9.1--.12")) == 0
        assert Semver("10.2.3-DEV-SNAPSHOT").compare(Semver("10.2.3-SNAPSHOT-123")) == -1

    def _verify_version(self, semver, major, minor, patch, pre_release="", is_stable=True):
        assert semver._major == major
        assert semver._minor == minor
        assert semver._patch == patch
        assert semver._pre_release == pre_release
        assert semver._is_stable == is_stable
