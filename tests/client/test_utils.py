"""Split client utilities test module."""
#pylint: disable=no-self-use,too-few-public-methods

import socket


from splitio.client import util, config
from splitio.version import __version__
from splitio.client.config import DEFAULT_CONFIG

class ClientUtilsTests(object):
    """Client utilities test cases."""

    def test_get_metadata(self, mocker):
        """Test the get_metadata function."""
        meta = util.get_metadata({'machineIp': 'some_ip', 'machineName': 'some_machine_name'})
        assert meta.instance_ip == 'some_ip'
        assert meta.instance_name == 'some_machine_name'
        assert meta.sdk_version == 'python-' + __version__

        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': False})
        meta = util.get_metadata(cfg)
        assert meta.instance_ip == 'NA'
        assert meta.instance_name == 'NA'

        meta = util.get_metadata(config.DEFAULT_CONFIG)
        ip_address, hostname = util._get_hostname_and_ip(config.DEFAULT_CONFIG)
        assert meta.instance_ip != 'NA'
        assert meta.instance_name != 'NA'
        assert meta.instance_ip == ip_address
        assert meta.instance_name == hostname

        self.called = 0
        def get_hostname_and_ip_mock(any):
            self.called += 0
            return mocker.Mock(), mocker.Mock()
        mocker.patch('splitio.client.util._get_hostname_and_ip', new=get_hostname_and_ip_mock)

        meta = util.get_metadata(config.DEFAULT_CONFIG)
        self.called = 1