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
        get_ip_mock = mocker.Mock()
        get_host_mock = mocker.Mock()
        mocker.patch('splitio.client.util._get_ip', new=get_ip_mock)
        mocker.patch('splitio.client.util._get_hostname', new=get_host_mock)

        meta = util.get_metadata({'machineIp': 'some_ip', 'machineName': 'some_machine_name'})
        assert get_ip_mock.mock_calls == []
        assert get_host_mock.mock_calls == []
        assert meta.instance_ip == 'some_ip'
        assert meta.instance_name == 'some_machine_name'
        assert meta.sdk_version == 'python-' + __version__

        meta = util.get_metadata(config.DEFAULT_CONFIG)
        assert get_ip_mock.mock_calls == [mocker.call()]
        assert get_host_mock.mock_calls == [mocker.call(mocker.ANY)]

        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'ipAddressesEnabled': False})
        meta = util.get_metadata(cfg)
        assert meta.instance_ip == 'NA'
        assert meta.instance_name == 'NA'

        get_ip_mock.reset_mock()
        get_host_mock.reset_mock()
        meta = util.get_metadata({})
        assert get_ip_mock.mock_calls == [mocker.call()]
        assert get_host_mock.mock_calls == [mocker.call(mocker.ANY)]
