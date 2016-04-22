import pytest
from datetime import datetime as DateTime
import json
from unittest.mock import Mock
from flatline import *


def test_create_session():
    consul = Consul()
    consul.call = Mock(return_value={
        'ID': '03a3e15e-01fb-41d3-9ed6-243fb8dc0f1b',
    })
    session = Session(consul, 'flatline')
    session.id = '03a3e15e-01fb-41d3-9ed6-243fb8dc0f1b'
    consul.call.assert_called_once_with('PUT', 'v1/session/create', data={
        'Name': 'flatline',
    })


def test_acquire_lock():
    consul = Consul()
    consul.call = Mock(return_value=True)
    session = Mock()
    session.id = '03a3e15e-01fb-41d3-9ed6-243fb8dc0f1b'
    acquire_lock(consul, 'abc', session)
    consul.call.assert_called_once_with('PUT', 'v1/kv/abc', data={}, params={
        'acquire': session.id,
    })


def test_acquire_lock_with_retry():
    consul = Consul()
    consul.call = Mock(side_effect=[False, False, True])
    session = Mock()
    session.id = '03a3e15e-01fb-41d3-9ed6-243fb8dc0f1b'
    acquire_lock(consul, 'abc', session, retry_delay=0)
    consul.call.assert_called_with('PUT', 'v1/kv/abc', data={}, params={
        'acquire': session.id,
    })


def test_release_lock():
    consul = Consul()
    consul.call = Mock(return_value=True)
    session = Mock()
    session.id = '03a3e15e-01fb-41d3-9ed6-243fb8dc0f1b'
    release_lock(consul, 'abc', session)
    consul.call.assert_called_with('PUT', 'v1/kv/abc', data={}, params={
        'release': session.id,
    })


def test_check_lock_still_holding():
    consul = Consul()
    consul.call = Mock(return_value=[
        {
            'CreateIndex': 100,
            'ModifyIndex': 200,
            'LockIndex': 200,
            'Key': 'zip',
            'Flags': 0,
            'Value': 'dGVzdA==',
            'Session': '03a3e15e-01fb-41d3-9ed6-243fb8dc0f1b'
        }
    ])
    session = Mock()
    session.id = '03a3e15e-01fb-41d3-9ed6-243fb8dc0f1b'
    check_lock(consul, 'abc', session)
    consul.call.assert_called_once_with('GET', 'v1/kv/abc', {})


def test_check_lock_other_holder():
    consul = Consul()
    consul.call = Mock(return_value=[
        {
            'CreateIndex': 100,
            'ModifyIndex': 200,
            'LockIndex': 200,
            'Key': 'zip',
            'Flags': 0,
            'Value': 'dGVzdA==',
            'Session': 'b31dae71-0fd2-4c47-b4ce-8882323ea8a3'
        }
    ])
    session = Mock()
    session.id = '03a3e15e-01fb-41d3-9ed6-243fb8dc0f1b'
    with pytest.raises(LockLost):
        check_lock(consul, 'abc', session)
    consul.call.assert_called_once_with('GET', 'v1/kv/abc', {})


def test_check_lock_no_hold():
    consul = Consul()
    consul.call = Mock(return_value=[
        {
            'CreateIndex': 100,
            'ModifyIndex': 200,
            'LockIndex': 200,
            'Key': 'zip',
            'Flags': 0,
            'Value': 'dGVzdA==',
            'Session': ''
        }
    ])
    session = Mock()
    session.id = '03a3e15e-01fb-41d3-9ed6-243fb8dc0f1b'
    with pytest.raises(LockLost):
        check_lock(consul, 'abc', session)
    consul.call.assert_called_once_with('GET', 'v1/kv/abc', {})


def test_query_health_check_clean_slate():
    consul = Consul()
    consul.call = Mock(return_value=([
        {
            "Node": "foobar",
            "CheckID": "serfHealth",
            "Name": "Serf Health Status",
            "Status": "passing",
            "Notes": "",
            "Output": "",
            "ServiceID": "",
            "ServiceName": ""
        },
        {
            "Node": "foobar",
            "CheckID": "service:redis",
            "Name": "Service 'redis' check",
            "Status": "critical",
            "Notes": "",
            "Output": "",
            "ServiceID": "redis",
            "ServiceName": "redis"
        }
    ], '12'))
    worker = Worker(consul, None, None)
    checks = worker.query_health_checks()
    assert list(checks) == [
        ('foobar', worker.HEALTHY),
        ('foobar', worker.UNHEALTHY),
    ]
    consul.call.assert_called_once_with('GET', 'v1/health/state/any', {
        'wait': '10s',
    }, return_index=True)
    worker.last_index = '12'


def test_query_health_check_index():
    consul = Consul()
    consul.call = Mock(return_value=([], '13'))
    worker = Worker(consul, None, None)
    worker.last_index = '12'
    list(worker.query_health_checks())
    consul.call.assert_called_once_with('GET', 'v1/health/state/any', {
        'wait': '10s',
        'index': '12'
    }, return_index=True)
    worker.last_index = '13'


def test_update_health_checks_blank():
    worker = Worker(None, None, None)
    worker.query_health_checks = Mock(return_value=[
        ('healthy', worker.HEALTHY),
        ('unhealthy2', worker.HEALTHY),
        ('healthy', worker.HEALTHY),
        ('unhealthy', worker.UNHEALTHY),
        ('unhealthy2', worker.UNHEALTHY),
        ('healthy', worker.HEALTHY),
    ])
    r = worker.update_health_checks()
    assert set(r) == {
        ('healthy', worker.HEALTHY),
        ('unhealthy', worker.UNHEALTHY),
        ('unhealthy2', worker.UNHEALTHY),
    }
    assert worker.node_status == {
        'healthy': worker.HEALTHY,
        'unhealthy': worker.UNHEALTHY,
        'unhealthy2': worker.UNHEALTHY,
    }


def test_update_health_no_change():
    worker = Worker(None, None, None)
    worker.node_status = {
        'healthy': worker.HEALTHY,
        'unhealthy': worker.UNHEALTHY,
        'unhealthy2': worker.UNHEALTHY,
    }
    worker.query_health_checks = Mock(return_value=[
        ('healthy', worker.HEALTHY),
        ('unhealthy2', worker.HEALTHY),
        ('healthy', worker.HEALTHY),
        ('unhealthy', worker.UNHEALTHY),
        ('unhealthy2', worker.UNHEALTHY),
        ('healthy', worker.HEALTHY),
    ])
    r = worker.update_health_checks()
    assert set(r) == set()
    assert worker.node_status == {
        'healthy': worker.HEALTHY,
        'unhealthy': worker.UNHEALTHY,
        'unhealthy2': worker.UNHEALTHY,
    }


def test_update_health_partial_change():
    worker = Worker(None, None, None)
    worker.node_status = {
        'healthy': worker.HEALTHY,
        'unhealthy': worker.UNHEALTHY,
        'unhealthy2': worker.UNHEALTHY,
    }
    worker.query_health_checks = Mock(return_value=[
        ('healthy', worker.HEALTHY),
        ('unhealthy2', worker.HEALTHY),
        ('healthy', worker.HEALTHY),
        ('unhealthy', worker.UNHEALTHY),
        ('unhealthy2', worker.HEALTHY),
        ('healthy', worker.HEALTHY),
    ])
    r = worker.update_health_checks()
    assert set(r) == {
        ('unhealthy2', worker.HEALTHY),
    }
    assert worker.node_status == {
        'healthy': worker.HEALTHY,
        'unhealthy': worker.UNHEALTHY,
        'unhealthy2': worker.HEALTHY,
    }


def test_get_node_ip():
    consul = Consul()
    consul.call = Mock(return_value=json.loads(
        """
        {
          "Node": {
            "Node": "foobar",
            "Address": "10.1.10.12",
            "TaggedAddresses": {
              "wan": "10.1.10.12"
            }
          },
          "Services": {
            "consul": {
              "ID": "consul",
              "Service": "consul",
              "Tags": null,
              "Port": 8300
            },
            "redis": {
              "ID": "redis",
              "Service": "redis",
              "Tags": [
                "v1"
              ],
              "Port": 8000
            }
          }
        }
        """))
    worker = Worker(consul, None, None)
    assert worker.get_node_ip('foobar') == '10.1.10.12'
    consul.call.assert_called_once_with('GET', 'v1/catalog/node/foobar', {})


def test_get_instance_id():
    ec2 = Mock()
    ec2.describe_instances = Mock(return_value={
        'Reservations': [
            {
                'ReservationId': 'string',
                'OwnerId': 'string',
                'RequesterId': 'string',
                'Groups': [
                    {
                        'GroupName': 'string',
                        'GroupId': 'string'
                    },
                ],
                'Instances': [
                    {
                        'InstanceId': 'i-1234',
                        'ImageId': 'string',
                        'State': {
                            'Code': 123,
                            'Name': 'running',
                        },
                        'PrivateDnsName': 'string',
                        'PublicDnsName': 'string',
                        'StateTransitionReason': 'string',
                        'KeyName': 'string',
                        'AmiLaunchIndex': 123,
                        'ProductCodes': [],
                        'InstanceType': 't1.micro',
                        'LaunchTime': DateTime(2015, 1, 1),
                        'Placement': {
                            'AvailabilityZone': 'string',
                            'GroupName': 'string',
                            'Tenancy': 'default',
                            'HostId': 'string',
                            'Affinity': 'string'
                        },
                        'KernelId': 'string',
                        'RamdiskId': 'string',
                        'Platform': 'Windows',
                        'Monitoring': {
                            'State': 'enabled',
                        },
                        'SubnetId': 'string',
                        'VpcId': 'string',
                        'PrivateIpAddress': '10.0.1.123',
                        'PublicIpAddress': 'string',
                        'StateReason': {
                            'Code': 'string',
                            'Message': 'string'
                        },
                        'Architecture': 'x86_64',
                        'RootDeviceType': 'ebs',
                        'RootDeviceName': 'string',
                        'BlockDeviceMappings': [
                            {
                                'DeviceName': 'string',
                                'Ebs': {
                                    'VolumeId': 'string',
                                    'Status': 'attached',
                                    'AttachTime': DateTime(2015, 1, 1),
                                    'DeleteOnTermination': True,
                                }
                            },
                        ],
                        'VirtualizationType': 'hvm',
                        'InstanceLifecycle': 'scheduled',
                        'SpotInstanceRequestId': 'string',
                        'ClientToken': 'string',
                        'Tags': [
                            {
                                'Key': 'string',
                                'Value': 'string'
                            },
                        ],
                        'SecurityGroups': [
                            {
                                'GroupName': 'string',
                                'GroupId': 'string'
                            },
                        ],
                        'SourceDestCheck': True,
                        'Hypervisor': 'xen',
                        'NetworkInterfaces': [
                            {
                                'NetworkInterfaceId': 'string',
                                'SubnetId': 'string',
                                'VpcId': 'string',
                                'Description': 'string',
                                'OwnerId': 'string',
                                'Status': 'available',
                                'MacAddress': 'string',
                                'PrivateIpAddress': 'string',
                                'PrivateDnsName': 'string',
                                'SourceDestCheck': True,
                                'Groups': [
                                    {
                                        'GroupName': 'string',
                                        'GroupId': 'string'
                                    },
                                ],
                                'Attachment': {
                                    'AttachmentId': 'string',
                                    'DeviceIndex': 123,
                                    'Status': 'attached',
                                    'AttachTime': DateTime(2015, 1, 1),
                                    'DeleteOnTermination': True,
                                },
                                'Association': {
                                    'PublicIp': 'string',
                                    'PublicDnsName': 'string',
                                    'IpOwnerId': 'string'
                                },
                                'PrivateIpAddresses': [
                                    {
                                        'PrivateIpAddress': 'string',
                                        'PrivateDnsName': 'string',
                                        'Primary': True,
                                        'Association': {
                                            'PublicIp': 'string',
                                            'PublicDnsName': 'string',
                                            'IpOwnerId': 'string'
                                        }
                                    },
                                ]
                            },
                        ],
                        'IamInstanceProfile': {
                            'Arn': 'string',
                            'Id': 'string'
                        },
                        'EbsOptimized': True,
                        'SriovNetSupport': 'string'
                    },
                ]
            },
        ],
        'NextToken': 'string'
    })
    worker = Worker(None, ec2, None)
    assert worker.get_instance_id('10.0.1.123') == 'i-1234'
    ec2.describe_instances.assert_called_once_with(
        Filters=[
            {
                'Name': 'private-ip-address',
                'Values': ['10.0.1.123'],
            },
        ],
    )


def test_update_instance_health():
    asg = Mock()
    worker = Worker(None, None, asg)
    worker.update_instance_health('i-1234', worker.HEALTHY)
    asg.set_instance_health.assert_called_once_with(
        InstanceId='i-1234',
        HealthStatus='Healthy',
    )
