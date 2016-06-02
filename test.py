import pytest
from datetime import datetime as DateTime
from collections import namedtuple
import json
from mock import Mock
from flatline import *


def test_check():
    check = Check({
        "Node": "foobar",
        "CheckID": "serfHealth",
        "Name": "Serf Health Status",
        "Status": "passing",
        "Notes": "",
        "Output": "",
        "ServiceID": "",
        "ServiceName": ""
    })
    assert check.healthy is True
    assert check.id == 'serfHealth'
    assert check.node == 'foobar'
    check = Check({
        "Node": "foobar",
        "CheckID": "service:redis",
        "Name": "Service 'redis' check",
        "Status": "critical",
        "Notes": "",
        "Output": "",
        "ServiceID": "redis",
        "ServiceName": "redis"
    })
    assert check.healthy is False
    assert check.id == 'service:redis'
    assert check.node == 'foobar'


def test_get_checks_cold():
    check1 = {
        "Node": "foobar",
        "CheckID": "serfHealth",
        "Name": "Serf Health Status",
        "Status": "passing",
        "Notes": "",
        "Output": "",
        "ServiceID": "",
        "ServiceName": ""
    }
    check2 = {
        "Node": "foobar",
        "CheckID": "service:redis",
        "Name": "Service 'redis' check",
        "Status": "critical",
        "Notes": "",
        "Output": "",
        "ServiceID": "redis",
        "ServiceName": "redis"
    }
    consul = Consul()
    consul.call = Mock(return_value=([check1, check2], '12'))
    worker = Worker(consul, None, None)
    checks = worker.get_checks()
    assert checks == [Check(check1), Check(check2)]
    consul.call.assert_called_once_with(
        'GET', 'v1/health/state/any', {},
    )
    worker.last_index = '12'


def test_get_checks_warm():
    consul = Consul()
    consul.call = Mock(return_value=([], '13'))
    worker = Worker(consul, None, None)
    worker.last_index = '12'
    assert worker.get_checks() == []
    consul.call.assert_called_once_with('GET', 'v1/health/state/any', {
        'wait': '60s',
        'index': '12'
    })
    worker.last_index = '13'


MockCheck = namedtuple('MockCheck', ['node', 'id', 'healthy'])


def test_node_healthy():
    node = Node(None, None, None, 'healthy', [
        MockCheck('healthy', '1', True),
        MockCheck('healthy', '2', True),
    ])
    assert node.healthy is True

    node = Node(None, None, None, 'unhealthy', [
        MockCheck('unhealthy', '1', True),
        MockCheck('unhealthy', '2', False),
    ])
    assert node.healthy is False


def test_node_maintenance():
    node = Node(None, None, None, 'healthy', [
        MockCheck('healthy', '1', True),
        MockCheck('healthy', '_node_maintenance', False),
    ])
    assert node.maintenance is True

    node = Node(None, None, None, 'unhealthy', [
        MockCheck('unhealthy', '1', True),
        MockCheck('unhealthy', '2', False),
    ])
    assert node.healthy is False


def test_node_blob():
    consul = Consul()
    consul.call = Mock(return_value=({'bar': 'foo'}, None))
    node = Node(consul, None, None, 'foobar', [])
    assert node.blob == {'bar': 'foo'}
    assert node.blob == {'bar': 'foo'}  # Call twice to verify sure @reify
    consul.call.assert_called_once_with('GET', 'v1/catalog/node/foobar', {})


def test_node_ip(monkeypatch):
    node = Node(None, None, None, 'foobar', [])
    monkeypatch.setattr(Node, 'blob', json.loads(
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
        """
    ))
    assert node.ip == '10.1.10.12'


def test_node_instance_id(monkeypatch):
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
    monkeypatch.setattr(Node, 'ip', '10.0.1.123')
    node = Node(None, ec2, None, 'foobar', [])
    assert node.instance_id == 'i-1234'
    assert node.instance_id == 'i-1234'
    ec2.describe_instances.assert_called_once_with(
        Filters=[
            {
                'Name': 'private-ip-address',
                'Values': ['10.0.1.123'],
            },
        ],
    )


def test_node_instance_id_not_found(monkeypatch):
    ec2 = Mock()
    ec2.describe_instances = Mock(return_value={
        'Reservations': [],
        'NextToken': 'string'
    })
    monkeypatch.setattr(Node, 'ip', '10.0.1.123')
    node = Node(None, ec2, None, 'foobar', [])
    assert node.instance_id is None


def test_node_is_asg_instance_true(monkeypatch):
    asg = Mock()
    asg.describe_auto_scaling_instances = Mock(return_value={
        'AutoScalingInstances': [
            {
                'InstanceId': 'string',
                'AutoScalingGroupName': 'string',
                'AvailabilityZone': 'string',
                'LifecycleState': 'InService',
                'HealthStatus': 'string',
                'LaunchConfigurationName': 'string',
                'ProtectedFromScaleIn': True,
            },
        ],
    })
    monkeypatch.setattr(Node, 'instance_id', 'i-1234')
    node = Node(None, None, asg, 'foobar', [])
    assert node.is_asg_instance is True
    asg.describe_auto_scaling_instances.assert_called_once_with(
        InstanceIds=['i-1234'],
    )


def test_node_is_asg_instance_false(monkeypatch):
    asg = Mock()
    asg.describe_auto_scaling_instances = Mock(return_value={
        'AutoScalingInstances': [],
    })
    monkeypatch.setattr(Node, 'instance_id', 'i-1234')
    node = Node(None, None, asg, 'foobar', [])
    assert node.is_asg_instance is False


def test_node_is_asg_instance_no_instance_id(monkeypatch):
    monkeypatch.setattr(Node, 'instance_id', None)
    node = Node(None, None, None, 'foobar', [])
    assert node.is_asg_instance is False


def test_node_update_instance_health(monkeypatch):
    asg = Mock()
    monkeypatch.setattr(Node, 'instance_id', 'i-1234')
    node = Node(None, None, asg, 'foobar', [])
    node.update_instance_health()
    asg.set_instance_health.assert_called_once_with(
        InstanceId='i-1234',
        HealthStatus='Healthy',
    )


def test_get_nodes(monkeypatch):
    checks = [
        MockCheck('healthy', '1', True),
        MockCheck('unhealthy2', '2', True),
        MockCheck('healthy', '3', True),
        MockCheck('unhealthy', '4', False),
        MockCheck('unhealthy2', '5', False),
        MockCheck('healthy', '6', True),
        MockCheck('maint', '7', False),
    ]
    monkeypatch.setattr(Worker, 'get_checks', lambda _: checks)
    monkeypatch.setattr(
        Node,
        'maintenance',
        property(lambda x: x.name == 'maint'),
    )
    worker = Worker('consul', 'ec2', 'asg')
    nodes = worker.get_nodes()
    assert set(nodes.keys()) == {'healthy', 'unhealthy', 'unhealthy2'}
    assert nodes['healthy'].name == 'healthy'
    assert set(nodes['healthy'].checks) == {checks[0], checks[2], checks[5]}
    assert nodes['healthy'].consul == 'consul'
    assert nodes['healthy'].ec2 == 'ec2'
    assert nodes['healthy'].asg == 'asg'
    assert nodes['unhealthy'].name == 'unhealthy'
    assert set(nodes['unhealthy'].checks) == {checks[3]}
    assert nodes['unhealthy2'].name == 'unhealthy2'
    assert set(nodes['unhealthy2'].checks) == {checks[1], checks[4]}


def test_diff_nodes(monkeypatch):
    _Node = namedtuple('Node', ['name', 'healthy'])
    worker = Worker(None, None, None)
    diff = worker.diff_nodes({
        '1': _Node('1', False),
        '2': _Node('2', False),
        '3': _Node('3', True),
    }, {
        '2': _Node('2', True),
        '3': _Node('3', True),
        '4': _Node('4', False),
    })
    assert {x.name for x in diff} == {'2', '4'}


def test_update_healthg(monkeypatch):
    node1 = Mock(is_asg_instance=True)
    node2 = Mock(is_asg_instance=False)
    get_nodes = Mock(return_value='mynodes')
    diff_nodes = Mock(return_value=[node1, node2])
    monkeypatch.setattr(Worker, 'get_nodes', get_nodes)
    monkeypatch.setattr(Worker, 'diff_nodes', diff_nodes)
    worker = Worker(None, None, None)
    worker.prev_nodes = 'prevnodes'
    worker.update_health()
    get_nodes.assert_called_once_with()
    assert worker.prev_nodes == 'mynodes'
    diff_nodes.assert_called_once_with('prevnodes', 'mynodes')
    node1.update_instance_health.assert_called_once_with()
    node2.update_instance_health.assert_not_called()
