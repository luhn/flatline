import pytest
from datetime import datetime as DateTime
import json
from mock import Mock
from flatline import *


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
        ('foobar', 'serfHealth', worker.HEALTHY),
        ('foobar', 'service:redis', worker.UNHEALTHY),
    ]
    consul.call.assert_called_once_with(
        'GET', 'v1/health/state/any', {}, return_index=True,
    )
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
        ('healthy', '1', worker.HEALTHY),
        ('unhealthy2', '2', worker.HEALTHY),
        ('healthy', '3', worker.HEALTHY),
        ('unhealthy', '4', worker.UNHEALTHY),
        ('unhealthy2', '5', worker.UNHEALTHY),
        ('healthy', '6', worker.HEALTHY),
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
        ('healthy', '1', worker.HEALTHY),
        ('unhealthy2', '2', worker.HEALTHY),
        ('healthy', '3', worker.HEALTHY),
        ('unhealthy', '4', worker.UNHEALTHY),
        ('unhealthy2', '5', worker.UNHEALTHY),
        ('healthy', '6', worker.HEALTHY),
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
        ('healthy', '1', worker.HEALTHY),
        ('unhealthy2', '2', worker.HEALTHY),
        ('healthy', '3', worker.HEALTHY),
        ('unhealthy', '4', worker.UNHEALTHY),
        ('unhealthy2', '5', worker.HEALTHY),
        ('healthy', '6', worker.HEALTHY),
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


def test_update_health_maintenance_mode():
    worker = Worker(None, None, None)
    worker.node_status = {
        'healthy': worker.HEALTHY,
    }
    worker.query_health_checks = Mock(return_value=[
        ('healthy', '1', worker.UNHEALTHY),
        ('healthy', '_node_maintenance', worker.UNHEALTHY),
    ])
    r = worker.update_health_checks()
    assert set(r) == set()
    assert worker.node_status == {
        'healthy': worker.HEALTHY,
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


def test_get_instance_id_not_found():
    ec2 = Mock()
    ec2.describe_instances = Mock(return_value={
        'Reservations': [],
        'NextToken': 'string'
    })
    worker = Worker(None, ec2, None)
    with pytest.raises(InstanceNotFound):
        worker.get_instance_id('10.0.1.123')


def test_is_asg_instance():
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
    worker = Worker(None, None, asg)
    assert worker.is_asg_instance('i-1234')
    asg.describe_auto_scaling_instances.assert_called_once_with(
        InstanceIds=['i-1234'],
    )


def test_is_asg_instance_no_exists():
    asg = Mock()
    asg.describe_auto_scaling_instances = Mock(return_value={
        'AutoScalingInstances': [],
    })
    worker = Worker(None, None, asg)
    assert worker.is_asg_instance('i-1234') is False


def _test_is_asg_instance_not_in_service():
    asg = Mock()
    asg.describe_auto_scaling_instances = Mock(return_value={
        'AutoScalingInstances': [
            {
                'InstanceId': 'string',
                'AutoScalingGroupName': 'string',
                'AvailabilityZone': 'string',
                'LifecycleState': 'Terminating',
                'HealthStatus': 'string',
                'LaunchConfigurationName': 'string',
                'ProtectedFromScaleIn': True,
            },
        ],
    })
    worker = Worker(None, None, asg)
    assert worker.is_asg_instance('i-1234')


def test_update_instance_health():
    asg = Mock()
    worker = Worker(None, None, asg)
    worker.update_instance_health('i-1234', worker.HEALTHY)
    asg.set_instance_health.assert_called_once_with(
        InstanceId='i-1234',
        HealthStatus='Healthy',
    )
