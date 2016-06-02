import logging
import itertools
from time import sleep
try:
    from urllib.parse import urljoin
except ImportError:
    from urlparse import urljoin

import boto3
import requests

from .decorator import reify


logger = logging.getLogger('flatline')
logging.basicConfig(level=logging.INFO)


class Consul(object):
    """
    A simple Consul client.

    :param url:  The URL of the Consul HTTP API.
    :type url:  str

    """
    def __init__(self, url='http://localhost:8500/'):
        self.url = url

    def call(self, method, path, params={}, data={}, retry=False):
        """
        Make a call to Consul.

        :param method:  The HTTP method to use.
        :type method:  str
        :param path:  The path to query.
        :type path:  str
        :param params:  The URL parameters to send.
        :type params:  dict
        :param data:  The data to send in the body.
        :type data:  dict
        :param retry:  If ``True``, the call will be retried indefinitely if it
        fails.
        :type retry:  bool

        :returns:  A two-tuple of the decoded response body and the
        X-Consul-Index header.

        """
        url = urljoin(self.url, path)
        while True:
            try:
                logger.debug('Consul request: %s %s', method, url)
                logger.debug('Request body: %s', str(data))
                r = requests.request(
                    method,
                    url,
                    params=params,
                    json=data,
                    timeout=70,
                )
                r.raise_for_status()
                logger.debug('Consul response:  HTTP %s', r.status_code)
                logger.debug('Response body:  %s', r.text)
                return r.json(), r.headers.get('X-Consul-Index')
            except requests.RequestException:
                if not retry:
                    raise
                logger.warning('Consul error.', exc_info=True)
                logger.debug('Waiting ten seconds before trying again.')
                sleep(10)

    def get(self, path, params={}, **kwargs):
        return self.call('GET', path, params, **kwargs)

    def post(self, path, data={}, **kwargs):
        return self.call('POST', path, data=data, **kwargs)

    def put(self, path, data={}, **kwargs):
        return self.call('PUT', path, data=data, **kwargs)

    def delete(self, path, data={}, **kwargs):
        return self.call('DELETE', path, data=data, **kwargs)


class Check(object):
    """
    A representation of a Consul health check.

    :param blob:  The check JSON blob from Consul.
    :type blob:  dict

    """
    def __init__(self, blob):
        self.blob = blob
        self.healthy = blob['Status'] == 'passing'
        self.id = blob['CheckID']
        self.node = blob['Node']

    def __eq__(self, other):
        return self.blob == other.blob


class Node(object):
    """
    A representation of a node, both as a Consul client and a AWS EC2 instance.

    :param consul:  The Consul client.
    :type consul:  :class:`Consul`
    :param ec2:  The EC2 client.
    :type ec2:  :class:`boto3.EC2.Client`
    :param asg:  The ASG client
    :type asg:  :class:`boto3.AutoScaling.Client`
    :param name:  The node name.
    :type name:  str
    :param checks:  The checks associated with the node.
    :type checks:  A list of :class:`Check` objects.

    """
    def __init__(self, consul, ec2, asg, name, checks):
        self.consul = consul
        self.ec2 = ec2
        self.asg = asg
        self.name = name
        self.checks = checks

    @property
    def healthy(self):
        """
        ``True`` if all health checks are passing.

        """
        return all(check.healthy for check in self.checks)

    @property
    def maintenance(self):
        """
        ``True`` if the node is in maintenance mode.

        """
        return any(check.id == '_node_maintenance' for check in self.checks)

    @reify
    def blob(self):
        """
        The JSON node representation from Consul.

        """
        return self.consul.get('v1/catalog/node/{}'.format(self.name))[0]

    @property
    def ip(self):
        """
        The IP address of the node.

        """
        return self.blob['Node']['Address']

    @reify
    def instance_id(self):
        """
        The EC2 instance ID.

        """
        r = self.ec2.describe_instances(
            Filters=[
                {
                    'Name': 'private-ip-address',
                    'Values': [self.ip],
                },
            ],
        )
        reservations = r['Reservations']
        if len(reservations) == 0:
            return None
        instances = reservations[0]['Instances']
        if len(instances) > 1:
            raise ValueError('Multiple results found.')
        return instances[0]['InstanceId']

    @reify
    def is_asg_instance(self):
        """
        ``True`` if the instance is part of an autoscaling group.

        """
        id = self.instance_id
        if id is None:
            return False
        r = self.asg.describe_auto_scaling_instances(
            InstanceIds=[self.instance_id],
        )
        instances = r['AutoScalingInstances']
        return len(instances) > 0

    def update_instance_health(self):
        """
        Set the autoscaling health check to match the Consul health.

        """
        self.asg.set_instance_health(
            InstanceId=self.instance_id,
            HealthStatus='Healthy' if self.healthy else 'Unhealthy',
        )


class Worker(object):
    """

    """
    last_index = None

    def __init__(self, consul, ec2, asg):
        super(Worker, self).__init__()
        self.consul = consul
        self.ec2 = ec2
        self.asg = asg
        self.prev_nodes = {}

    def run(self):
        """
        Run :meth:`.update_health` indefinitely.

        """
        logger.info('Starting worker...')
        while True:
            self.update_health()

    def update_health(self):
        """
        Query Consul for health checks and update ASG health checks as
        necessary.

        """
        nodes = self.get_nodes()
        updated = self.diff_nodes(self.prev_nodes, nodes)
        self.prev_nodes = nodes
        for node in updated:
            logging.info(
                '%s is now %s',
                node.name,
                'Healthy' if node.healthy else 'Unhealthy',
            )
            if node.is_asg_instance:
                node.update_instance_health()

    def diff_nodes(self, prev_nodes, nodes):
        """
        Compare the a set of nodes to a previous set.

        :param prev_nodes:  The previous nodes.
        :type prev_nodes:  dict
        :param nodes:  The current nodes.
        :type nodes:  dict

        :returns:   Nodes that are new or have changed in health.
        :rtype:  generator

        """
        for node in nodes.values():
            prev_node = prev_nodes.get(node.name)
            if prev_node is None or prev_node.healthy is not node.healthy:
                yield node

    def get_nodes(self):
        """
        Query Consul for health checks and group them into nodes.

        :returns:  A dictionary with the node name as keys and a corresponding
        :class:`Node` as values.

        """
        nodes = dict()
        checks = sorted(self.get_checks(), key=lambda x: x.node)
        for name, checks in itertools.groupby(checks, lambda x: x.node):
            node = Node(self.consul, self.ec2, self.asg, name, list(checks))
            if not node.maintenance:
                nodes[node.name] = node
        return nodes

    def get_checks(self):
        """
        Query Consul for health checks.  Blocks for up to 60 seconds while
        waiting for changes.

        :returns:  A list of :class:`Check` objects.

        """
        logging.info('Querying Consul for health checks.')
        if self.last_index is None:
            params = {}
        else:
            params = {
                'wait': '60s',
                'index': self.last_index,
            }
        r, index = self.consul.get(
            'v1/health/state/any',
            params,
        )
        self.last_index = index
        return [Check(obj) for obj in r]


def main():
    consul = Consul()
    ec2 = boto3.client('ec2')
    asg = boto3.client('autoscaling')
    worker = Worker(consul, ec2, asg)
    worker.run()
