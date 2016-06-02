import logging
import itertools
from time import sleep
try:
    from urllib.parse import urljoin
except ImportError:
    from urlparse import urljoin
from threading import Thread

import boto3
import requests


logger = logging.getLogger('flatline')
logging.basicConfig(level=logging.DEBUG)


class Consul(object):
    def __init__(self, url='http://localhost:8500/'):
        self.url = url

    def call(self, method, path, params={}, data={}, retry=False,
             return_index=False):
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
                if return_index:
                    return r.json(), r.headers.get('X-Consul-Index')
                else:
                    return r.json()
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


class InstanceNotFound(Exception):
    pass


class Worker(Thread):
    cancelled = False
    last_index = None

    HEALTHY = 0
    UNHEALTHY = 1

    def __init__(self, consul, ec2, asg):
        super(Worker, self).__init__()
        self.consul = consul
        self.ec2 = ec2
        self.asg = asg
        self.node_status = {}

    def run(self):
        logger.info('Starting worker...')
        while not self.cancelled:
            self.body()

    def cancel(self):
        self.cancelled = True

    def body(self):
        updated = self.update_health_checks()
        for node, status in updated:
            logging.info(
                '%s is now %s',
                node,
                'Healthy' if status == self.HEALTHY else 'Unhealthy',
            )
            logging.info('Updating ASG health.')
            ip = self.get_node_ip(node)
            try:
                id = self.get_instance_id(ip)
            except InstanceNotFound:
                continue
            if self.is_asg_instance(id):
                self.update_instance_health(id, status)

    def update_health_checks(self):
        checks = sorted(self.query_health_checks(), key=lambda x: x[0])
        for k, g in itertools.groupby(checks, lambda x: x[0]):
            is_healthy = all(entry[2] == self.HEALTHY for entry in g)
            is_maint = any(entry[1] == '_node_maintenance' for entry in g)
            status = (
                self.HEALTHY if is_healthy or is_maint else self.UNHEALTHY
            )
            old_status = self.node_status.get(k)
            if old_status != status:
                self.node_status[k] = status
                yield k, status

    def query_health_checks(self):
        logging.info('Querying Consul for health checks.')
        if self.last_index is None:
            params = {}
        else:
            params = {
                'wait': '10s',
                'index': self.last_index,
            }
        r, index = self.consul.get(
            'v1/health/state/any',
            params,
            return_index=True,
        )
        self.last_index = index
        for entry in r:
            health = (
                self.HEALTHY if entry['Status'] == 'passing'
                else self.UNHEALTHY
            )
            yield entry['Node'], entry['CheckID'], health

    def get_node_ip(self, node_name):
        r = self.consul.get('v1/catalog/node/{}'.format(node_name))
        return r['Node']['Address']

    def get_instance_id(self, ip):
        r = self.ec2.describe_instances(
            Filters=[
                {
                    'Name': 'private-ip-address',
                    'Values': [ip],
                },
            ],
        )
        try:
            reservation = r['Reservations'][0]
        except IndexError:
            raise InstanceNotFound()
        instances = reservation['Instances']
        if len(instances) > 1:
            raise ValueError('Multiple results found.')
        return instances[0]['InstanceId']

    def is_asg_instance(self, id):
        r = self.asg.describe_auto_scaling_instances(
            InstanceIds=[id],
        )
        instances = r['AutoScalingInstances']
        if len(instances) == 0:
            return False
        else:
            return True

    def update_instance_health(self, id, status):
        self.asg.set_instance_health(
            InstanceId=id,
            HealthStatus='Healthy' if status == self.HEALTHY else 'Unhealthy',
        )


def main():
    consul = Consul()
    ec2 = boto3.client('ec2')
    asg = boto3.client('autoscaling')
    worker = Worker(consul, ec2, asg)
    worker.run()


if __name__ == '__main__':
    main()
