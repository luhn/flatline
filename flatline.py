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
                logger.debug('Consul response:  HTTP %s', r.status)
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


class Session(object):
    id = None

    def __init__(self, consul, name):
        logger.info('Acquiring session...')
        r = consul.put('v1/session/create', {
            'Name': name,
        })
        self.id = r['ID']
        logger.info('Session acquired.  ID=%s', self.id)


class WorkerDied(Exception):
    pass


class LockLost(Exception):
    pass


def acquire_lock(consul, name, session, retry_delay=5):
    logger.info('Waiting to acquire lock...')
    while True:
        logger.debug('Attempting to acquire...')
        r = consul.put('v1/kv/{}'.format(name), params={
            'acquire': session.id,
        })
        if r:
            logger.info('Lock acquired.')
            return
        logger.info('Failed to acquire.')
        sleep(retry_delay)


def release_lock(consul, name, session):
    logger.info('Releasing lock...')
    consul.put('v1/kv/{}'.format(name), params={
        'release': session.id,
    })


def check_lock(consul, name, session):
    logger.debug('Checking lock...')
    r = consul.get('v1/kv/{}'.format(name))
    if len(r) == 0:
        raise LockLost()
    key = r[0]
    if key['Session'] != session.id:
        raise LockLost()


def run_with_lock(consul, name, worker_factory):
    session = Session(consul, name)
    while True:
        acquire_lock(consul, name, session)
        thread = worker_factory(consul)
        thread.start()
        try:
            while True:
                check_lock(consul, name, session)
                if not thread.isAlive():
                    raise WorkerDied()
                sleep(5)
        except WorkerDied:
            logger.error('Worker died!')
            release_lock(consul, name, session)
        except LockLost:
            logger.error('Lock lost.')
        finally:
            if thread.isAlive():
                logger.info('Quitting worker...')
                thread.cancel()


class Worker(Thread):
    cancelled = False
    last_index = None

    HEALTHY = 0
    UNHEALTHY = 1

    def __init__(self, consul, ec2, asg):
        super(Thread, self).__init__()
        self.consul = consul
        self.ec2 = ec2
        self.asg = asg
        self.node_status = {}

    def run(self):
        logger.info('Starting worker...')
        while not self.cancelled:
            self._body()

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
            id = self.get_instance_id(ip)
            self.update_instance_health(id, status)

    def update_health_checks(self):
        checks = sorted(self.query_health_checks(), key=lambda x: x[0])
        for k, g in itertools.groupby(checks, lambda x: x[0]):
            is_healthy = all(entry[1] == self.HEALTHY for entry in g)
            status = (
                self.HEALTHY if is_healthy else self.UNHEALTHY
            )
            old_status = self.node_status.get(k)
            if old_status != status:
                self.node_status[k] = status
                yield k, status

    def query_health_checks(self):
        logging.info('Querying Consul for health checks.')
        params = {
            'wait': '10s',
        }
        if self.last_index is not None:
            params['index'] = self.last_index
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
            yield entry['Node'], health

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
            raise ValueError('No results found.')
        instances = reservation['Instances']
        if len(instances) > 1:
            raise ValueError('Multiple results found.')
        return instances[0]['InstanceId']

    def update_instance_health(self, id, status):
        self.asg.set_instance_health(
            InstanceId=id,
            HealthStatus='Healthy' if status == self.HEALTHY else 'Unhealthy',
        )


def main():
    consul = Consul()
    ec2 = boto3.client('ec2')
    asg = boto3.client('autoscaling')
    worker_factory = lambda: Worker(consul, ec2, asg)
    run_with_lock(consul, 'flatline', worker_factory)


if __name__ == '__main__':
    main()
