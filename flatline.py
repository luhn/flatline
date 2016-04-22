import logging
from time import sleep
from urllib.parse import urljoin
from threading import Thread

import requests


logger = logging.getLogger('flatline')
logging.basicConfig(level=logging.DEBUG)


class Consul(object):
    def __init__(self, url='http://localhost:8500/'):
        self.url = url

    def call(self, method, path, params={}, data={}, retry=False):
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
                    logger.error('Worker died!')
                    raise WorkerDied()
                sleep(5)
        except (WorkerDied, LockLost):
            pass
        finally:
            if thread.isAlive():
                logger.info('Quitting worker...')
                thread.cancel()


class Worker(Thread):
    cancelled = False

    HEALTHY = 0
    UNHEALTHY = 1

    def __init__(self, consul):
        super(self, Thread).__init__()
        self.consul = consul
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
            ip = self.get_node_ip(node)
            id = self.get_instance_id(ip)
            self.update_node_health(id, status)

    def update_health_checks(self):
        pass

    def query_health_checks(self):
        pass

    def get_node_ip(self, node_name):
        pass

    def get_instance_id(self, ip):
        pass

    def update_node_health(self, id, status):
        pass


if __name__ == '__main__':
    consul = Consul()
    worker_factory = lambda: Worker(consul)
    run_with_lock(consul, 'flatline', worker_factory)
