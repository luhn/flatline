from time import sleep
from urllib.parse import urljoin
from threading import Thread

import requests


class Consul(object):
    def __init__(self, url='http://localhost:8500/'):
        self.url = url

    def call(self, method, path, params={}, data={}, retry=False):
        url = urljoin(self.url, path)
        while True:
            try:
                r = requests.request(
                    method,
                    url,
                    params,
                    json=data,
                    timeout=70,
                )
                r.raise_for_status()
                return r.json()
            except requests.RequestException:
                if not retry:
                    break
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

    def __init__(self, consul):
        pass


def acquire_lock(consul, name, session):
    pass


def monitor_lock(consul, name, session):
    pass


def run_with_lock(consul, name, worker_factory):
    session = Session(consul)
    while True:
        acquire_lock(consul, 'flatline', session)
        thread = worker_factory(consul)
        thread.start()
        try:
            monitor_lock(consul, 'flatline', session)
        finally:
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
