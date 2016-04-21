import requests


class Consul(object):
    def __init__(self, host='localhost', port=8500):
        pass

    def call(self, method, path, data={}, body={}, retry=False):
        pass

    def get(self, path, data={}, **kwargs):
        pass

    def post(self, path, body={}, **kwargs):
        pass

    def put(self, path, body={}, **kwargs):
        pass

    def delete(self, path, body={}, **kwargs):
        pass


class Session(object):
    id = None

    def __init__(self, consul):
        pass


def acquire_lock(consul, name, session):
    pass


def monitor_lock(consul, name, session):
    pass


def run_with_lock(consul, name, worker_factory):
    self.session = Session(consul)
    while True:
        self.acquire()
        thread = worker_factory(consul)
        thread.start()
        try:
            self.monitor()
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

    def body():
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
