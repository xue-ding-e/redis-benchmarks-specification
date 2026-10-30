from unittest.mock import Mock

import pytest
import redis

from redis_benchmarks_specification.__self_contained_coordinator__ import docker


OK = b"cluster_state:ok\r\ncluster_slots_assigned:16384\r\n"
FAIL = "cluster_state:fail\r\n"


class FakeClock:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class Node:
    def __init__(self, responses):
        self.responses = responses
        self.info_calls = 0
        self.ping = Mock(return_value=True)

    def execute_command(self, *args):
        if args == ("CLUSTER", "INFO"):
            response = self.responses[min(self.info_calls, len(self.responses) - 1)]
            self.info_calls += 1
            if isinstance(response, Exception):
                raise response
            return response
        return "OK"


@pytest.fixture
def cluster(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(docker, "time", clock)
    monkeypatch.setattr(docker, "server_knows_config", Mock(return_value=False))
    monkeypatch.setattr(docker, "start_redis_container", Mock())

    def prepare(states):
        nodes = [Node(state) for state in states]
        monkeypatch.setattr(docker.redis, "StrictRedis", Mock(side_effect=nodes))

        def start():
            return docker.spin_docker_cluster_redis(
                3, 3, 0, Mock(), {}, [], 7380, "unused", "/synthetic-only"
            )

        return nodes, clock, start

    return prepare


def test_all_nodes_ready_immediately_with_bytes_and_text(cluster):
    nodes, clock, start = cluster([[OK], [OK.decode()], [OK]])
    conns, cpu = start()
    assert conns == nodes
    assert cpu == 3
    assert [node.info_calls for node in nodes] == [1, 1, 1]
    assert clock.sleeps == []


@pytest.mark.parametrize("lagging_node", [1, 2])
def test_waits_for_nonfirst_node(cluster, lagging_node):
    states = [[OK], [OK], [OK]]
    states[lagging_node] = [FAIL, FAIL, OK]
    nodes, clock, start = cluster(states)
    start()
    assert [node.info_calls for node in nodes] == [3, 3, 3]
    assert clock.sleeps == [0.5, 0.5]


def test_requires_same_poll_readiness_not_sticky_success(cluster):
    nodes, clock, start = cluster([[OK, FAIL, OK], [FAIL, OK, OK], [OK]])
    start()
    assert [node.info_calls for node in nodes] == [3, 3, 3]
    assert clock.sleeps == [0.5, 0.5]


@pytest.mark.parametrize("failing_node", [0, 1, 2])
def test_any_node_never_ready_times_out(cluster, failing_node):
    states = [[OK], [OK], [OK]]
    states[failing_node] = [FAIL]
    nodes, clock, start = cluster(states)
    with pytest.raises(RuntimeError, match="within 60s"):
        start()
    assert clock.now == 60
    assert [node.info_calls for node in nodes] == [120, 120, 120]


def test_nonfirst_node_command_failure_propagates(cluster):
    error = redis.exceptions.ConnectionError("synthetic node 2 unavailable")
    nodes, clock, start = cluster([[OK], [error], [OK]])
    with pytest.raises(redis.exceptions.ConnectionError) as raised:
        start()
    assert raised.value is error
    assert clock.sleeps == []
