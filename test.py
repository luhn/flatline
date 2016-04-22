import pytest
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
