#!/usr/bin/env python3
"""Exercise the running demo API and await real RocketMQ/MySQL convergence."""
import http.cookiejar
import json
import os
import time
import urllib.error
import urllib.request

base = os.environ.get('BASE_URL', 'http://127.0.0.1:8080')
client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))


def request(path, method='GET', data=None):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(base + '/api/v1' + path, body, {'Content-Type': 'application/json'}, method=method)
    with client.open(req, timeout=15) as response:
        return json.load(response)


request('/session', 'POST', {'user_id': '1003'})
path = '/targets/1/2003/like'
original = request(path)
try:
    liked = request(path, 'PUT', {'action': 'LIKE'})
    repeated = request(path, 'PUT', {'action': 'LIKE'})
    assert repeated['liked'] and not repeated['changed']
    assert repeated['version'] == liked['version'] and repeated['count'] == liked['count']
    unliked = request(path, 'PUT', {'action': 'UNLIKE'})
    assert not unliked['liked'] and unliked['changed']
    assert int(unliked['count']) == int(liked['count']) - 1
    repeated = request(path, 'PUT', {'action': 'UNLIKE'})
    assert not repeated['changed'] and repeated['version'] == unliked['version']
    liked = request(path, 'PUT', {'action': 'LIKE'})
    assert liked['changed'] and int(liked['version']) == int(unliked['version']) + 1
    request('/session', 'POST', {'user_id': '1002'})
    other = request(path)
    assert other['count'] == liked['count']
    request('/session', 'POST', {'user_id': '1003'})
    try:
        request(path, 'PUT', {'action': 'TOGGLE'})
        raise AssertionError('invalid action accepted')
    except urllib.error.HTTPError as error:
        assert error.code == 400
finally:
    final = request(path, 'PUT', {'action': 'LIKE' if original['liked'] else 'UNLIKE'})

deadline = time.monotonic() + 45
while True:
    persisted = request('/targets/1/2003/sync')
    if all(persisted[k] == final[k] for k in ('liked', 'count', 'version')):
        break
    assert time.monotonic() < deadline, f'MySQL did not converge: {persisted} != {final}'
    time.sleep(1)
print('PASS: authenticated LIKE/UNLIKE, state idempotency, user switching, validation, RocketMQ → MySQL convergence')
print(json.dumps(persisted))
