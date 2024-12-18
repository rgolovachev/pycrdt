import subprocess
import requests
import random
import time

CONFIG_PATH = "test_simple.conf"

def start_process(id):
    return subprocess.Popen(['python3', 'crdt.py', str(id), CONFIG_PATH], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    nodes = [start_process(0), start_process(1), start_process(2)]

    time.sleep(1.5)

    nodes_cfg = None
    with open(CONFIG_PATH, 'r') as f:
        line_parts = map(lambda line: line.split(), f.read().strip().split("\n"))
        nodes_cfg = dict([(int(p[0]), (p[1], int(p[2]), float(p[3]))) for p in line_parts])

    def finish_test(is_succ):
        for node in nodes:
            node.terminate()
        if not is_succ:
            exit(1)

    def check_node_state(id, cur_kv, cur_ts):
        (host, port, _) = nodes_cfg[id]
        addr = f"http://{host}:{port}/state_dump"
        resp = requests.get(addr)
        resp_json = resp.json()

        if resp_json['data'] != cur_kv:
            print("KV state mismatch")
            print('current: ' + str(resp_json['data']))
            print('expected: ' + str(cur_kv))
            finish_test(False)
        for node_id, t in cur_ts.items():
            if node_id not in resp_json['cur_ts'] or resp_json['cur_ts'][node_id] != t:
                print("vector clocks mismatch")
                print('current: ' + str(resp_json['cur_ts']))
                print('expected: ' + str(cur_ts))
                finish_test(False)
    
    def print_node_state(id):
        (host, port, _) = nodes_cfg[id]
        addr = f"http://{host}:{port}/state_dump"
        print(requests.get(addr).json())

    try:
        #
        # basic functional
        #
        random_id = random.randint(0, len(nodes) - 1)
        (host, port, _) = nodes_cfg[random_id]
        addr = f"http://{host}:{port}/change"
        cur_kv = {'k': 'v'}
        cur_ts = {str(random_id): 1}

        requests.patch(addr, json=cur_kv, headers={'Content-Type':'application/json'})
        time.sleep(1)

        for node_id in range(3):
            check_node_state(node_id, cur_kv, cur_ts)

        random_id = random.randint(0, len(nodes) - 1)
        (host, port, _) = nodes_cfg[random_id]
        addr = f"http://{host}:{port}/change"
        cur_kv['k2'] = 'v2'
        if str(random_id) in cur_ts:
            cur_ts[str(random_id)] += len(cur_kv)
        else:
            cur_ts[str(random_id)] = len(cur_kv)

        requests.patch(addr, json=cur_kv, headers={'Content-Type':'application/json'})
        time.sleep(1)

        for node_id in range(3):
            check_node_state(node_id, cur_kv, cur_ts)

        random_id = random.randint(0, len(nodes) - 1)
        (host, port, _) = nodes_cfg[random_id]
        addr = f"http://{host}:{port}/change"
        cur_kv['k2'] = ''
        cur_kv['k'] = 'v2'
        if str(random_id) in cur_ts:
            cur_ts[str(random_id)] += len(cur_kv)
        else:
            cur_ts[str(random_id)] = len(cur_kv)

        requests.patch(addr, json=cur_kv, headers={'Content-Type':'application/json'})
        time.sleep(1)

        del cur_kv['k2']
        for node_id in range(3):
            check_node_state(node_id, cur_kv, cur_ts)

        #
        # conflicts resolving
        #
        (host0, port0, _) = nodes_cfg[0]
        addr0 = f"http://{host0}:{port0}/change"
        (host1, port1, _) = nodes_cfg[1]
        addr1 = f"http://{host1}:{port1}/change"
        cur_ts['0'] = cur_ts.get('0', 0) + 1
        cur_ts['1'] = cur_ts.get('1', 0) + 1

        cur_kv2 = dict()
        cur_kv2['k3'] = 'v3'
        requests.patch(addr0, json=cur_kv2, headers={'Content-Type':'application/json'})

        cur_kv2['k3'] = 'v4'
        requests.patch(addr1, json=cur_kv2, headers={'Content-Type':'application/json'})

        time.sleep(1.5)

        cur_kv['k3'] = 'v4'
        for node_id in range(3):
            check_node_state(node_id, cur_kv, cur_ts)

        print('simple test passed')
    except Exception as e:
        print(f'exception while executing simple test: {e}')
        finish_test(False)

    finish_test(True)

if __name__ == '__main__':
    main()
