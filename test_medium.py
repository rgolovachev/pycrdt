import subprocess
import time
import requests

CONFIG_PATH = 'test_medium.conf'

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
        if resp_json['cur_ts'] != cur_ts:
            print("vector clocks mismatch")
            print('current: ' + str(resp_json['cur_ts']))
            print('expected: ' + str(cur_ts))
            finish_test(False)


    try:
        (host0, port0, _) = nodes_cfg[0]
        addr0 = f"http://{host0}:{port0}"
        (host1, port1, _) = nodes_cfg[1]
        addr1 = f"http://{host1}:{port1}"
        (host2, port2, _) = nodes_cfg[2]
        addr2 = f"http://{host2}:{port2}"

        requests.put(f"{addr1}/blacklist", headers={'Nodes': '0,2'})

        requests.patch(f"{addr0}/change", json={'k': 'v0'}, headers={'Content-Type':'application/json'})
        requests.patch(f"{addr1}/change", json={'k': 'v1'}, headers={'Content-Type':'application/json'})

        time.sleep(1)

        check_node_state(0, {'k': 'v0'}, {'0': 1})
        check_node_state(2, {'k': 'v0'}, {'0': 1})

        check_node_state(1, {'k': 'v1'}, {'1': 1})

        requests.patch(f"{addr2}/change", json={'k': 'v2'}, headers={'Content-Type':'application/json'})

        time.sleep(1)

        check_node_state(0, {'k': 'v2'}, {'0': 1, '2': 1})
        check_node_state(2, {'k': 'v2'}, {'0': 1, '2': 1})

        check_node_state(1, {'k': 'v1'}, {'1': 1})

        requests.put(f"{addr1}/blacklist", headers={'Nodes': '52'})

        time.sleep(1)

        check_node_state(0, {'k': 'v2'}, {'0': 1, '1': 1, '2': 1})
        check_node_state(1, {'k': 'v2'}, {'0': 1, '1': 1, '2': 1})
        check_node_state(2, {'k': 'v2'}, {'0': 1, '1': 1, '2': 1})

        print('medium test passed')
    except Exception as e:
        print(f'exception while executing medium test: {e}')
        finish_test(False)

    finish_test(True)

if __name__ == '__main__':
    main()
