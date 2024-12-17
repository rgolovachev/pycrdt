import sys
import threading
from flask import Flask, request, jsonify, Response
from dataclasses import dataclass, asdict
import json
import requests
import copy
from marshmallow import fields, Schema, ValidationError
from marshmallow_dataclass import class_schema


@dataclass
class Operation:
    key: str
    value: str
    op_type: str
    src: int
    ts: dict


OperationSchema = class_schema(Operation)


hb_duration = 40
is_terminating = False
cur_id = -1
nodes = dict()
hb_event = threading.Event()
state_lock = threading.Lock()
state = {
    'log': [],
    'data': dict(),
    'data_ts': dict(),
    'cur_ts': dict(),
    'hb_timer': None
}

#
# misc
#

def reset_hb_timer():
    with state_lock:
        state['hb_timer'] = threading.Timer(hb_duration, hb_event.set)
        state['hb_timer'].start()

def inc_ts():
    with state_lock:
        state['cur_ts'][cur_id] += 1

#
# heartbeats sender
#
def hb_thread():
    reset_hb_timer()
    while not is_terminating:
        try:
            if hb_event.wait(timeout=0.5):
                hb_event.clear()

                data = None
                with state_lock:
                    data = json.dumps([asdict(oper) for oper in state['log']], ensure_ascii=False, indent=4)

                for node_id in nodes.keys():
                    if node_id != cur_id:
                        # send log
                        (host, port, _) = nodes[node_id]
                        addr = f"http://{host}:{port}/sync"
                        threading.Thread(target=send_log_routine, args=(addr,data,), daemon=True).start()

                reset_hb_timer()
        except:
            pass

def send_log_routine(addr, data):
    requests.put(addr, data=data, headers={'Content-Type':'application/json', 'Node': str(cur_id)}, timeout=5)

#
# CRDT
#

# need to call under lock
def is_newer(oper: Operation):
    if oper.key not in state['data_ts']:
        # print(oper.key + ' not in state["data_ts"]')
        return True

    # try:
    #     print('cur key ts: ' + str(state['data_ts'][oper.key]) + "; " + oper.key + ":" + state['data'][oper.key])
    #     print('oper key ts: ' + str(oper.ts) + "; " + oper.key + ":" + oper.value)
    # except Exception as e:
    #     print(e)
    #     return
    cur_is_newer = False
    oper_is_newer = False
    for node_id, t in oper.ts.items():
        if node_id not in state['data_ts'][oper.key] or state['data_ts'][oper.key][node_id] < t:
            oper_is_newer = True
        elif state['data_ts'][oper.key][node_id] > t:
            cur_is_newer = True

    cur_nodes = set(state['data_ts'][oper.key].keys())
    oper_nodes = set(oper.ts.keys())

    if len(cur_nodes - oper_nodes) > 0:
        cur_is_newer = True

    if cur_is_newer:
        if not oper_is_newer:
            return False
        # conflict if we are here
    else:
        if not oper_is_newer:
            return False
        return True

    return oper.value > state['data'][oper.key]


# need to call under lock
def apply(oper: Operation):
    verdict = is_newer(oper)
    # print('verdict: ' + str(verdict))
    if verdict:
        if oper.op_type == 'set':
            state['data'][oper.key] = oper.value
        elif oper.op_type == 'del' and oper.key in state['data']:
            del state['data'][oper.key]

        if oper.key not in state['data_ts']:
            state['data_ts'][oper.key] = dict()

        for node_id, t in oper.ts.items():
            state['data_ts'][oper.key][node_id] = t

        state['log'].append(oper)


# need to call under lock
def match_clocks(oper_ts):
    for node_id, t in oper_ts.items():
        if node_id not in state['cur_ts'] or state['cur_ts'][node_id] < t:
            state['cur_ts'][node_id] = t


app = Flask(__name__)

#
# HTTP server handler
#
@app.route('/change', methods=['PATCH'])
def change_values():
    try:
        updates = request.get_json()

        if not isinstance(updates, dict):
            return jsonify({'error': 'invalid body format'}), 400

        print("UPDATES: " + str(updates), file=sys.stderr)
        for k, v in updates.items():
            inc_ts()

            op_type = None
            if v != "":
                op_type = 'set'
            else:
                op_type = 'del'

            with state_lock:
                oper = Operation(key=k, value=v, op_type=op_type, src=cur_id, ts=copy.deepcopy(state['cur_ts']))
                apply(oper)

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        print(e)
        return jsonify({'error': 'caught exception'}), 500


@app.route('/values', methods=['GET'])
def get_values():
    with state_lock:
        return jsonify(state['data']), 200

@app.route('/state_dump', methods=['GET'])
def get_state_dump():
    with state_lock:
        result = dict()
        result['data'] = state['data']
        result['data_ts'] = state['data_ts']
        result['cur_ts'] = state['cur_ts']
        return jsonify(result), 200


@app.route('/sync', methods=['PUT'])
def sync_clocks():
    try:
        json_data = request.get_json()

        oper_schema = OperationSchema(many=True)
        opers = oper_schema.load(json_data)

        # print('sender node is ' + request.headers['Node'])
        for oper in opers:
            oper.ts = {int(k): v for k, v in oper.ts.items()}
            # print('oper:', oper)
            # print('cur_ts: ', state['cur_ts'])
            with state_lock:
                apply(oper)
                match_clocks(oper.ts)

        return jsonify({'status': 'success'}), 200
    except:
        return jsonify({'error': 'caught exception'}), 500


def main(id):
    global app
    global cur_id
    global is_terminating

    print(nodes)

    cur_id = id
    (host, port, hb_t) = nodes[cur_id]

    global hb_duration
    hb_duration = hb_t

    state['cur_ts'] = dict()
    state['cur_ts'][cur_id] = 0

    hb_thr = threading.Thread(target=hb_thread)
    hb_thr.start()

    try:
        app.run(host, port)
    except KeyboardInterrupt:
        is_terminating = True
        hb_thr.join()


if __name__ == '__main__':
    [id, cfg_path] = sys.argv[1:]
    with open(cfg_path, 'r') as f:
        line_parts = map(lambda line: line.split(), f.read().strip().split("\n"))
        nodes = dict([(int(p[0]), (p[1], int(p[2]), float(p[3]))) for p in line_parts])
        print(list(nodes))
    main(int(id))
