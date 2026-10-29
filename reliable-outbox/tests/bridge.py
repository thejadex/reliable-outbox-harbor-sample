"""Runs as the agent user. The trusted test driver communicates over JSON lines."""
import importlib
import json
import sys

sys.path.insert(0, '/app')
store = None
for line in sys.stdin:
    try:
        request = json.loads(line)
        if request['method'] == 'open':
            store = importlib.import_module('outbox').Store(request['args'][0])
            result = None
        elif request['method'] == 'invalid_payload':
            variants = [[], {1: 'bad'}, {'x': (1, 2)}, {'x': b'bad'}, {'x': object()}]
            cycle = {}; cycle['self'] = cycle
            variants.append(cycle)
            result = store.submit('t', 'bad', variants[request['args'][0]], now=0)
        else:
            result = getattr(store, request['method'])(*request.get('args', []), **request.get('kwargs', {}))
        response = {'ok': True, 'result': result}
    except Exception as exc:
        response = {'ok': False, 'error': type(exc).__name__, 'message': str(exc)}
    print(json.dumps(response, allow_nan=True), flush=True)
