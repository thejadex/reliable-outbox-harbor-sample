"""Independent behavioral verifier. Candidate code runs only as an unprivileged child."""
import concurrent.futures
import json
import os
from pathlib import Path
import random
import select
import subprocess
import tempfile
import unittest

BRIDGE = Path('/tests/bridge.py').read_text()


class Client:
    def __init__(self, path):
        self.proc = subprocess.Popen(['runuser', '-u', 'agent', '--', 'python3', '-I', '-c', BRIDGE], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1, start_new_session=True)
        self.call('open', str(path))

    def call(self, method, *args, **kwargs):
        self.proc.stdin.write(json.dumps(dict(method=method,args=args,kwargs=kwargs))+'\n')
        self.proc.stdin.flush()
        if not select.select([self.proc.stdout], [], [], 30)[0]:
            raise RuntimeError('candidate response timeout')
        line = self.proc.stdout.readline()
        response = json.loads(line)
        if not response.get('ok'):
            if response.get('error') == 'ValueError':
                raise ValueError(response.get('message'))
            raise RuntimeError(str(response))
        return response['result']

    def close(self):
        import signal
        if self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGKILL)
        self.proc.wait()
        self.proc.stdin.close()
        self.proc.stdout.close()


class Contract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='outbox-check-')
        os.chmod(self.temp.name, 0o777)
        self.path = Path(self.temp.name)/'queue.db'
        self.clients = []
        self.s = self.client()

    def client(self):
        client = Client(self.path)
        self.clients.append(client)
        return client

    def tearDown(self):
        for client in self.clients:
            client.close()
        self.temp.cleanup()

    def seed(self, endpoints=('a',), tenant='t', event='one', now=0, payload=None):
        for endpoint in endpoints:
            self.s.call('register',tenant,endpoint)
        return self.s.call('submit',tenant,event,{'v': 1} if payload is None else payload,now=now)

    def claim(self, now=0, lease=10, limit=100, client=None):
        return (client or self.s).call('claim','worker',now=now,lease_seconds=lease,limit=limit)

    def finish(self, item, method='ack', now=0, **kwargs):
        return self.s.call(method,item['tenant'],item['event_id'],item['endpoint'],item['token'],now=now,**kwargs)

    def test_initial_empty_state(self):
        self.assertEqual(self.s.call('snapshot','absent'),[])
        self.assertEqual(self.claim(),[])

    def test_subscription_snapshot_and_empty_event(self):
        self.assertEqual(self.s.call('submit','t','empty',{},now=0),0)
        self.s.call('register','t','a')
        self.assertEqual(self.s.call('submit','t','empty',{},now=0),0)
        self.s.call('register','t','b',False)
        self.assertEqual(self.s.call('submit','t','one',{},now=0),1)
        self.s.call('register','t','a',False)
        self.s.call('register','t','b',True)
        self.assertEqual(self.s.call('submit','t','one',{},now=0),1)
        self.assertEqual(self.s.call('submit','t','two',{},now=0),1)
        rows=self.s.call('snapshot','t')
        self.assertEqual([(r['event_id'],r['endpoint']) for r in rows],[('one','a'),('two','b')])

    def test_canonical_identity_and_conflict(self):
        self.seed(payload={'z':[1,{'β':'λ'}],'a':True})
        self.assertEqual(self.s.call('submit','t','one',{'a':True,'z':[1,{'β':'λ'}]},now=0),1)
        before=self.s.call('snapshot','t')
        with self.assertRaises(ValueError):
            self.s.call('submit','t','one',{'a':True,'z':[1.0,{'β':'λ'}]},now=0)
        self.assertEqual(self.s.call('snapshot','t'),before)

    def test_snapshot_and_claim_schema(self):
        self.seed(('b','a'),now=2.5,payload={'list':[1,None,False]})
        rows=self.s.call('snapshot','t')
        self.assertEqual([r['endpoint'] for r in rows],['a','b'])
        self.assertEqual(set(rows[0]),{'event_id','endpoint','payload','state','attempts','available_at','lease_until'})
        self.assertEqual(rows[0],dict(event_id='one',endpoint='a',payload={'list':[1,None,False]},state='pending',attempts=0,available_at=2.5,lease_until=None))
        item=self.claim(now=3,lease=1.5,limit=1)[0]
        self.assertEqual(set(item),{'tenant','event_id','endpoint','payload','attempt','token','lease_until'})
        self.assertEqual((item['attempt'],item['lease_until']),(1,4.5))
        self.assertIsInstance(item['token'],str)
        self.assertGreaterEqual(len(item['token']),22)

    def test_tenant_isolation(self):
        self.seed(tenant='left')
        self.seed(tenant='right',payload={'v':2})
        claims=self.claim()
        self.assertEqual(len(claims),2)
        left=next(c for c in claims if c['tenant']=='left')
        with self.assertRaises(ValueError):
            self.s.call('ack','right','one','a',left['token'],now=0)
        self.finish(left)
        self.assertEqual(self.s.call('snapshot','left')[0]['state'],'delivered')
        self.assertEqual(self.s.call('snapshot','right')[0]['state'],'inflight')

    def test_commit_order_not_event_id_or_tenant(self):
        self.seed(('b','a'),tenant='z',event='z')
        self.seed(('b','a'),tenant='a',event='a')
        claims=self.claim(limit=3)
        self.assertEqual([(c['tenant'],c['endpoint']) for c in claims],[('z','a'),('z','b'),('a','a')])

    def test_fifo_heads_and_independent_partitions(self):
        self.seed(('a','b'))
        self.s.call('submit','t','two',{},now=0)
        first=self.claim(limit=1)[0]
        self.assertEqual(first['endpoint'],'a')
        second=self.claim(limit=99)
        self.assertEqual([(c['event_id'],c['endpoint']) for c in second],[('one','b')])
        self.finish(first)
        next_claim=self.claim()
        self.assertEqual([(c['event_id'],c['endpoint']) for c in next_claim],[('two','a')])

    def test_due_time_precedes_insertion_order(self):
        self.seed(('a',),event='first')
        first=self.claim(lease=50)[0]
        self.finish(first,'fail',now=1)
        self.seed(('b',),tenant='other',event='later',now=2)
        claims=self.claim(now=3)
        self.assertEqual([c['event_id'] for c in claims],['later','first'])

    def test_retry_blocks_partition_until_exact_boundary(self):
        self.seed()
        self.s.call('submit','t','two',{},now=0)
        first=self.claim(lease=20)[0]
        self.finish(first,'fail',now=1)
        self.assertEqual(self.claim(now=2.999),[])
        retry=self.claim(now=3)[0]
        self.assertEqual((retry['event_id'],retry['attempt']),('one',2))
        self.assertNotEqual(retry['token'],first['token'])

    def test_ack_boundary_and_stale_token_no_mutation(self):
        self.seed()
        old=self.claim(lease=2)[0]
        before=self.s.call('snapshot','t')
        with self.assertRaises(ValueError): self.finish(old,now=2)
        self.assertEqual(self.s.call('snapshot','t'),before)
        current=self.claim(now=2)[0]
        with self.assertRaises(ValueError): self.finish(old,now=2)
        with self.assertRaises(ValueError): self.finish(old,'fail',now=2)
        self.finish(current,now=2)
        self.assertEqual(self.s.call('snapshot','t')[0]['state'],'delivered')

    def test_retry_exhaustion_and_dead_release(self):
        self.seed()
        self.s.call('submit','t','two',{},now=0)
        at=0
        for attempt in range(1,6):
            item=self.claim(now=at,lease=100)[0]
            self.assertEqual(item['attempt'],attempt)
            self.finish(item,'fail',now=at)
            row=self.s.call('snapshot','t')[0]
            self.assertEqual(row['lease_until'],None)
            if attempt<5:
                at+=min(2**attempt,32)
                self.assertEqual(row['available_at'],at)
            else:
                self.assertEqual(row['state'],'dead')
        self.assertEqual(self.claim(now=at)[0]['event_id'],'two')

    def test_permanent_failure_preserves_available_at(self):
        self.seed(now=4)
        item=self.claim(now=5)[0]
        self.finish(item,'fail',now=6,retryable=False)
        row=self.s.call('snapshot','t')[0]
        self.assertEqual((row['state'],row['available_at'],row['lease_until']),('dead',4,None))

    def test_five_crashes_eventually_release_partition(self):
        self.seed()
        self.s.call('submit','t','two',{},now=0)
        tokens=set()
        for attempt in range(1,6):
            item=self.claim(now=attempt-1,lease=1)[0]
            self.assertEqual((item['event_id'],item['attempt']),('one',attempt))
            tokens.add(item['token'])
        self.assertEqual(len(tokens),5)
        item=self.claim(now=5)[0]
        self.assertEqual(item['event_id'],'two')
        self.assertEqual(self.s.call('snapshot','t')[0]['state'],'dead')

    def test_expiry_recovery_on_empty_claim(self):
        self.seed()
        for at in range(5):
            self.claim(now=at,lease=1)
        self.assertEqual(self.claim(now=5),[])
        row=self.s.call('snapshot','t')[0]
        self.assertEqual((row['state'],row['attempts'],row['lease_until']),('dead',5,None))

    def test_reopen_and_killed_worker_persistence(self):
        self.seed()
        item=self.claim(lease=2)[0]
        self.s.close()
        self.s=self.client()
        self.assertEqual(self.s.call('snapshot','t')[0]['state'],'inflight')
        recovered=self.claim(now=2)[0]
        self.assertNotEqual(recovered['token'],item['token'])
        self.finish(recovered,now=2)
        another=self.client()
        self.assertEqual(another.call('snapshot','t')[0]['state'],'delivered')

    def test_invalid_payloads_are_atomic(self):
        self.s.call('register','t','a')
        for kind in range(6):
            with self.assertRaises(ValueError): self.s.call('invalid_payload',kind)
        for payload in ({'x':float('nan')},{'x':float('inf')},{'nested':[{'x':float('-inf')}]}):
            with self.assertRaises(ValueError): self.s.call('submit','t','bad',payload,now=0)
        self.assertEqual(self.s.call('snapshot','t'),[])
        self.assertEqual(self.s.call('submit','t','bad',{},now=0),1)

    def test_bad_arguments_leave_state_unchanged(self):
        self.seed()
        original=self.s.call('snapshot','t')
        calls=[('register',('', 'a'),{}),('register',('t',''),{}),('register',('t','b',1),{}),('submit',('t','',{}),{'now':0}),('snapshot',('',),{}),('claim',('',),{'now':0,'lease_seconds':1,'limit':1})]
        for bad in (-1,True,None,float('nan'),float('inf')):
            calls.append(('submit',('t','bad',{}),{'now':bad}))
        for bad in (0,-1,True,None,float('nan'),float('inf')):
            calls.append(('claim',('w',),{'now':0,'lease_seconds':bad,'limit':1}))
        for bad in (0,-1,True,1.5,None):
            calls.append(('claim',('w',),{'now':0,'lease_seconds':1,'limit':bad}))
        for method,args,kwargs in calls:
            with self.subTest(method=method,args=args,kwargs=kwargs):
                with self.assertRaises(ValueError): self.s.call(method,*args,**kwargs)
                self.assertEqual(self.s.call('snapshot','t'),original)

    def test_invalid_completion_ownership_and_types(self):
        self.seed()
        item=self.claim()[0]
        before=self.s.call('snapshot','t')
        cases=[('ack',('t','missing','a',item['token']),{'now':0}),('ack',('t','one','missing',item['token']),{'now':0}),('ack',('t','one','a','wrong'),{'now':0}),('ack',('t','one','a',''),{'now':0}),('ack',('t','one','a',item['token']),{'now':True}),('fail',('t','one','a',item['token']),{'now':0,'retryable':1})]
        for method,args,kwargs in cases:
            with self.assertRaises(ValueError): self.s.call(method,*args,**kwargs)
            self.assertEqual(self.s.call('snapshot','t'),before)
        self.finish(item)
        with self.assertRaises(ValueError): self.finish(item)
        with self.assertRaises(ValueError): self.finish(item,'fail')

    def test_concurrent_initialization_and_duplicate_submission(self):
        fresh=Path(self.temp.name)/'concurrent-first-open.db'
        def action(index):
            client=Client(fresh)
            self.clients.append(client)
            client.call('register','t','a')
            client.call('register','t','b')
            return client.call('submit','t','race',{'same':True},now=0)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(list(pool.map(action,range(16))),[2]*16)
        self.assertEqual(len(self.clients[-1].call('snapshot','t')),2)

    def test_concurrent_conflicting_submission(self):
        self.s.call('register','t','a')
        clients=[self.client() for _ in range(8)]
        def action(pair):
            index,client=pair
            try:
                return client.call('submit','t','race',{'winner':index},now=0)
            except ValueError:
                return 'conflict'
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results=list(pool.map(action,enumerate(clients)))
        self.assertEqual(results.count(1),1)
        self.assertEqual(results.count('conflict'),7)
        self.assertEqual(len(self.s.call('snapshot','t')),1)

    def test_concurrent_claims_do_not_overlap(self):
        self.seed(tuple('endpoint-%02d'%i for i in range(40)))
        clients=[self.client() for _ in range(8)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            batches=list(pool.map(lambda c:self.claim(limit=7,client=c),clients))
        claims=[c for batch in batches for c in batch]
        self.assertEqual(len(claims),40)
        self.assertEqual(len({c['endpoint'] for c in claims}),40)
        self.assertEqual(len({c['token'] for c in claims}),40)
        self.assertEqual(self.claim(),[])

    def test_seeded_delivery_replay(self):
        rng=random.Random(713)
        expected=[]
        for tenant in ('b','a','c'):
            for endpoint in ('z','x','y'):
                self.s.call('register',tenant,endpoint)
        for seq in range(90):
            tenant=rng.choice(('b','a','c'))
            event='event-%03d'%seq
            payload={'sequence':seq,'nested':[None,False,'δ']}
            self.s.call('submit',tenant,event,payload,now=0)
            expected.extend((seq,tenant,event,e,payload) for e in ('x','y','z'))
        pending=list(expected)
        while pending:
            heads={}
            for row in pending:
                heads.setdefault((row[1],row[3]),row)
            want=sorted(heads.values(),key=lambda r:(r[0],r[3]))[:rng.randint(1,12)]
            got=self.claim(limit=len(want),lease=100)
            self.assertEqual([(g['tenant'],g['event_id'],g['endpoint'],g['payload']) for g in got],[(r[1],r[2],r[3],r[4]) for r in want])
            for item,row in zip(got,want):
                self.finish(item)
                pending.remove(row)
        self.assertEqual(self.claim(),[])
        for tenant in ('b','a','c'):
            self.assertTrue(all(r['state']=='delivered' for r in self.s.call('snapshot',tenant)))


if __name__ == '__main__':
    unittest.main(verbosity=2)
