import copy
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from logplane import cli as logplane_cli
from logplane.observer import ObserverClient
from logplane.operator_controls import intervene, read_snapshot
from logplane.operations import discovery_view
from logplane.pm_agent import plan_next_emission
from logplane.projection import replay_events
from tests.test_pm_agent import registered, created, assigned, event


class Log:
    iter_events = ObserverClient.iter_events

    def __init__(self, events):
        self.events = copy.deepcopy(events)
        self.posts = 0
        self.before_publish = None
        self.timestamp = 101.0
        self.pages = []

    def health(self):
        return {'ok':True, 'last_event_id':self.events[-1]['id'] if self.events else 0}

    def query(self, *, after_id=0, limit=1000, topics=None):
        self.pages.append((after_id, limit))
        return [e for e in self.events if e['id'] > after_id and (not topics or e['topic'] in topics)][:limit]

    def publish(self, intent):
        self.posts += 1
        if self.before_publish:
            self.before_publish()
        result = dict(intent, id=len(self.events)+1, ts=self.timestamp)
        self.events.append(result)
        return result

    def close(self):
        pass


def active_log():
    return Log([registered(), created(correlation_id='flow'), assigned(3, 1)])


def blocked_log():
    log = active_log()
    log.events += [event(4,'task.blocked','alice',{'task_id':1,'assignment_id':'task:1:attempt:1',
        'worker_instance_id':'alice-1','reason':'Which environment?'}, correlation_id='flow'),
        event(5,'decision.needed','pm',{'task_id':1,'assignment_id':'task:1:attempt:1',
            'decision_id':'decision:task:1:attempt:1','reason':'Which environment?'}, correlation_id='flow')]
    return log


def failed_log():
    log = active_log()
    log.events.append(event(4,'task.attempt_failed','alice',{
        'task_id':1,'assignment_id':'task:1:attempt:1','worker_instance_id':'alice-1',
        'retryable':False,'failure_code':'bad','reason':'failure'}, correlation_id='flow'))
    planned = plan_next_emission(replay_events(log.events), now=101)
    assert planned['topic'] == 'task.failed', planned
    log.events.append(dict(planned,id=5,actor='pm',ts=101.0,schema_version=2))
    return log


class InterventionTests(unittest.TestCase):
    def invoke(self, log, command, **kwargs):
        return intervene(log, log.publish, command=command, task_id=1, now=101, **kwargs)

    def test_cancel_is_pending_not_terminal_and_repeated_command_is_idempotent(self):
        log = active_log()
        first = self.invoke(log,'cancel',reason='stop')
        self.assertTrue(first['accepted'])
        self.assertEqual('cancellation_requested',first['task_status'])
        self.assertEqual(2, first['event']['caused_by'])
        self.assertEqual('flow', first['event']['correlation_id'])
        second = self.invoke(log,'cancel',reason='stop')
        self.assertTrue(second['duplicate'])
        self.assertEqual(first['event']['id'],second['event']['id'])
        self.assertEqual(1,log.posts)
        with self.assertRaisesRegex(ValueError,'different intent'):
            self.invoke(log,'cancel',reason='changed')

    def test_completion_crossing_cancel_publication_is_reported_as_ignored(self):
        log = active_log()
        log.before_publish = lambda: log.events.append(event(4,'task.completed','alice',{
            'task_id':1,'assignment_id':'task:1:attempt:1','worker_instance_id':'alice-1'},correlation_id='flow'))
        result = self.invoke(log,'cancel',reason='too late')
        self.assertTrue(result['recorded'])
        self.assertFalse(result['accepted'])
        self.assertEqual('completed',result['task_status'])
        self.assertEqual(1,log.posts)

    def test_failed_task_retry_is_anchored_and_grants_exactly_requested_opportunities(self):
        log = failed_log()
        result = self.invoke(log,'retry',reason='fixed',anchor=5,additional_retries=2)
        self.assertTrue(result['accepted'])
        self.assertEqual('open',result['task_status'])
        self.assertEqual(5,result['event']['caused_by'])
        self.assertEqual(1,replay_events(log.events).tasks[1].max_retries)
        self.assertTrue(self.invoke(log,'retry',reason='fixed',anchor=5,additional_retries=2)['duplicate'])
        with self.assertRaises(ValueError):
            self.invoke(log,'retry',reason='fixed',anchor=4,additional_retries=2)
        self.assertEqual(1,log.posts)

    def test_decision_reply_keeps_identity_and_never_retargets(self):
        log = blocked_log()
        result = self.invoke(log,'decide',anchor=5,decision={'environment':'staging'})
        self.assertTrue(result['accepted'])
        self.assertEqual('open',result['task_status'])
        payload = result['event']['payload']
        self.assertEqual('decision:task:1:attempt:1',payload['decision_id'])
        self.assertEqual('task:1:attempt:1',payload['assignment_id'])
        self.assertTrue(self.invoke(log,'decide',anchor=5,decision={'environment':'staging'})['duplicate'])
        with self.assertRaises(ValueError):
            self.invoke(log,'decide',anchor=5,decision='new',idempotency_key='new-key')
        self.assertEqual(1,log.posts)

    def test_cancellation_crossing_decision_publication_rejects_reply(self):
        log = blocked_log()
        log.before_publish = lambda: log.events.append(event(6,'task.cancel_requested','human',
            {'task_id':1,'reason':'stop'},correlation_id='flow'))
        result = self.invoke(log,'decide',anchor=5,decision='staging')
        self.assertFalse(result['accepted'])
        self.assertEqual('cancellation_requested',result['task_status'])

    def test_supersession_crossing_retry_publication_rejects_retry(self):
        log = failed_log()
        replacement = created(6,task_id=2,correlation_id='flow')
        replacement['payload'].update(supersedes_task_id=1, supersession_reason='new intent')
        log.before_publish = lambda: log.events.append(replacement)
        result = self.invoke(log,'retry',reason='fixed',anchor=5,additional_retries=1)
        self.assertFalse(result['accepted'])

    def test_deadline_crossing_cancel_publication_rejects_claim(self):
        log = active_log()
        log.events[1]['payload']['deadline_at'] = 102.0
        log.before_publish = lambda: setattr(log,'timestamp',103.0)
        result = self.invoke(log,'cancel',reason='stop')
        self.assertFalse(result['accepted'])

    def test_duplicate_reply_does_not_answer_a_new_question(self):
        log = blocked_log()
        original = self.invoke(log,'decide',anchor=5,decision='first answer')
        log.events += [assigned(7,2),
            event(8,'task.blocked','alice',{'task_id':1,'assignment_id':'task:1:attempt:2',
                'worker_instance_id':'alice-1','reason':'New question'},correlation_id='flow'),
            event(9,'decision.needed','pm',{'task_id':1,'assignment_id':'task:1:attempt:2',
                'decision_id':'decision:task:1:attempt:2','reason':'New question'},correlation_id='flow')]
        repeated = self.invoke(log,'decide',anchor=5,decision='first answer')
        self.assertTrue(repeated['duplicate'])
        self.assertEqual(original['event']['id'],repeated['event']['id'])
        self.assertEqual(9,replay_events(log.events).tasks[1].decision_event_id)
        with self.assertRaises(ValueError):
            self.invoke(log,'decide',anchor=5,decision='first answer',idempotency_key='stale-again')

    def test_changed_boolean_and_number_are_not_the_same_intent(self):
        log = blocked_log()
        self.invoke(log,'decide',anchor=5,decision=True)
        with self.assertRaisesRegex(ValueError,'different intent'):
            self.invoke(log,'decide',anchor=5,decision=1)

    def test_recorded_but_unreadable_event_is_not_claimed_accepted(self):
        log = active_log()
        def publish(intent):
            result = log.publish(intent)
            log.events.pop()
            return result
        with self.assertRaisesRegex(ValueError,'could not be verified'):
            intervene(log,publish,command='cancel',task_id=1,reason='stop',now=101)


class DiscoveryTests(unittest.TestCase):
    def test_pages_and_pending_decisions_are_derived(self):
        log = blocked_log()
        log.events.append(created(6,task_id=2,correlation_id='other'))
        state, _, _, _, _ = read_snapshot(log)
        tasks = discovery_view(state,kind='tasks',now=101,lease_seconds=20,limit=1)
        self.assertEqual(1,tasks['next_after_task_id'])
        following = discovery_view(state,kind='tasks',now=101,lease_seconds=20,after_task_id=1)
        self.assertEqual(2,following['tasks'][0]['task_id'])
        decisions = discovery_view(state,kind='decisions',now=101,lease_seconds=20)
        self.assertEqual(5,decisions['decisions'][0]['decision_event_id'])
        workflows = discovery_view(state,kind='workflows',now=101,lease_seconds=20,limit=1)
        self.assertEqual('flow',workflows['next_after_workflow'])
        self.assertEqual(0,log.posts)

    def test_bounded_pages_and_fixed_head_ignore_newer_events(self):
        log = Log([created(i,task_id=i,correlation_id='flow') for i in range(1,1003)])
        events = list(log.iter_events(through_id=1001))
        self.assertEqual(1001,len(events))
        self.assertEqual([(0,1000),(1000,1000)],log.pages)
        self.assertEqual(0,log.posts)

    def test_invalid_order_and_missing_head_fail_closed(self):
        log = Log([created(2),created(1)])
        with self.assertRaises(ValueError):
            list(log.iter_events(through_id=3))
        log.health = lambda: {'ok':True}
        with self.assertRaisesRegex(ValueError,'upgrade'):
            read_snapshot(log)

    def test_cli_discovery_is_read_only_and_does_not_create_offsets(self):
        log = blocked_log()
        with tempfile.TemporaryDirectory() as tmp:
            old = os.getcwd()
            try:
                os.chdir(tmp)
                for name in ('tasks','workflows','decisions'):
                    out,err=io.StringIO(),io.StringIO()
                    code = logplane_cli.main([name,'--json','--url','http://test'],stdout=out,stderr=err,
                        client_factory=lambda *args,**kwargs:log,clock=lambda:101)
                    self.assertEqual(0,code,err.getvalue())
                    self.assertIn(name,json.loads(out.getvalue()))
                self.assertEqual([],list(Path(tmp).iterdir()))
            finally:
                os.chdir(old)

    def test_cli_intervention_uses_same_url_and_emits_acceptance(self):
        log = active_log()
        class Response:
            def __init__(self,value):self.value=value
            def raise_for_status(self):pass
            def json(self):return self.value
        def post(url, *, json, headers, timeout):
            self.assertEqual('http://test/events',url)
            return Response(log.publish(json))
        out,err=io.StringIO(),io.StringIO()
        with patch('logplane.cli.httpx.post',side_effect=post):
            code=logplane_cli.main(['cancel','1','--reason','stop','--json','--url','http://test'],
                stdout=out,stderr=err,client_factory=lambda *args,**kwargs:log,clock=lambda:101)
        self.assertEqual(0,code,err.getvalue())
        self.assertTrue(json.loads(out.getvalue())['accepted'])


if __name__ == '__main__':
    unittest.main()
