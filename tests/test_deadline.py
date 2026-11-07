import pathlib,sys,unittest,tempfile,json,asyncio
R=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(R))
class Clock:
 def __init__(self):self.m=0.;self.w=1000.
 def mono(self):return self.m
 def wall(self):return self.w
class DeadlineTests(unittest.TestCase):
 def setUp(self):
  import deadline_guard as d
  self.d=d;self.c=Clock();base=pathlib.Path(__file__).parent/'cpu_cases';base.mkdir(exist_ok=True);self.out=pathlib.Path(tempfile.mkdtemp(prefix=self._testMethodName+'_',dir=base));self.g=d.Ledger(self.out,'cpu-v1',100,mono=self.c.mono,wall=self.c.wall)
 def begin(self,rid='original-rid',kind='model'):return self.g.begin(rid,kind,'payload-'+rid)
 def test_sufficient_complete(self):
  self.assertEqual(self.begin(),100);self.g.dispatch('original-rid');self.c.m=3;self.g.complete('original-rid','raw-sha');self.assertEqual(self.g.rows['original-rid']['state'],'completed');self.assertTrue(self.g.safe_to_advance)
 def test_insufficient_rejected(self):
  self.c.m=98.5
  with self.assertRaises(self.d.AdmissionRejected):self.begin()
  self.assertEqual(self.g.rows['original-rid']['state'],'deadline_admission_rejected');self.assertEqual(self.g.rows['original-rid']['remote_outcome'],'not_sent');self.assertTrue(self.g.safe_to_advance)
 def test_sent_deadline_cancelled_unknown(self):
  self.begin();self.g.dispatch('original-rid');self.c.m=100;self.g.cancel('original-rid','SDK CancelledError');self.assertEqual(self.g.rows['original-rid']['state'],'cancelled');self.assertEqual(self.g.rows['original-rid']['remote_outcome'],'outcome_unknown');self.assertFalse(self.g.safe_to_advance)
 def test_server_done_without_client_ack_unknown(self):
  self.begin();self.g.dispatch('original-rid');(self.out/'server_effect.txt').write_text('server completed but ACK lost');self.g.unknown('original-rid','ACK lost');self.assertEqual(self.g.rows['original-rid']['state'],'outcome_unknown')
 def test_unknown_side_effect_never_retried(self):
  self.begin(kind='tool');self.g.dispatch('original-rid');self.g.unknown('original-rid','side effect possible')
  with self.assertRaises(self.d.ReplayBlocked):self.g.begin('different-rid','tool','payload-original-rid')
 def test_patch_before_later_cancel_preserved(self):
  self.begin('submit-1','submit_patch');self.g.dispatch('submit-1');self.g.complete('submit-1','sdk-ok');self.g.patch_ack('submit-1',b'valid patch');self.begin('later');self.g.dispatch('later');self.g.cancel('later','SDK deadline');self.assertEqual(self.g.patch['sha256'],self.d.sha(b'valid patch'));self.assertEqual((self.out/self.g.patch['path']).read_bytes(),b'valid patch')
 def test_submit_cancelled_has_no_ack(self):
  self.begin(kind='submit_patch');self.g.dispatch('original-rid');self.g.cancel('original-rid','cancel during submit')
  with self.assertRaises(self.d.OutcomeUnknown):self.g.patch_ack('original-rid',b'maybe patch')
  self.assertIsNone(self.g.patch)
 def test_command_timeout_capped_to_remaining(self):
  self.c.m=98.2;self.assertAlmostEqual(self.g.begin('cmd','command','cmd-hash',cap=240),1.8)
 def test_duplicate_cancellation_idempotent(self):
  self.begin();self.g.dispatch('original-rid');self.g.cancel('original-rid','first');self.g.cancel('original-rid','repeat');self.assertEqual(self.g.rows['original-rid']['remote_outcome'],'outcome_unknown')
 def test_late_response_cleanup_never_success(self):
  self.begin();self.g.dispatch('original-rid');self.g.start_cleanup()
  with self.assertRaises(self.d.OutcomeUnknown):self.g.complete('original-rid','late-response-sha')
  self.assertNotEqual(self.g.rows['original-rid']['state'],'completed')
 def test_resume_unknown_no_execution(self):
  self.begin();self.g.dispatch('original-rid');other=self.d.Ledger(self.out,'cpu-v1',100,mono=self.c.mono,wall=self.c.wall,resume=True)
  self.assertEqual(other.rows['original-rid']['remote_outcome'],'outcome_unknown')
  with self.assertRaises(self.d.ReplayBlocked):other.begin('new','tool','new-hash')
 def test_cross_protocol_refused(self):
  with self.assertRaises(self.d.ProtocolMismatch):self.d.Ledger(self.out,'old-protocol',100,mono=self.c.mono,wall=self.c.wall,resume=True)
 def test_wall_jump_does_not_extend_deadline(self):
  self.c.w-=100000;self.c.m=98
  with self.assertRaises(self.d.AdmissionRejected):self.begin()
 def test_exact_reserve_boundary(self):
  self.c.m=95;self.assertEqual(self.begin(),5)
 def test_expired_prepared_never_dispatched(self):
  self.begin();self.c.m=101
  with self.assertRaises(self.d.AdmissionRejected):self.g.dispatch('original-rid')
  self.assertEqual(self.g.rows['original-rid']['remote_outcome'],'not_sent')
 def test_cancel_after_proven_completion_not_relabelled(self):
  self.begin();self.g.dispatch('original-rid');self.g.complete('original-rid','raw-sha');self.g.cancel('original-rid','caller cancelled after ACK');self.assertEqual(self.g.rows['original-rid']['state'],'completed')
 def test_late_completion_after_deadline_is_not_ack(self):
  self.begin();self.g.dispatch('original-rid');self.c.m=101
  with self.assertRaises(self.d.OutcomeUnknown):self.g.complete('original-rid','late')
 def test_journal_keeps_id_clocks_errors(self):
  self.begin();self.g.dispatch('original-rid');self.g.cancel('original-rid','original error');records=[json.loads(x) for x in (self.out/'lifecycle.jsonl').read_text().splitlines()];self.assertTrue(any(x.get('request_id')=='original-rid' and x.get('error')=='original error' for x in records));self.assertTrue(all('mono' in x and 'wall' in x for x in records))
 def test_nested_tool_command_can_start(self):
  self.begin('submit-parent','submit_patch');self.g.dispatch('submit-parent');self.assertEqual(self.g.begin('child-command','command','git-diff',cap=240),100)
 def test_explicit_repeated_completed_command_not_automatic_retry(self):
  self.g.begin('first','command','same-command');self.g.dispatch('first');self.g.complete('first','ACK');self.g.begin('second','command','same-command');self.assertEqual(self.g.rows['second']['state'],'prepared')
 def test_fixed_clock_uses_earlier_session_limit(self):
  f=self.d.FixedDeadline(1800,50,mono=self.c.mono);self.c.m=40;self.assertEqual(f.remaining(),10);self.c.w+=100000;self.assertEqual(f.deadline,50)
 def test_bridge_charges_provider_delay(self):
  def remaining():self.c.m=2;return 10
  f=self.d.FixedDeadline.bridge(remaining,mono=self.c.mono);self.assertEqual(f.deadline,10);self.assertEqual(f.remaining(),8)
if __name__=='__main__':unittest.main()
