import unittest,asyncio,pathlib,tempfile,sys
R=pathlib.Path(__file__).resolve().parents[1];sys.path.insert(0,str(R))
from deadline_guard import Ledger,tracked_exchange,OutcomeUnknown
class ExchangeTests(unittest.IsolatedAsyncioTestCase):
 def setUp(self):
  import time
  base=pathlib.Path(__file__).parent/'cpu_cases';base.mkdir(exist_ok=True);self.out=pathlib.Path(tempfile.mkdtemp(prefix=self._testMethodName+'_',dir=base));self.g=Ledger(self.out,'async-fixture',time.monotonic()+10);self.g.begin('original-model-id','model','original-body')
 async def test_normal_exchange(self):
  sent=[]
  async def send(timeout):sent.append(timeout);return b'complete response'
  self.assertEqual(await tracked_exchange(self.g,'original-model-id',send),b'complete response');self.assertEqual(len(sent),1)
 async def test_cancellation_after_server_side_effect(self):
  started=asyncio.Event();side=[]
  async def send(timeout):side.append('server accepted');started.set();await asyncio.Event().wait()
  task=asyncio.create_task(tracked_exchange(self.g,'original-model-id',send));await started.wait();task.cancel()
  with self.assertRaises(asyncio.CancelledError):await task
  self.assertEqual(side,['server accepted']);self.assertEqual(self.g.rows['original-model-id']['remote_outcome'],'outcome_unknown')
 async def test_server_done_ack_missing_not_completed(self):
  started=asyncio.Event()
  async def send(timeout):
   (self.out/'server_done_receipt.txt').write_text('mock server has finished; no client ACK');started.set();await asyncio.Event().wait()
  task=asyncio.create_task(tracked_exchange(self.g,'original-model-id',send));await started.wait();task.cancel()
  with self.assertRaises(asyncio.CancelledError):await task
  self.assertNotEqual(self.g.rows['original-model-id']['state'],'completed')
 async def test_response_during_cleanup_ignored(self):
  async def send(timeout):self.g.start_cleanup();return b'late response'
  with self.assertRaises(OutcomeUnknown):await tracked_exchange(self.g,'original-model-id',send)
  self.assertFalse(self.g.safe_to_advance)
 async def test_timeout_never_retry_and_no_background_child(self):
  count=[];ended=[]
  self.g.rows['original-model-id']['timeout_seconds']=.02
  async def send(timeout):
   count.append('send')
   try:await asyncio.sleep(10)
   finally:ended.append('awaited cancellation complete')
  with self.assertRaises(TimeoutError):await tracked_exchange(self.g,'original-model-id',send)
  self.assertEqual(len(count),1);self.assertEqual(len(ended),1);self.assertEqual(self.g.rows['original-model-id']['remote_outcome'],'outcome_unknown')
 async def test_late_sender_cannot_restore_success(self):
  async def send(timeout):self.g.cancel('original-model-id','external cleanup');return b'response after cancellation'
  with self.assertRaises(OutcomeUnknown):await tracked_exchange(self.g,'original-model-id',send)
if __name__=='__main__':unittest.main()
