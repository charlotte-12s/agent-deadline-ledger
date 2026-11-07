"""Durable project-only request accounting. No model, network, SDK or GPU imports."""
import asyncio,hashlib,json,math,os,pathlib,threading,time
VERSION='monotonic-request-guard-v1.2'
POLICY=dict(version=VERSION,minimum_model_admission_seconds=5.,minimum_tool_admission_seconds=0.,request_cap_seconds=900.,command_cap_seconds=240.,task_seconds=1800,session_seconds=12000,retry_unknown=False)
def sha(data):return hashlib.sha256(data).hexdigest()
class AdmissionRejected(TimeoutError):pass
class OutcomeUnknown(RuntimeError):pass
class ReplayBlocked(RuntimeError):pass
class ProtocolMismatch(RuntimeError):pass
def finite(x):return type(x) in (int,float) and math.isfinite(x)

class FixedDeadline:
 def __init__(self,task_seconds,session_deadline,*,mono=time.monotonic):
  if not finite(task_seconds) or task_seconds<=0 or not finite(session_deadline):raise ValueError('invalid monotonic deadline')
  self.mono=mono;self.started=mono();self.deadline=min(self.started+task_seconds,session_deadline)
 @classmethod
 def bridge(cls,remaining_provider,*,mono=time.monotonic):
  before=mono();remaining=remaining_provider()
  if not finite(remaining) or remaining<0:raise ValueError('invalid SDK remaining time')
  # Charge time spent querying the provider; never rebase/extend this instance.
  return cls(remaining,before+remaining,mono=mono) if remaining>0 else cls(.000001,before,mono=mono)
 def remaining(self):return max(0.,self.deadline-self.mono())

class Ledger:
 def __init__(self,out,protocol,deadline,*,mono=time.monotonic,wall=time.time,resume=False):
  if not finite(deadline):raise ValueError('invalid monotonic deadline')
  self.out=pathlib.Path(out);self.out.mkdir(parents=True,exist_ok=True);self.path=self.out/'lifecycle.jsonl'
  self.protocol=protocol;self.deadline=deadline;self.mono=mono;self.wall=wall;self.rows={};self.patch=None;self.closed=False;self.resumed=False;self.budget_stopped=False;self.lock=threading.RLock();self.broken=False
  identity=dict(protocol=protocol,policy=POLICY)
  self.identity=sha(json.dumps(identity,sort_keys=True).encode())
  if self.path.exists():
   events=[json.loads(line) for line in self.path.read_text(encoding='utf8').splitlines()]
   if not events or events[0].get('identity')!=self.identity:raise ProtocolMismatch('old protocol/guard policy cannot resume')
   if not resume:raise ReplayBlocked('existing ledger; explicit read-only resume required')
   self.resumed=True
   for event in events:self._apply(event)
   for row in self.rows.values():
    if row['state'] in ('prepared','dispatched'):row.update(state='outcome_unknown',remote_outcome='outcome_unknown',resume_note='interrupted receipt; never resend')
   return
  with self.path.open('x',encoding='utf8'):pass
  self._event('header',identity=self.identity,protocol=protocol,policy=POLICY,monotonic_deadline=deadline)
 def _apply(self,e):
  rid=e.get('request_id')
  if e.get('row') is not None:self.rows[rid]=e['row']
  if e['event']=='patch_ack':self.patch=e['patch']
  if e['event']=='cleanup_started':self.closed=True
  if e['event']=='deadline_admission_rejected':self.budget_stopped=True
 def _event(self,event,request_id=None,**details):
  record=dict(event=event,request_id=request_id,mono=self.mono(),wall=self.wall(),remaining_seconds=max(0.,self.deadline-self.mono()),**details)
  try:
   with self.path.open('a',encoding='utf8') as f:f.write(json.dumps(record,ensure_ascii=False)+'\n');f.flush();os.fsync(f.fileno())
  except OSError:self.broken=True;raise
  self._apply(record);return record
 @property
 def safe_to_advance(self):
  return not (self.closed or self.resumed or self.broken) and all(r['state'] in ('completed','deadline_admission_rejected','not_sent') for r in self.rows.values())
 def begin(self,rid,kind,fingerprint,*,cap=None,min_remaining=None):
  with self.lock:
   if self.resumed or self.closed or self.broken:raise ReplayBlocked('closed/read-only/failed ledger cannot dispatch')
   if rid in self.rows or any(r['kind']==kind and r['fingerprint']==fingerprint and r['state']!='completed' for r in self.rows.values()):raise ReplayBlocked('recorded operation must not be retried')
   if any(r['state'] in ('cancelled','outcome_unknown') and r['remote_outcome']=='outcome_unknown' for r in self.rows.values()):raise ReplayBlocked('unknown outcome blocks new execution')
   if kind=='model' and any(r['kind']=='model' and r['state']=='dispatched' for r in self.rows.values()):raise ReplayBlocked('overlapping model request')
   threshold=POLICY['minimum_model_admission_seconds'] if kind=='model' else POLICY['minimum_tool_admission_seconds']
   if min_remaining is not None:threshold=min_remaining
   cap=POLICY['request_cap_seconds'] if cap is None else cap
   if not finite(cap) or cap<=0 or not finite(threshold) or threshold<0:raise ValueError('invalid timeout policy')
   remaining=self.deadline-self.mono();row=dict(kind=kind,fingerprint=fingerprint,state='prepared',remote_outcome='not_sent',minimum_remaining=threshold,timeout_seconds=max(0.,min(cap,remaining)),started_mono=self.mono(),started_wall=self.wall())
   if remaining<=0 or remaining<threshold:
    row['state']='deadline_admission_rejected';self._event('deadline_admission_rejected',rid,row=row,reason='insufficient remaining task/session budget');raise AdmissionRejected('deadline_admission_rejected')
   self._event('admitted',rid,row=row);return row['timeout_seconds']
 def dispatch(self,rid):
  with self.lock:
   row=dict(self.rows[rid]);remaining=self.deadline-self.mono()
   if self.closed or row['state']!='prepared':raise ReplayBlocked('request is not prepared for dispatch')
   if remaining<=0 or remaining<row['minimum_remaining']:
    row.update(state='deadline_admission_rejected',remote_outcome='not_sent');self._event('deadline_admission_rejected',rid,row=row,reason='budget changed before send');raise AdmissionRejected('deadline_admission_rejected')
   row.update(state='dispatched',remote_outcome='outcome_unknown',dispatched_mono=self.mono(),dispatched_wall=self.wall(),timeout_seconds=min(row['timeout_seconds'],remaining));self._event('dispatched',rid,row=row);return row['timeout_seconds']
 def complete(self,rid,evidence):
  with self.lock:
   row=dict(self.rows[rid])
   if self.closed or self.resumed or row['state']!='dispatched' or self.mono()>=self.deadline:
    if row['state'] not in ('cancelled','outcome_unknown'):row.update(state='outcome_unknown',remote_outcome='outcome_unknown')
    self._event('late_response_ignored',rid,row=row,evidence=evidence);raise OutcomeUnknown('late/cancelled/cleanup response cannot become success')
   row.update(state='completed',remote_outcome='completed',completed_mono=self.mono(),completed_wall=self.wall(),completion_evidence=evidence);self._event('completed',rid,row=row)
 def cancel(self,rid,error):
  with self.lock:
   row=dict(self.rows[rid])
   if row['state']=='completed':self._event('cancel_after_complete',rid,error=error);return
   if row['state']=='cancelled':self._event('duplicate_cancel',rid,error=error);return
   row.update(state='cancelled',remote_outcome='outcome_unknown' if row['state']=='dispatched' else 'not_sent');self._event('cancelled',rid,row=row,error=error)
 def unknown(self,rid,error):
  with self.lock:
   row=dict(self.rows[rid]);row.update(state='outcome_unknown',remote_outcome='outcome_unknown');self._event('outcome_unknown',rid,row=row,error=error)
 def not_sent(self,rid,error):
  with self.lock:
   row=dict(self.rows[rid])
   if row['state']!='prepared':raise OutcomeUnknown('cannot relabel a dispatched request as not sent')
   row.update(state='not_sent',remote_outcome='not_sent');self._event('not_sent',rid,row=row,error=error)
 def patch_ack(self,rid,patch):
  with self.lock:
   row=self.rows[rid]
   if row['kind']!='submit_patch' or row['state']!='completed' or self.closed:raise OutcomeUnknown('no proven successful submit_patch ACK')
   if not isinstance(patch,bytes) or not patch:raise ValueError('empty/nonbytes submitted patch')
   digest=sha(patch);path='submitted_patch_'+digest+'.diff';target=self.out/path
   if target.exists():
    if target.read_bytes()!=patch:raise ValueError('patch identity drift')
   else:
    with target.open('xb') as f:f.write(patch);f.flush();os.fsync(f.fileno())
   self._event('patch_ack',rid,patch=dict(path=path,sha256=digest,bytes=len(patch),request_id=rid,source='client observed SDK success and submitted bytes'))
 def start_cleanup(self):
  with self.lock:
   if self.closed:return
   self._event('cleanup_started')
   for rid,row in list(self.rows.items()):
    if row['state']=='dispatched':self.unknown(rid,'cleanup started before completion ACK')
 def snapshot(self):return dict(protocol=self.protocol,identity=self.identity,rows=self.rows,patch=self.patch,safe_to_advance=self.safe_to_advance,budget_stopped=self.budget_stopped,closed=self.closed,resumed_read_only=self.resumed)

async def tracked_exchange(ledger,rid,send):
 """One awaited exchange only. No retries, shields or detached background tasks."""
 timeout=ledger.dispatch(rid)
 try:
  response=await asyncio.wait_for(send(timeout),timeout=timeout)
  evidence=dict(kind='client_observed_complete_response')
  if isinstance(response,tuple) and len(response)==2 and isinstance(response[1],bytes):evidence.update(raw_sha256=sha(response[1]),bytes=len(response[1]),http_status=getattr(response[0],'status_code',None))
  ledger.complete(rid,evidence);return response
 except asyncio.CancelledError:
  ledger.cancel(rid,'client CancelledError; remote outcome UNKNOWN');raise
 except (TimeoutError,Exception) as error:
  if ledger.rows[rid]['state'] not in ('completed','cancelled','outcome_unknown'):ledger.unknown(rid,type(error).__name__+': '+str(error))
  raise
