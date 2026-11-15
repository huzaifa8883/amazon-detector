import sys, tempfile, pathlib, time
from datetime import datetime, timezone
src = pathlib.Path(__file__).parent / 'tests/reference'
sys.path.insert(0, str(src))
from job_registry import Registry

with tempfile.TemporaryDirectory() as tmp:
    db = Registry(pathlib.Path(tmp)/'registry.sqlite3')
    now=time.time()
    obs = {'detector_id':'detector-01', 'sequence':75,
           'observed_at':datetime.fromtimestamp(now,timezone.utc).isoformat(),
           'render_complete':True,
           'job':{'job_id':'cardtext-v1-sequence-simulation','title':'Warehouse Associate',
                  'location':'Within 14.2 mi | Harlow, England',
                  'visible_card_text':'Warehouse Associate Type: Reduced Duration: Fixed-term Pay rate: From GBP16.10 Within 14.2 mi | Harlow, England'}}
    assert db.observe(obs,received_at=now).status=='new_job'
    obs['sequence']=6
    assert db.observe(obs,received_at=now+0.1).status=='out_of_order'
    assert db.db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0]==1
    obs['sequence']=1793804567890
    assert db.observe(obs,received_at=now+0.2).status=='unchanged'
    assert db.db.execute("SELECT sequence FROM detector_state WHERE detector_id='detector-01'").fetchone()[0]==1793804567890
    db.close()
print('PASS: isolated Oracle registry rejects sequence 6 after 75 and accepts millisecond-based sequence')
