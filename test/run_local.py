import os, sys, time, json
os.environ["RESULTS_BUCKET"]="local"
sys.path.insert(0, "../src")
import handler as h
csvfile = sys.argv[1]
h.DATA_URL = "file://" + os.path.abspath(csvfile)  # urllib handles file://
class Ctx:
    def __init__(s, secs): s.end = time.time()+secs
    def get_remaining_time_in_millis(s): return int((s.end-time.time())*1000)
job = {"request_id":"local","season":"x","team":"Man United","bootstrap_fits":int(sys.argv[2]),"sims_per_fit":int(sys.argv[3]),"seed":7}
t=time.time(); r = h.run(job, Ctx(1800)); 
r["position_distribution"] = {k: round(v,4) for k,v in r["position_distribution"].items()}
print(json.dumps(r, indent=1)); print("wall", round(time.time()-t,2), "s")
