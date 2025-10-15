import math
def pick_bucket(ask0, asks_px, tick, slip=0.01, pad=2):
    if not asks_px or ask0<=0 or tick<=0: return 20
    ceil = math.floor(ask0*(1.0+slip)/tick)*tick
    gaps=[asks_px[i]-asks_px[i-1] for i in range(1,len(asks_px)) if asks_px[i]>asks_px[i-1]]
    m = sorted(gaps)[len(gaps)//2] if gaps else tick
    delta=max(0.0, ceil-ask0)
    n_est= 5 if m<=0 else math.ceil(delta/m)+pad
    n_tick_cap= math.ceil(delta/tick)
    n_est=min(max(5,n_est), max(5,n_tick_cap))
    return 20 if n_est<=20 else 50

