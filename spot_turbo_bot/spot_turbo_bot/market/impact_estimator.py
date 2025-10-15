import math
from statistics import median

from ..core.config import (
    IMPACT_PUMP_BASE,
    IMPACT_PUMP_MAX,
    IMPACT_PUMP_MIN,
    IMPACT_PUMP_SENSITIVITY,
    IMPACT_RATIO_CAP,
    IMPACT_SLIP_DEFAULT,
    IMPACT_UTIL_LAST,
)

_IMPACT_SLIP_DEFAULT = IMPACT_SLIP_DEFAULT
_IMPACT_UTIL_LAST = IMPACT_UTIL_LAST
_IMPACT_PUMP_BASE = IMPACT_PUMP_BASE
_IMPACT_PUMP_MIN = IMPACT_PUMP_MIN
_IMPACT_PUMP_MAX = IMPACT_PUMP_MAX
_IMPACT_PUMP_SENSITIVITY = IMPACT_PUMP_SENSITIVITY
_IMPACT_RATIO_CAP = IMPACT_RATIO_CAP

def floor_to_tick(px, tick): return math.floor(px/tick)*tick if tick>0 else px
def floor_to_step(q, step):  return math.floor(q/step)*step if step>0 else q

def impact_1pct_lb(book, slip=_IMPACT_SLIP_DEFAULT, util_last=_IMPACT_UTIL_LAST):
    ask0 = book.best_ask; tick=book.tick; step=book.step
    if ask0<=0 or not book.asks_px:
        return {"covered":False,"quote_lb":0.0,"levels_to_ceil":0,"ceil":None}
    ceil = floor_to_tick(ask0*(1.0+slip), tick)
    acc=0.0; j=0
    for i,p in enumerate(book.asks_px):
        if p < ceil:
            qf=floor_to_step(book.asks_q[i], step); acc += p*qf; j=i+1
        elif p == ceil:
            full_q=floor_to_step(book.asks_q[i], step)
            util_q=floor_to_step(book.asks_q[i]*util_last, step)
            acc += p*max(0.0, util_q); j=i+1; break
        else: break
    covered = (j>0 and book.asks_px[j-1] >= ceil)
    return {"covered": covered, "quote_lb": acc, "levels_to_ceil": j, "ceil": ceil, "ask0": ask0}

# تقدير أدق: دمج Power-Law + Exponential (مبسّط ونحيف)
def _safe(x, eps=1e-12): return max(x, eps)
def _log(x): return math.log(_safe(x))
def _density_min(asks_px, asks_q, tick, step):
    dens=[]
    for i in range(1,len(asks_px)):
        gap=max(asks_px[i]-asks_px[i-1], tick)
        dens.append(asks_q[i]/gap)
    return min(dens) if dens else 0.0

def estimate_fused(book, leftover_px, pump_k=_IMPACT_PUMP_BASE, util_last=_IMPACT_UTIL_LAST):
    px=book.asks_px; q=book.asks_q; tick=book.tick; step=book.step
    if leftover_px<=0 or len(px)<3: return 0.0, {"r2_pl":0.0,"fit_ex":0.0}
    # Power-law
    base=px[0]; X=[]; Y=[]
    for i in range(1,len(px)):
        d=max(px[i]-base, tick); dens=max(0.0, q[i]/max(px[i]-px[i-1], tick))
        if dens>0: X.append(d); Y.append(dens)
    if len(X)<3: return 0.0, {"r2_pl":0.0,"fit_ex":0.0}
    n=len(X); sx=sy=sxx=sxy=syy=0.0
    for i in range(n):
        lx=_log(X[i]); ly=_log(Y[i])
        sx+=lx; sy+=ly; sxx+=lx*lx; sxy+=lx*ly; syy+=ly*ly
    denom= n*sxx - sx*sx
    if abs(denom)<1e-12: return 0.0, {"r2_pl":0.0,"fit_ex":0.0}
    beta=(n*sxy - sx*sy)/denom; lnk=(sy - beta*sx)/n; k=math.exp(lnk)
    # R2
    mean_ly=sy/n; sse=sum(((lnk+beta*_log(X[i]) - _log(Y[i]))**2) for i in range(n))
    sst=sum(((_log(Y[i])-mean_ly)**2) for i in range(n)); r2_pl=0.0 if sst<=1e-12 else max(0.0,1.0-sse/sst)
    # integral
    x0=max(px[min(len(px)-1,1)]-base, tick); x1=x0+leftover_px
    if abs(beta+1.0)<1e-9: extra_base = k*math.log(x1/x0)
    else: extra_base = (k/(beta+1.0))*(x1**(beta+1.0) - x0**(beta+1.0))
    extra_quote_pl = max(0.0, extra_base) * px[-1]

    # Exponential (approx)
    deltas=[]; cum=[]; acc=0.0
    for i in range(1,len(px)):
        acc += max(0.0, q[i])
        deltas.append(max(px[i]-base, tick)); cum.append(acc)
    if len(deltas)>=3:
        i1=len(deltas)//3; i2=2*len(deltas)//3
        x1,y1=deltas[i1], cum[i1]; x2,y2=deltas[i2], cum[i2]
        A=max(cum); t1=_safe(1.0 - y1/_safe(A)); t2=_safe(1.0 - y2/_safe(A))
        lam=max(1e-6, (math.log(t1)-math.log(t2))/_safe(x2-x1))
        x0=deltas[-1]; x1=x0+leftover_px
        extra_base_ex=A*(math.exp(-lam*x0)-math.exp(-lam*x1))
        extra_quote_ex=max(0.0, extra_base_ex)*px[-1]; fit_ex= 1.0 - abs(A - cum[-1])/_safe(A,1.0)
    else:
        extra_quote_ex=0.0; fit_ex=0.0

    w1=max(0.0,min(1.0,r2_pl)); w2=max(0.0,min(1.0,fit_ex)); ws=max(1e-9, w1+w2)
    fused=(w1*extra_quote_pl + w2*extra_quote_ex)/ws
    fused*= pump_k * util_last
    return max(0.0, fused), {"r2_pl":round(r2_pl,3),"fit_ex":round(fit_ex,3)}

def impact_1pct_estimate(book, lb_res, pump_ctx, ratio_cap=_IMPACT_RATIO_CAP, util_last=_IMPACT_UTIL_LAST):
    if lb_res["levels_to_ceil"]<=0: last_px = book.best_ask
    else: last_px = book.asks_px[lb_res["levels_to_ceil"]-1]
    leftover = max(0.0, lb_res["ceil"] - last_px)
    dynamic_pump = _IMPACT_PUMP_BASE + _IMPACT_PUMP_SENSITIVITY * pump_ctx.get("cancel_add", 0.0)
    pump_factor = max(_IMPACT_PUMP_MIN, min(_IMPACT_PUMP_MAX, dynamic_pump))
    extra, fit = estimate_fused(book, leftover, pump_k=pump_factor, util_last=util_last)
    est = lb_res["quote_lb"] + extra
    est = min(est, lb_res["quote_lb"]*ratio_cap)
    rm = pump_ctx.get("rolling_min_quote")
    if rm is not None: est = min(est, rm)
    return {"quote_est_fused": est, **fit}
