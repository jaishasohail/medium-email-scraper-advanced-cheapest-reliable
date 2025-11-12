from typing import Iterable, List, Dict

def _lc(s: str) -> str:
    return s.lower() if isinstance(s, str) else ""

def dedupe_by_url(records: List[Dict]) -> List[Dict]:
    seen = set()
    out: List[Dict] = []
    for r in records:
        url = _lc(r.get("url", ""))
        if url and url not in seen:
            seen.add(url)
            out.append(r)
    return out

def apply_keyword_filter(records: Iterable[Dict], keywords: List[str]) -> List[Dict]:
    if not keywords:
        return list(records)
    kws = [k.lower().strip() for k in keywords if k and k.strip()]
    out: List[Dict] = []
    for r in records:
        hay = " ".join([_lc(r.get("title")), _lc(r.get("snippet"))])
        if any(k in hay for k in kws):
            out.append(r)
    return out

def apply_domain_filter(records: Iterable[Dict], domains: List[str]) -> List[Dict]:
    if not domains:
        return list(records)
    ds = {d.lower().strip() for d in domains if d and d.strip()}
    out: List[Dict] = []
    for r in records:
        dom = _lc(r.get("email_domain"))
        if dom and (dom in ds or any(dom.endswith(f".{d}") for d in ds)):
            out.append(r)
    return out

def apply_location_filter(records: Iterable[Dict], locations: List[str]) -> List[Dict]:
    if not locations:
        return list(records)
    locs = [l.lower().strip() for l in locations if l and l.strip()]
    out: List[Dict] = []
    for r in records:
        hay = _lc(r.get("location")) + " " + _lc(r.get("snippet"))
        if any(l in hay for l in locs):
            out.append(r)
    return out