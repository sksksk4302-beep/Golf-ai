# crawler_utils.py
import json, os, re, time as _time, urllib.parse
import requests as std_requests  # Standard requests (always available, needed for teescan SSL bypass)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
try:
    from curl_cffi import requests
    HAS_CURL_CFFI = True
except ImportError:
    import requests
    HAS_CURL_CFFI = False
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple
from datetime import datetime

# urllib3 warnings are not relevant for curl_cffi


# ─────────────────────────────────────────────────────────────────────────────
# 구장 정보 로딩 (static/golf_clubs.json, Golpang_code: 골팡 표기 문자열)
base_dir = os.path.dirname(__file__)
golf_club_path = os.path.join(base_dir, "static", "golf_clubs.json")
with open(golf_club_path, "r", encoding="utf-8") as f:
    GOLF_CLUBS = json.load(f)

# ─────────────────────────────────────────────────────────────────────────────
# 공통 설정
GOLFPANG_BASE = "https://www.golfpang.com"
LIST_URL     = f"{GOLFPANG_BASE}/web/round/booking_list.do"
NODE_URL     = f"{GOLFPANG_BASE}/web/round/booking_node.do"
TBLLIST_URL  = f"{GOLFPANG_BASE}/web/round/booking_tblList.do"

CONNECT_TIMEOUT = int(os.environ.get("GPANG_CONNECT_TIMEOUT", 5))
READ_TIMEOUT    = int(os.environ.get("GPANG_READ_TIMEOUT", 20))
SLEEP_BETWEEN   = float(os.environ.get("GPANG_SLEEP", 0.25))
PROXY_WORKER_URL = os.environ.get("GPANG_PROXY_WORKER", "") # e.g., https://my-worker.workers.dev
TEESCAN_PROXY_URL = os.environ.get("TEESCAN_PROXY_URL", "") # e.g., https://teescan-proxy.xxx.workers.dev

COMMON_HEADERS = {
    # User-Agent is handled by curl_cffi impersonate="chrome"
    "Accept-Language": "ko,en;q=0.9",
}
HTML_HEADERS = {
    **COMMON_HEADERS,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Origin": GOLFPANG_BASE,
    "Referer": LIST_URL,
}
AJAX_HEADERS = {
    **COMMON_HEADERS,
    "Accept": "text/html, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": GOLFPANG_BASE,
    "Referer": LIST_URL,
    "x-customer-check": "gp-post-key-2019",
}

# ─────────────────────────────────────────────────────────────────────────────
# 유틸
def _make_session() -> requests.Session:
    # impersonate="chrome124" mimics a real Chrome browser's TLS fingerprint and headers
    if HAS_CURL_CFFI:
        s = requests.Session(impersonate="chrome124")
    else:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        })
    return s

def _get_url(url: str) -> str:
    """If PROXY_WORKER_URL is set, rewrite the URL to go through the proxy."""
    if PROXY_WORKER_URL:
        # Check if it already has query params
        sep = "&" if "?" in PROXY_WORKER_URL else "?"
        return f"{PROXY_WORKER_URL}{sep}url={url}"
    return url

def _fmt_ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def _parse_price(txt: str) -> Optional[int]:
    """Parse price from text, filtering out non-numeric entries like '가격문의', '상담' etc."""
    if not txt: return None
    
    # Filter out common Korean text patterns that indicate non-numeric prices
    txt_lower = txt.lower()
    non_numeric_patterns = ["문의", "상담", "확인", "전화", "call", "tbd", "미정"]
    if any(pattern in txt_lower for pattern in non_numeric_patterns):
        return None
    
    # Extract only digits
    d = re.sub(r"[^0-9]", "", txt)
    
    # Return None if no digits found or if the number seems invalid
    if not d:
        return None
    
    try:
        price = int(d)
        # Filter out unreasonably large or small prices (sanity check)
        # Golf prices typically range from 10,000 to 500,000 won
        if price < 1000 or price > 10000000:
            return None
        return price
    except (ValueError, OverflowError):
        return None

def _normalize_time_to_hour_num(t: str) -> Tuple[str, int]:
    """'08:12' / '8시' / '8시12분' → ('08시대', 8)"""
    if not t: return ("-1시대", -1)
    m = re.search(r"(\d{1,2})\s*:\s*(\d{2})", t)
    if m: h = int(m.group(1)); return (f"{h:02d}시대", h)
    m = re.search(r"(\d{1,2})\s*시", t)
    if m: h = int(m.group(1)); return (f"{h:02d}시대", h)
    return ("-1시대", -1)

def _fav_ok(name: str, favorite: List[str]) -> bool:
    if not favorite: return True
    return any(f and (f == name or f in name) for f in favorite)

def _sector_from_address(addr: Optional[str]) -> Optional[int]:
    """주소 앞 토큰으로 섹터 추정: 경기=5, 충청=4, 강원=8 (표시/필터용)"""
    if not addr: return None
    return None

def _normalize_md_from_kor_date(date_text: str) -> Optional[str]:
    m = re.search(r"(\d{1,2})\s*월\s*(\d{1,2})\s*일", date_text or "")
    if not m: return None
    return f"{int(m.group(1)):02d}-{int(m.group(2)):02d}"

def _same_mmdd(target_yyyy_mm_dd: str, kor_date_text: str) -> bool:
    dt = datetime.strptime(target_yyyy_mm_dd, "%Y-%m-%d")
    tgt = f"{dt.month:02d}-{dt.day:02d}"
    return (_normalize_md_from_kor_date(kor_date_text) or "") == tgt

def _is_maintenance_html(text: str) -> bool:
    if not text: return False
    t = str(text)
    return ("점검" in t and "서비스" in t) or ("점검중" in t) or ("점검 중" in t)

def _bootstrap_gp_session(s: requests.Session, date_str: str, sector: Optional[int] = None) -> bool:
    """골팡 세션/쿠키 준비: list.do GET → node.do POST(여러 페이로드). 실패해도 관용 모드."""
    ok_list = ok_node = False
    try:
        target_url = _get_url(LIST_URL)
        r1 = s.get(target_url, headers=HTML_HEADERS,
                   timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False)
        print(f"[{_fmt_ts()}] [Golfpang] bootstrap list.do status={r1.status_code}", flush=True)
        ok_list = (r1.status_code == 200)
    except Exception as e:
        print(f"[{_fmt_ts()}] [Golfpang] bootstrap list.do err={e}", flush=True)

    payloads = [
        {"Depth": "2", "GID": str(sector) if sector else "5", "SUB_GID": ""},
        {"roundDay": date_str},
        {"rd_date": date_str},
        {"rd_date": date_str, "sector": sector if sector is not None else ""},
    ]
    for p in payloads:
        try:
            target_url = _get_url(NODE_URL)
            r2 = s.post(target_url, headers=AJAX_HEADERS, data=p,
                        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False)
            print(f"[{_fmt_ts()}] [Golfpang] bootstrap node.do status={r2.status_code} payload={p}", flush=True)
            if r2.status_code == 200 and "점검" not in r2.text:
                ok_node = True; break
        except Exception as e:
            print(f"[{_fmt_ts()}] [Golfpang] bootstrap node.do err={e} payload={p}", flush=True)

    if not ok_node:
        print(f"[{_fmt_ts()}] [Golfpang] bootstrap node.do 실패 → list.do 쿠키만 진행(관용 모드)", flush=True)
    return ok_list or ok_node

# ─────────────────────────────────────────────────────────────────────────────
# 이름 매칭 (사이트 표기 ↔ 우리 JSON 표기)
def _norm_name(n: str) -> str:
    if not n: return ""
    # Remove parenthesis characters (do not remove contents)
    n = n.replace("(", "").replace(")", "")
    n = re.sub(r"\s+", "", n)          # 공백 제거
    n = re.sub(r"C\.?C\.?$", "CC", n)  # C.C → CC
    n = n.replace("-", "")
    return n

def _name_match(site_txt: str, gp_code_txt: str) -> bool:
    a = _norm_name(site_txt)
    b = _norm_name(gp_code_txt)
    if a == b: return True
    if b and b in a:
        # If substring match, ensure we aren't matching "Name" to "Name(Public)"
        # Check if the extra part contains parentheses
        extra = a.replace(b, "")
        if "(" in extra or ")" in extra:
            return False
        if "퍼" in extra or "9" in extra or "퍼블릭" in extra:
            return False
        return True
    return False

# ─────────────────────────────────────────────────────────────────────────────
# Teescan (원본 유지)
def get_teescan_times(s: std_requests.Session, seq: str, date_str: str) -> List[Dict]:
    """티스캐너 API에서 특정 구장/날짜의 티타임 리스트 조회 (with retry)"""
    raw_url = (
        "https://foapi.teescanner.com/v1/booking/getTeeTimeListbyGolfclub"
        f"?golfclub_seq={seq}&roundDay={date_str}&orderType="
    )
    
    # Check proxy setting
    seq_str = str(seq)
    if TEESCAN_PROXY_URL:
        sep = "&" if "?" in TEESCAN_PROXY_URL else "?"
        url = f"{TEESCAN_PROXY_URL}{sep}url={urllib.parse.quote(raw_url)}"
        if seq_str == "51": # Log URL only for 태광 to avoid leaking all URLs
            print(f"[Teescan] Using Proxy: {TEESCAN_PROXY_URL[:40]}...", flush=True)
    else:
        url = raw_url
    
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            # Removed the seq == "51" restriction for simple status logging
            r = s.get(url, timeout=30, verify=False)
            
            if r.status_code != 200:
                print(f"[Teescan] seq={seq_str} status={r.status_code} text={r.text[:200]}", flush=True)
                print(f"[Teescan] seq={seq_str} status={r.status_code} text={r.text[:200]}", flush=True)
                if attempt < max_retries:
                    _time.sleep(1.0)
                    continue
                return []
            
            try:
                data = r.json()
                tee_list = data.get("data", {}).get("teeTimeList", [])
                
                # LOG EVERY EMPTY CASE to see what the server is actually responding
                if not tee_list:
                    print(f"[Teescan] seq={seq_str} 0 items. ResultCode={data.get('resultCode')} Msg={data.get('resultMsg')}", flush=True)
                    
                return tee_list
            except Exception as json_e:
                # If JSON parsing fails, it's likely the proxy returned HTML (error page)
                print(f"[Teescan] JSON Error (seq={seq_str}): {json_e}. Raw start: {r.text[:100]}", flush=True)
                if attempt < max_retries:
                    _time.sleep(1.0)
                    continue
                return []
                
        except Exception as e:
            if attempt < max_retries:
                _time.sleep(1.0)
                continue
            print(f"[Teescan] seq={seq_str} Request Error: {e}", flush=True)
            return []

def crawl_teescan(date_str: str, favorite: List[str]):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    res: List[Dict] = []
    visited = set()
    tasks = []
    
    # Filter targets first
    targets = []
    for club in GOLF_CLUBS:
        name = club.get("name")
        if not name or name in visited: continue
        visited.add(name)
        seq = club.get("seq")
        if not seq: continue
        if favorite and name not in favorite: continue
        targets.append((name, seq))
        
    # Use standard requests.Session (NOT curl_cffi) for Teescan
    # curl_cffi ignores verify=False, causing SSL failures with teescan's self-signed cert
    s = std_requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.teescanner.com/",
        "Origin": "https://www.teescanner.com"
    })
    
    # Sequential crawling Instead of ThreadPool to avoid threading issues and ensure clear logs
    for t_name, t_seq in targets:
        try:
            print(f"[Teescan] Start crawling {t_name} (seq={t_seq})...", flush=True)
            items = get_teescan_times(s, t_seq, date_str)
            print(f"[Teescan] Found {len(items)} items for {t_name}", flush=True)
            
            for it in items:
                try:
                    price = int(it.get("min_cost") or it.get("price") or 0)
                    if price < 1000 or price > 10000000:
                        continue
                except (ValueError, TypeError):
                    continue
                
                ttxt  = str(it.get("teetime_time", "00:00"))
                h     = int(ttxt.split(":")[0]) if ":" in ttxt else int(ttxt[:2] or 0)
                # 티스캐너 API에서 benefit 필드 추출
                benefit_text = str(it.get("benefit", "") or "").strip()
                teescan_url = f"https://m.teescanner.com/booking/detail?tab=teetime&golfclub_seq={t_seq}&roundDay={date_str}&isJoin=N&entry_path=RT&step=1"
                res.append({
                    "golf": t_name, "date": date_str,
                    "hour": f"{h:02d}시대", "hour_num": h,
                    "price": price, "benefit": benefit_text,
                    "time": ttxt,
                    "url": teescan_url, "source": "teescan",
                })
        except Exception as e:
            print(f"[Teescan] Error processing {t_name}: {e}", flush=True)
            
    return res

# ─────────────────────────────────────────────────────────────────────────────
# Golfpang — clubname 비움 + 섹터(5,4,8) 순회 + 페이지 무제한 + 행단위 매핑 + 즉시 로그
def crawl_golfpang(date_str: str, favorite: List[str], sectors: List[int] = None):
    """
    - sector는 기본 [5,4,8]만 순회(환경변수 GPANG_SECTORS='5,4,8'로 변경 가능)
    - clubname='' 로 전체 수신 → <tr id="tr_*">를 행 단위 파싱
    - 병렬 처리: 각 섹터를 별도 스레드/세션으로 처리하여 속도 향상.
    """
    out: List[Dict] = []
    # 섹터 결정
    if sectors is None or len(sectors) == 0:
        env = os.environ.get("GPANG_SECTORS", "5,4,8")
        try:
            sectors = [int(x.strip()) for x in env.split(",") if x.strip()]
        except Exception:
            sectors = [5,4,8]
    else:
        sectors = [s for s in sectors if s in (5,4,8)]

    # 수집 대상 구장 준비 (공통)
    targets_all: List[Dict] = []
    for club in GOLF_CLUBS:
        name = club.get("name")
        gp_name = str(club.get("Golpang_code", "")).strip()
        if not name or not gp_name: continue
        if not _fav_ok(name, favorite): continue
        sector_guess = _sector_from_address(club.get("address"))
        targets_all.append({"name": name, "gp": gp_name, "sector": sector_guess})

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _process_sector(sector):
        local_out = []
        # 섹터별 대상 필터링
        targets = [t for t in targets_all if t["sector"] == sector or t["sector"] is None]
        
        # 각 스레드별 독립 세션 사용 (중요)
        with _make_session() as s:
            _bootstrap_gp_session(s, date_str, sector)
            print(f"[{_fmt_ts()}] [Golfpang] [START] sector={sector} date={date_str}", flush=True)

            seen = set()
            page = 1
            empty_consecutive_pages = 0
            
            while True:
                form = {
                    "pageNum": page,
                    "rd_date": date_str,
                    "sector": sector,
                    "clubname": "",
                    "bkOrder": "", "idx": "", "cust_nick": "",
                    "sector2": "", "sector3": "", "cdOrder": "",
                }
                
                try:
                    target_url = _get_url(TBLLIST_URL)
                    r = s.post(target_url, data=form, headers=AJAX_HEADERS,
                               timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False)
                    status = r.status_code
                    ctype = r.headers.get("Content-Type", "")
                    
                    if status >= 500 or _is_maintenance_html(r.text):
                        print(f"[{_fmt_ts()}] [Golfpang]   retry bootstrap (500/maintenance) sec={sector}", flush=True)
                        _bootstrap_gp_session(s, date_str, sector)
                        target_url = _get_url(TBLLIST_URL)
                        r = s.post(target_url, data=form, headers=AJAX_HEADERS,
                                   timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False)
                        status = r.status_code
                    
                    try:
                        soup = BeautifulSoup(r.text, "lxml")
                    except Exception:
                        soup = BeautifulSoup(r.text, "html.parser")

                    rows = soup.select('tr[id^="tr_"]')
                    added_this_page = 0

                    for tr in rows:
                        tds = tr.find_all("td")
                        if len(tds) < 5: continue

                        date_txt = tds[1].get_text(" ", strip=True)
                        time_txt = tds[2].get_text(" ", strip=True)
                        club_txt = tds[4].get_text(" ", strip=True)

                        if not _same_mmdd(date_str, date_txt):
                            continue

                        matched = None
                        # First pass: Exact match
                        for t in targets:
                            if _norm_name(club_txt) == _norm_name(t["gp"]):
                                matched = t; break
                        # Second pass: Substring match
                        if not matched:
                            for t in targets:
                                if _name_match(club_txt, t["gp"]):
                                    matched = t; break
                                    
                        if not matched:
                            continue

                        price_txt = ""
                        price_span = tr.select_one("span.price")
                        if price_span:
                            price_txt = price_span.get_text(strip=True)
                        if not price_txt:
                            m_price = re.search(r"([0-9][0-9,]{3,})\s*원?", tr.get_text(" ", strip=True))
                            price_txt = m_price.group(1) if m_price else ""
                        price = _parse_price(price_txt)
                        if price is None:
                            continue

                        hour_label, hour_num = _normalize_time_to_hour_num(time_txt)
                        if hour_num < 0:
                            continue

                        key = (matched["name"], date_str, hour_num, price)
                        if key in seen:
                            continue
                        seen.add(key)

                        local_out.append({
                            "golf": matched["name"],
                            "date": date_str,
                            "hour": hour_label,
                            "hour_num": hour_num,
                            "price": price,
                            "benefit": "",
                            "time": time_txt,
                            "url": GOLFPANG_BASE + "/",
                            "source": "golfpang",
                            "source_idx": tr.get("id", "").replace("tr_", ""),
                        })
                        added_this_page += 1

                    # Log/Break conditions
                    if not rows:
                        print(f"[{_fmt_ts()}] [Golfpang]  [STOP] No more rows. Stop sector={sector}", flush=True)
                        break
                    
                    if added_this_page == 0:
                        empty_consecutive_pages += 1
                    else:
                        empty_consecutive_pages = 0
                        
                    if empty_consecutive_pages >= 3:
                        print(f"[{_fmt_ts()}] [Golfpang]  [STOP] 3 consecutive pages with no matches. Stop sector={sector}", flush=True)
                        break

                    if page >= 20:
                        print(f"[{_fmt_ts()}] [Golfpang]  [STOP] Max page reached. Stop sector={sector}", flush=True)
                        break

                    page += 1
                    _time.sleep(0.05)
                    
                except Exception as e:
                    print(f"[{_fmt_ts()}] [Golfpang] Error processing sector={sector} page={page}: {e}", flush=True)
                    break
                    
        return local_out

    # Execute sectors in parallel
    workers = min(len(sectors), 3)
    if workers < 1: workers = 1
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_sector = {executor.submit(_process_sector, s): s for s in sectors}
        for future in as_completed(future_to_sector):
            sec = future_to_sector[future]
            try:
                data = future.result()
                out.extend(data)
                print(f"[{_fmt_ts()}] [Golfpang] [DONE] sector={sec} count={len(data)}", flush=True)
            except Exception as e:
                print(f"[{_fmt_ts()}] [Golfpang] [FAILED] sector={sec} err={e}", flush=True)

    out.sort(key=lambda x: (x.get("date",""), x.get("hour_num", 99), x.get("golf",""), x.get("price", 1<<60)))
    return out

# ─────────────────────────────────────────────────────────────────────────────
# Golfpang Specific Club (for optimization/repair)
def crawl_golfpang_specific_club(date_str: str, club_id: str, sector: int) -> List[Dict]:
    """
    Crawl a specific club using its ID.
    Uses 'clubname' and 'sector3' parameters with the club ID.
    """
    out: List[Dict] = []
    
    # Find club name from ID for logging/result
    club_name = "Unknown"
    for c in GOLF_CLUBS:
        if str(c.get("golfpang_id", "")) == str(club_id):
            club_name = c.get("name")
            break
            
    with _make_session() as s:
        _bootstrap_gp_session(s, date_str, sector)
        print(f"[{_fmt_ts()}] [Golfpang] [START_SPECIFIC] Club={club_name}({club_id}) date={date_str}", flush=True)
        
        seen = set()
        page = 1
        while True:
            form = {
                "pageNum": page,
                "rd_date": date_str,
                "sector": sector,
                "clubname": club_id,  # Club ID
                "bkOrder": "", "idx": "", "cust_nick": "",
                "sector2": "", 
                "sector3": club_id,   # Club ID
                "cdOrder": "",
            }
            
            try:
                target_url = _get_url(TBLLIST_URL)
                r = s.post(target_url, data=form, headers=AJAX_HEADERS,
                           timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False)
                
                try:
                    soup = BeautifulSoup(r.text, "lxml")
                except Exception:
                    soup = BeautifulSoup(r.text, "html.parser")
                rows = soup.select('tr[id^="tr_"]')
                
                if not rows:
                    break
                    
                added_this_page = 0
                for tr in rows:
                    tds = tr.find_all("td")
                    if len(tds) < 5: continue

                    date_txt = tds[1].get_text(" ", strip=True)
                    time_txt = tds[2].get_text(" ", strip=True)
                    # club_txt = tds[4].get_text(" ", strip=True) # Should match our club
                    
                    price_txt = ""
                    price_span = tr.select_one("span.price")
                    if price_span:
                        price_txt = price_span.get_text(strip=True)
                    if not price_txt:
                        m_price = re.search(r"([0-9][0-9,]{3,})\s*원?", tr.get_text(" ", strip=True))
                        price_txt = m_price.group(1) if m_price else ""
                    price = _parse_price(price_txt)
                    if price is None:
                        continue

                    hour_label, hour_num = _normalize_time_to_hour_num(time_txt)
                    if hour_num < 0:
                        continue
                        
                    key = (club_name, date_str, hour_num, price)
                    if key in seen:
                        continue
                    seen.add(key)
                    
                    out.append({
                        "golf": club_name,
                        "date": date_str,
                        "hour": hour_label,
                        "hour_num": hour_num,
                        "price": price,
                        "benefit": "",
                        "time": time_txt,
                        "url": GOLFPANG_BASE + "/",
                        "source": "golfpang",
                        "source_idx": tr.get("id", "").replace("tr_", ""),
                    })
                    added_this_page += 1
                
                print(f"[{_fmt_ts()}] [Golfpang]  Club={club_name} page={page} added={added_this_page}", flush=True)
                
                if page >= 10: # Safety limit for single club
                    break
                    
                page += 1
                _time.sleep(0.05)
                
            except Exception as e:
                print(f"Error crawling specific club {club_name}: {e}")
                break
                
    return out
