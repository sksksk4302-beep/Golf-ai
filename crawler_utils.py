# crawler_utils.py
import requests, json, os, re, time as _time
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

COMMON_HEADERS = {
    "User-Agent": os.environ.get(
        "GPANG_UA",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    ),
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
}

# ─────────────────────────────────────────────────────────────────────────────
# 유틸
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6, connect=6, read=6, backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=40)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def _fmt_ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def _parse_price(txt: str) -> int:
    d = re.sub(r"[^0-9]", "", txt or "")
    return int(d) if d else 10**12

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
        r1 = s.get(LIST_URL, headers=HTML_HEADERS,
                   timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False)
        print(f"[{_fmt_ts()}] [Golfpang] bootstrap list.do status={r1.status_code}", flush=True)
        ok_list = (r1.status_code == 200)
    except Exception as e:
        print(f"[{_fmt_ts()}] [Golfpang] bootstrap list.do err={e}", flush=True)

    payloads = [
        {"roundDay": date_str},
        {"rd_date": date_str},
        {"rd_date": date_str, "sector": sector if sector is not None else ""},
    ]
    for p in payloads:
        try:
            r2 = s.post(NODE_URL, headers=AJAX_HEADERS, data=p,
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
    # n = re.sub(r"\(.*?\)", "", n)      # 괄호 제거 (REMOVED to distinguish Public/Member)
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
        return True
    return False

# ─────────────────────────────────────────────────────────────────────────────
# Teescan (원본 유지)
def get_teescan_times(seq: str, date_str: str) -> List[Dict]:
    """티스캐너 API에서 특정 구장/날짜의 티타임 리스트 조회"""
    url = (
        "https://foapi.teescanner.com/v1/booking/getTeeTimeListbyGolfclub"
        f"?golfclub_seq={seq}&roundDay={date_str}&orderType="
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=3)
        return r.json().get("data", {}).get("teeTimeList", [])
    except Exception as e:
        print(f"[Teescan] seq={seq} date={date_str} 오류: {e}", flush=True)
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
        
    def _fetch(t_name, t_seq):
        # print(f"[Teescan] Fetching {t_name}...", flush=True)
        try:
            items = get_teescan_times(t_seq, date_str)
        except Exception as e:
            print(f"[Teescan] Error fetching {t_name}: {e}", flush=True)
            return []
            
        local_res = []
        for it in items:
            price = int(it.get("price", 10**12))
            ttxt  = str(it.get("teetime_time", "00:00"))
            h     = int(ttxt.split(":")[0]) if ":" in ttxt else int(ttxt[:2] or 0)
            local_res.append({
                "golf": t_name, "date": date_str,
                "hour": f"{h:02d}시대", "hour_num": h,
                "price": price, "benefit": "",
                "time": ttxt,
                "url": "https://www.teescanner.com/", "source": "teescan",
            })
        return local_res

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_club = {executor.submit(_fetch, n, s): n for n, s in targets}
        for future in as_completed(future_to_club):
            club_name = future_to_club[future]
            try:
                data = future.result(timeout=10)
                res.extend(data)
            except Exception as e:
                print(f"[Teescan] Error processing {club_name}: {e}")
                
    return res

# ─────────────────────────────────────────────────────────────────────────────
# Golfpang — clubname 비움 + 섹터(5,4,8) 순회 + 페이지 무제한 + 행단위 매핑 + 즉시 로그
def crawl_golfpang(date_str: str, favorite: List[str], sectors: List[int] = None):
    """
    - sector는 기본 [5,4,8]만 순회(환경변수 GPANG_SECTORS='5,4,8'로 변경 가능)
    - clubname='' 로 전체 수신 → <tr id="tr_*">를 행 단위 파싱
      * td[1]=날짜('10월15일 (수)') → date_str와 MM-DD 비교
      * td[2]=시간('06:42'/'19시')
      * td[4]=골프장명(사이트 표기) → GOLF_CLUBS[].Golpang_code와 이름 매칭
      * span.price=가격
    - pageNum 1→… 순회 (해당 페이지에서 신규 결과 0 → 그 섹터 종료)
    - ampm 완전 제거
    """
    out: List[Dict] = []
    # 섹터 결정: 입력 인자 > 환경변수 > 기본값(5,4,8)
    if sectors is None or len(sectors) == 0:
        env = os.environ.get("GPANG_SECTORS", "1,4,5,8,16")
        try:
            sectors = [int(x.strip()) for x in env.split(",") if x.strip()]
        except Exception:
            sectors = [1,4,5,8,16]
    else:
        sectors = [s for s in sectors if s in (1,4,5,8,16)]

    with _make_session() as s:
        # 수집 대상 구장(즐겨찾기/섹터 필터 적용)
        targets_all: List[Dict] = []
        for club in GOLF_CLUBS:
            name = club.get("name")
            gp_name = str(club.get("Golpang_code", "")).strip()
            if not name or not gp_name: continue
            if not _fav_ok(name, favorite): continue
            sector_guess = _sector_from_address(club.get("address"))
            targets_all.append({"name": name, "gp": gp_name, "sector": sector_guess})

        for sector in sectors:
            # 섹터별 대상(주소에서 추정된 섹터가 동일한 구장만 우선 매칭)
            targets = [t for t in targets_all if t["sector"] == sector or t["sector"] is None]

            # 섹터 컨텍스트로 부트스트랩
            _bootstrap_gp_session(s, date_str, sector)
            print(f"[{_fmt_ts()}] [Golfpang] ▶ START sector={sector} date={date_str}", flush=True)

            seen = set()  # (golf, date, hour_num, price)
            page = 1
            while True:
                form = {
                    "pageNum": page,
                    "rd_date": date_str,
                    "sector": sector,     # ★ 섹터 지정
                    "clubname": "",       # ★ 전체
                    "bkOrder": "", "idx": "", "cust_nick": "",
                    "sector2": "", "sector3": "", "cdOrder": "",
                }
                r = s.post(TBLLIST_URL, data=form, headers=AJAX_HEADERS,
                           timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False)
                status = r.status_code
                ctype = r.headers.get("Content-Type", "")
                print(f"[{_fmt_ts()}] [Golfpang]  sector={sector} page={page} status={status} ctype='{ctype}'", flush=True)

                if status >= 500 or _is_maintenance_html(r.text):
                    print(f"[{_fmt_ts()}] [Golfpang]   retry bootstrap (500/maintenance)", flush=True)
                    _bootstrap_gp_session(s, date_str, sector)
                    r = s.post(TBLLIST_URL, data=form, headers=AJAX_HEADERS,
                               timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False)
                    status = r.status_code
                    ctype = r.headers.get("Content-Type", "")
                    print(f"[{_fmt_ts()}] [Golfpang]   sector={sector} page={page} retry status={status} ctype='{ctype}'", flush=True)

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

                    # 이름 매칭
                    matched: Optional[Dict] = None
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
                    price = _parse_price(price_txt) if price_txt else 10**12

                    hour_label, hour_num = _normalize_time_to_hour_num(time_txt)
                    if hour_num < 0:
                        continue

                    key = (matched["name"], date_str, hour_num, price)
                    if key in seen:
                        continue
                    seen.add(key)

                    out.append({
                        "golf": matched["name"],
                        "date": date_str,
                        "hour": hour_label,
                        "hour_num": hour_num,
                        "price": price,
                        "benefit": "",
                        "time": time_txt,
                        "url": GOLFPANG_BASE + "/",
                        "source": "golfpang",
                    })
                    added_this_page += 1

                print(f"[{_fmt_ts()}] [Golfpang]  ◀ sector={sector} page={page} added={added_this_page}", flush=True)

                # 수정: 해당 페이지에 매칭된 항목이 없어도(added_this_page == 0),
                #       실제 데이터 행(rows)이 존재하면 다음 페이지에 우리가 원하는 데이터가 있을 수 있음.
                #       따라서 rows가 아예 없거나, 페이지가 너무 많아질 때만 종료.
                if not rows:
                    print(f"[{_fmt_ts()}] [Golfpang]  ⏹ No more rows. Stop sector={sector}", flush=True)
                    break
                
                if page >= 50:  # 안전장치: 최대 50페이지까지만
                    print(f"[{_fmt_ts()}] [Golfpang]  ⏹ Max page reached. Stop sector={sector}", flush=True)
                    break

                page += 1
                _time.sleep(0.05)  # 페이지 간 예의상 짧은 딜레이

            print(f"[{_fmt_ts()}] [Golfpang] ◀ DONE sector={sector} total_added={len([x for x in out if _sector_from_address(next((c['address'] for c in GOLF_CLUBS if c.get('name')==x['golf']), '' )) in (sector,None)])}", flush=True)
            _time.sleep(SLEEP_BETWEEN)

    out.sort(key=lambda x: (x.get("date",""), x.get("hour_num", 99), x.get("golf",""), x.get("price", 1<<60)))
    return out
