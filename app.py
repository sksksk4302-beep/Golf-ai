
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from datetime import datetime, timedelta
import threading, os, inspect
from collections import defaultdict

from crawler_utils import crawl_teescan, crawl_golfpang, GOLF_CLUBS

# ─────────────────────────────────────────────────────────────────────────────
# (옵션) IPv6 경로 문제 우회: FORCE_IPV4=1 이면 IPv4만 사용
try:
    if os.environ.get("FORCE_IPV4") == "1":
        import socket
        import urllib3.util.connection as urllib3_cn
        urllib3_cn.allowed_gai_family = lambda: socket.AF_INET
        print("🔧 IPv4-only mode enabled (FORCE_IPV4=1)")
except Exception as e:
    print("⚠️ IPv4-only 설정 실패:", e)
# ─────────────────────────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app)

# ─────────────────────────────────────────────────────────────────────────────
# 설정 (환경변수/엔드포인트로 변경 가능)
MAX_DAYS = int(os.environ.get("MAX_DAYS", 18))                # 최대 수집일자 (기본 18일)
REFRESH_INTERVAL_SEC = int(os.environ.get("REFRESH_INTERVAL_SEC", 3600))  # 수집 주기(초) 기본 1시간

# 고정 섹터: 5(경기), 4(충청), 8(강원) — crawler_utils가 내부적으로 사용
GOLFPANG_SECTORS = [5, 4, 8]

# 캐시 & 동기화
MEMORY_CACHE = {}              # { "YYYY-MM-DD": [ items... ] }
CACHE_LOCK   = threading.Lock()

# 루프 제어
_loop_wakeup_event = threading.Event()
_loop_thread = None

# crawl_golfpang 시그니처 유연 호환
def _call_crawl_golfpang(date_str: str, favorite: list, sectors: list):
    try:
        sig = inspect.signature(crawl_golfpang)
        if 'sectors' in sig.parameters:
            return crawl_golfpang(date_str, favorite=favorite, sectors=sectors)
        return crawl_golfpang(date_str, favorite=favorite)
    except TypeError:
        return crawl_golfpang(date_str, favorite=favorite)

# ─────────────────────────────────────────────────────────────────────────────
# Golfpang 회로 차단기(circuit breaker)
from datetime import datetime as _dt, timedelta as _td
GOLFPANG_CB = {
    "fails": 0,
    "open_until": None,   # datetime or None
    "THRESH": int(os.environ.get("GOLFPANG_CB_THRESH", 5)),      # 연속 실패 임계
    "COOL_MIN": int(os.environ.get("GOLFPANG_CB_COOL_MIN", 5)),  # 쿨다운(분)
}

def _golfpang_allowed_now():
    now = _dt.now()
    if GOLFPANG_CB["open_until"] and now < GOLFPANG_CB["open_until"]:
        return False
    return True

def _golfpang_on_success():
    GOLFPANG_CB["fails"] = 0
    GOLFPANG_CB["open_until"] = None

def _golfpang_on_failure():
    GOLFPANG_CB["fails"] += 1
    if GOLFPANG_CB["fails"] >= GOLFPANG_CB["THRESH"]:
        cool = _td(minutes=GOLFPANG_CB["COOL_MIN"])
        GOLFPANG_CB["open_until"] = _dt.now() + cool
        print(f"🧯 Golfpang 회로 열림: {GOLFPANG_CB['COOL_MIN']}분 동안 스킵 (연속실패={GOLFPANG_CB['fails']})")

# ─────────────────────────────────────────────────────────────────────────────
# 로깅 유틸
def _banner(msg):
    line = "─" * max(40, min(80, len(msg) + 8))
    print(f"\n{line}\n{msg}\n{line}", flush=True)

def _fmt_ts():
    return _dt.now().strftime("%H:%M:%S")

def _log_date_summary(date_str, teescan_items, golfpang_items):
    print(f"[{_fmt_ts()}] 📊 {date_str} 수집 완료 — Teescan:{len(teescan_items)}개 / Golfpang:{len(golfpang_items)}개 / 합계:{len(teescan_items) + len(golfpang_items)}개", flush=True)

def _log_per_club_breakdown(date_str, teescan_items, golfpang_items):
    # club -> {"T": count, "G": count}
    per = defaultdict(lambda: {"T": 0, "G": 0})
    for it in teescan_items:
        per[it.get("golf","")]["T"] += 1
    for it in golfpang_items:
        per[it.get("golf","")]["G"] += 1

    if not per:
        print(f"[{_fmt_ts()}] • {date_str} 구장별 집계: (수집 항목 없음)")
        return

    print(f"[{_fmt_ts()}] • {date_str} 구장별 집계:")
    names = sorted(per.keys())
    line_chunks = []
    for name in names:
        t = per[name]["T"]; g = per[name]["G"]; tot = t + g
        line_chunks.append(f"{name} T={t} / G={g} / 합={tot}")
        if len(line_chunks) >= 4:  # 줄바꿈 가독성
            print("   - " + "  |  ".join(line_chunks))
            line_chunks = []
    if line_chunks:
        print("   - " + "  |  ".join(line_chunks))

# ─────────────────────────────────────────────────────────────────────────────
# 캐시 갱신 루틴
def _refresh_one_date(date_str: str, favorite=None):
    favorite = favorite or []
    teescan_items = []
    golfpang_items = []
    # Teescan
    try:
        teescan_items = crawl_teescan(date_str, favorite=favorite)
    except Exception as e_ts:
        print(f"[{_fmt_ts()}] ❗️ {date_str} Teescan 실패: {e_ts}")
    # Golfpang
    if _golfpang_allowed_now():
        try:
            golfpang_items = _call_crawl_golfpang(date_str, favorite=favorite, sectors=GOLFPANG_SECTORS)
            _golfpang_on_success()
        except Exception as e_gp:
            print(f"[{_fmt_ts()}] ❗️ {date_str} Golfpang 실패: {e_gp}")
            _golfpang_on_failure()
    else:
        left = int((GOLFPANG_CB["open_until"] - _dt.now()).total_seconds())
        print(f"[{_fmt_ts()}] ⏸️ {date_str} Golfpang 스킵(회로 열림, {left}s 남음)")
        golfpang_items = []

    items = teescan_items + golfpang_items

    # 상세 로그 (날짜 요약 + 구장별 breakdown)
    _log_date_summary(date_str, teescan_items, golfpang_items)
    _log_per_club_breakdown(date_str, teescan_items, golfpang_items)

    got_lock = CACHE_LOCK.acquire(timeout=5)
    if got_lock:
        try:
            MEMORY_CACHE[date_str] = items
            print(f"[{_fmt_ts()}] ✅ {date_str} 캐시 저장 완료 ({len(items)}건)")
        finally:
            CACHE_LOCK.release()
    else:
        print(f"[{_fmt_ts()}] ⛔️ {date_str} 캐시 갱신 실패 - 락 획득 실패")

def full_refresh_cache():
    today = _dt.now().date()
    total = 0
    updated_days = 0
    _banner(f"자동 갱신 시작 (MAX_DAYS={MAX_DAYS}, INTERVAL={REFRESH_INTERVAL_SEC}s)")
    for i in range(MAX_DAYS):
        date_str = (today + timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            _refresh_one_date(date_str)
            total += len(MEMORY_CACHE.get(date_str, []))
            updated_days += 1
        except Exception as e:
            print(f"[{_fmt_ts()}] ❌ {date_str} 전체 루프 실패: {e}")

    # 전체 요약
    keys = list(MEMORY_CACHE.keys())
    print(f"[{_fmt_ts()}] 🧠 전체 캐시 갱신 완료 — 대상일수:{updated_days}일 / 누적아이템:{total}건 / keys:{keys}")

def _refresh_loop():
    while True:
        try:
            full_refresh_cache()
        except Exception as e:
            print(f"[{_fmt_ts()}] ❌ 자동 갱신 루프 오류: {e}")
        # 대기 (중간에 설정 변경 시 즉시 깨어남)
        _loop_wakeup_event.wait(timeout=REFRESH_INTERVAL_SEC)
        _loop_wakeup_event.clear()

def start_refresh_loop_once():
    global _loop_thread
    if _loop_thread and _loop_thread.is_alive():
        return
    _loop_thread = threading.Thread(target=_refresh_loop, daemon=True)
    _loop_thread.start()

# ─────────────────────────────────────────────────────────────────────────────
# 유틸
def get_from_cache(date_str, favorite):
    got_lock = CACHE_LOCK.acquire(timeout=3)
    if not got_lock:
        print(f"[{_fmt_ts()}] ⛔️ {date_str} 캐시 잠금 획득 실패 - 다른 작업 중")
        return []
    try:
        base = MEMORY_CACHE.get(date_str, [])
        filtered = [item for item in base if not favorite or item["golf"] in favorite]
        return filtered
    finally:
        CACHE_LOCK.release()

def get_consolidated_teetime(start_dt, end_dt, hour_range, favorite):
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    result = []
    cur = start_dt.date()
    end = end_dt.date()

    while cur <= end:
        date_str = cur.strftime("%Y-%m-%d")
        items = get_from_cache(date_str, favorite)

        if not items:
            try:
                _refresh_one_date(date_str, favorite=favorite)
                items = get_from_cache(date_str, favorite)
            except Exception as e:
                print(f"[{_fmt_ts()}] ⚠️ on-demand 갱신 실패({date_str}): {e}")
                items = []

        if hour_range:
            hr_set = set(int(h) for h in hour_range)
            items = [it for it in items if int(it.get("hour_num", -1)) in hr_set]

        result.extend(items)
        cur += timedelta(days=1)

    result.sort(key=lambda x: (x.get("date",""), x.get("hour_num", 99), x.get("golf",""), x.get("price", 1<<60)))
    return result

# ─────────────────────────────────────────────────────────────────────────────
# 라우트
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/get_all_golfclubs")
def get_all_golfclubs():
    names = sorted(c.get("name","") for c in GOLF_CLUBS if c.get("name"))
    return jsonify(names)

@app.route("/get_ttime_grouped", methods=["POST"])
def get_grouped_teetime():
    try:
        data = request.get_json(force=True)
        start = datetime.strptime(data["start_date"], "%Y-%m-%d")
        end   = datetime.strptime(data["end_date"], "%Y-%m-%d")
        hour_range = data.get("hour_range")
        favorite   = data.get("favorite_clubs", [])
        return jsonify(get_consolidated_teetime(start, end, hour_range, favorite))
    except Exception as e:
        print(f"[{_fmt_ts()}] ❌ API 오류: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/get_ttime_grouped", methods=["GET"])
def get_grouped_teetime_gpt():
    start_str = request.args.get("start_date")
    end_str   = request.args.get("end_date")
    if not start_str or not end_str:
        return jsonify({"error": "Missing start_date or end_date"}), 400
    try:
        start = datetime.strptime(start_str, "%Y-%m-%d")
        end   = datetime.strptime(end_str, "%Y-%m-%d")
    except Exception as e:
        return jsonify({"error": f"Invalid date format: {e}"}), 400
    return jsonify(get_consolidated_teetime(start, end, None, []))

@app.route("/admin/refresh", methods=["POST"])
def admin_refresh():
    def _task():
        print(f"[{_fmt_ts()}] 🔧 수동 캐시 갱신 요청 수신됨")
        full_refresh_cache()
    threading.Thread(target=_task, daemon=True).start()
    _loop_wakeup_event.set()
    return jsonify({"status": "refresh_started"})

@app.route("/admin/config", methods=["GET", "POST"])
def admin_config():
    global MAX_DAYS, REFRESH_INTERVAL_SEC
    if request.method == "GET":
        return jsonify({
            "max_days": MAX_DAYS,
            "refresh_interval_sec": REFRESH_INTERVAL_SEC,
            "golfpang_cb": {
                "fails": GOLFPANG_CB["fails"],
                "open_until": GOLFPANG_CB["open_until"].isoformat() if GOLFPANG_CB["open_until"] else None,
                "threshold": GOLFPANG_CB["THRESH"],
                "cool_min": GOLFPANG_CB["COOL_MIN"],
            },
        })
    else:
        data = request.get_json(force=True) if request.data else {}
        if "max_days" in data:
            try:
                MAX_DAYS = max(1, int(data["max_days"]))
            except Exception:
                return jsonify({"error": "invalid max_days"}), 400
        if "interval_sec" in data:
            try:
                REFRESH_INTERVAL_SEC = max(60, int(data["interval_sec"]))  # 최소 60초
                _loop_wakeup_event.set()
            except Exception:
                return jsonify({"error": "invalid interval_sec"}), 400
        return jsonify({"ok": True, "max_days": MAX_DAYS, "refresh_interval_sec": REFRESH_INTERVAL_SEC})

# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 서버 시작 시 자동 갱신 루프 기동 + 첫 라운드 즉시
    start_refresh_loop_once()
    _loop_wakeup_event.set()  # 첫 full_refresh_cache를 즉시 수행
    port = int(os.environ.get("PORT", 5000))
    print(f"🌐 Flask 서버 실행 시작: 포트 {port}")
    app.run(host="0.0.0.0", port=port)
