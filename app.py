from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from itertools import combinations



DB_PATH = Path("tt.db")

app = FastAPI()
templates = Jinja2Templates(directory="templates")


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)


def init_db() -> None:
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            p1_id INTEGER NOT NULL,
            p2_id INTEGER NOT NULL,
            p1_sets INTEGER NOT NULL,
            p2_sets INTEGER NOT NULL,
            played_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(p1_id, p2_id),
            FOREIGN KEY(p1_id) REFERENCES players(id),
            FOREIGN KEY(p2_id) REFERENCES players(id)
        )
    """)

    # Migracje: małe punkty + zapis setów
    if not _column_exists(conn, "matches", "p1_points"):
        cur.execute("ALTER TABLE matches ADD COLUMN p1_points INTEGER NOT NULL DEFAULT 0")
    if not _column_exists(conn, "matches", "p2_points"):
        cur.execute("ALTER TABLE matches ADD COLUMN p2_points INTEGER NOT NULL DEFAULT 0")
    if not _column_exists(conn, "matches", "sets_detail"):
        cur.execute("ALTER TABLE matches ADD COLUMN sets_detail TEXT NOT NULL DEFAULT ''")

    # NOWE: terminarz
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day_no INTEGER NOT NULL,
            order_in_day INTEGER NOT NULL,
            p1_id INTEGER NOT NULL,
            p2_id INTEGER NOT NULL,
            UNIQUE(p1_id, p2_id),
            FOREIGN KEY(p1_id) REFERENCES players(id),
            FOREIGN KEY(p2_id) REFERENCES players(id)
        )
    """)

    conn.commit()
    conn.close()


@app.on_event("startup")
def _startup():
    init_db()


def get_players(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute("SELECT id, name FROM players ORDER BY name COLLATE NOCASE").fetchall()


def get_matches(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute("""
        SELECT m.id, m.p1_id, p1.name AS p1_name, m.p2_id, p2.name AS p2_name,
               m.p1_sets, m.p2_sets, m.p1_points, m.p2_points, m.sets_detail, m.played_at
        FROM matches m
        JOIN players p1 ON p1.id = m.p1_id
        JOIN players p2 ON p2.id = m.p2_id
        ORDER BY m.played_at DESC
    """).fetchall()


def normalize_pair(p1_id: int, p2_id: int, payload: Dict) -> Tuple[int, int, Dict]:
    """
    Trzymamy w DB parę w ustalonej kolejności (mniejsze ID jako p1),
    żeby UNIQUE(p1_id, p2_id) działało niezależnie od kolejności wpisu.
    Jeśli zamieniamy zawodników, musimy też zamienić wynik (sety/punkty i sety_detail).
    """
    if p1_id < p2_id:
        return p1_id, p2_id, payload

    # swap payload for reversed players
    swapped = dict(payload)
    swapped["p1_sets"], swapped["p2_sets"] = payload["p2_sets"], payload["p1_sets"]
    swapped["p1_points"], swapped["p2_points"] = payload["p2_points"], payload["p1_points"]

    # sets_detail format: "11-7,8-11,11-9" => swap each "a-b" -> "b-a"
    detail = payload.get("sets_detail", "")
    if detail:
        parts = [p.strip() for p in detail.split(",") if p.strip()]
        swapped_parts = []
        for part in parts:
            if "-" in part:
                a, b = part.split("-", 1)
                swapped_parts.append(f"{b.strip()}-{a.strip()}")
            else:
                swapped_parts.append(part)
        swapped["sets_detail"] = ",".join(swapped_parts)

    return p2_id, p1_id, swapped


def parse_sets_best_of_3(
    s1a: Optional[int], s1b: Optional[int],
    s2a: Optional[int], s2b: Optional[int],
    s3a: Optional[int], s3b: Optional[int],
) -> Optional[Dict]:
    """
    Z wejścia (małe punkty per set) buduje:
    - p1_sets / p2_sets (do 2 wygranych)
    - p1_points / p2_points (suma małych punktów)
    - sets_detail (np. "11-7,8-11,11-9")
    Zwraca None jeśli dane są niepoprawne.
    """
    raw = [(s1a, s1b), (s2a, s2b), (s3a, s3b)]

    sets = []
    for a, b in raw:
        # set jest wpisany tylko jeśli oba pola są podane
        if a is None and b is None:
            continue
        if a is None or b is None:
            return None
        if a < 0 or b < 0:
            return None
        if a == b:
            return None
        sets.append((a, b))

    # musi być co najmniej 2 sety, max 3
    if len(sets) < 2 or len(sets) > 3:
        return None

    p1_sets = 0
    p2_sets = 0
    p1_points = 0
    p2_points = 0
    detail_parts = []

    for idx, (a, b) in enumerate(sets, start=1):
        p1_points += a
        p2_points += b
        detail_parts.append(f"{a}-{b}")

        if a > b:
            p1_sets += 1
        else:
            p2_sets += 1

        # best-of-3: kończymy, gdy ktoś ma 2 wygrane sety
        if p1_sets == 2 or p2_sets == 2:
            # jeśli ktoś wygrał 2 sety przed 3. setem, a user wpisał jednak 3 set — odrzucamy
            if idx < len(sets):
                # tu jeszcze przetwarzamy kolejne sety, więc sprawdzimy po pętli
                pass

    # warunek: ktoś musi mieć dokładnie 2 wygrane sety
    if not (p1_sets == 2 or p2_sets == 2):
        return None

    # dodatkowa walidacja: jeśli mecz skończył się 2:0, nie powinno być 3 setów
    if (p1_sets == 2 and p2_sets == 0) or (p2_sets == 2 and p1_sets == 0):
        if len(sets) != 2:
            return None
    # jeśli 2:1, muszą być 3 sety
    if (p1_sets == 2 and p2_sets == 1) or (p2_sets == 2 and p1_sets == 1):
        if len(sets) != 3:
            return None

    return {
        "p1_sets": p1_sets,
        "p2_sets": p2_sets,
        "p1_points": p1_points,
        "p2_points": p2_points,
        "sets_detail": ",".join(detail_parts),
    }


def compute_table(players: List[sqlite3.Row], matches: List[sqlite3.Row]) -> List[Dict]:
    stats: Dict[int, Dict] = {}
    for p in players:
        stats[p["id"]] = {
            "id": p["id"],
            "name": p["name"],
            "played": 0,
            "wins": 0,
            "losses": 0,
            "points": 0,  # 2 za wygraną, 0 za porażkę
            "sets_for": 0,
            "sets_against": 0,
            "pts_for": 0,      # małe punkty
            "pts_against": 0,
        }

    h2h: Dict[Tuple[int, int], int] = {}  # (a,b)->1 jeśli a wygrał z b, -1 jeśli przegrał

    for m in matches:
        a = m["p1_id"]
        b = m["p2_id"]

        if a not in stats or b not in stats:
            continue

        a_sets = m["p1_sets"]
        b_sets = m["p2_sets"]
        a_pts = m["p1_points"]
        b_pts = m["p2_points"]

        stats[a]["played"] += 1
        stats[b]["played"] += 1

        stats[a]["sets_for"] += a_sets
        stats[a]["sets_against"] += b_sets
        stats[b]["sets_for"] += b_sets
        stats[b]["sets_against"] += a_sets

        stats[a]["pts_for"] += a_pts
        stats[a]["pts_against"] += b_pts
        stats[b]["pts_for"] += b_pts
        stats[b]["pts_against"] += a_pts

        if a_sets > b_sets:
            stats[a]["wins"] += 1
            stats[b]["losses"] += 1
            stats[a]["points"] += 2
            h2h[(a, b)] = 1
            h2h[(b, a)] = -1
        else:
            stats[b]["wins"] += 1
            stats[a]["losses"] += 1
            stats[b]["points"] += 2
            h2h[(a, b)] = -1
            h2h[(b, a)] = 1

    rows = []
    for s in stats.values():
        s["sets_diff"] = s["sets_for"] - s["sets_against"]
        s["pts_diff"] = s["pts_for"] - s["pts_against"]
        rows.append(s)

    # sort: punkty, wygrane, różnica setów, różnica małych punktów, małe punkty zdobyte
    def key_base(r: Dict):
        return (r["points"], r["wins"], r["sets_diff"], r["pts_diff"], r["pts_for"])

    rows.sort(key=key_base, reverse=True)

    # tie-break: jeśli DWIE osoby remisowe wg key_base => H2H
    i = 0
    while i < len(rows) - 1:
        j = i
        while j + 1 < len(rows) and key_base(rows[j]) == key_base(rows[j + 1]):
            j += 1

        if j > i:
            group = rows[i:j+1]
            if len(group) == 2:
                a_id = group[0]["id"]
                b_id = group[1]["id"]
                res = h2h.get((a_id, b_id))
                if res == -1:
                    rows[i], rows[i+1] = rows[i+1], rows[i]
            # dla remisów 3+ można zrobić „małą tabelę” między zainteresowanymi — dopiszę, jeśli chcesz.
        i = j + 1

    for idx, r in enumerate(rows, start=1):
        r["rank"] = idx
    return rows

def generate_schedule_fair(player_ids: List[int], days: int = 20, max_matches_per_day: int = 5):
    """
    Generuje terminarz na 'days' dni.
    Zasada: max 1 mecz / zawodnik / dzień.
    Heurystyka równego czekania: wybieramy pary, które najdłużej nie grały.
    """
    ids = sorted(player_ids)
    all_matches = {(a, b) for a, b in combinations(ids, 2)}
    scheduled = {d: [] for d in range(1, days + 1)}
    last_played = {p: 0 for p in ids}

    def idle(p, day):
        return day - last_played[p]

    for day in range(1, days + 1):
        used_today = set()

        remaining = len(all_matches)
        remaining_days = (days - day + 1)
        target = max(1, round(remaining / remaining_days))  # zwykle 2–3
        target = min(target, max_matches_per_day)

        while len(scheduled[day]) < target and all_matches:
            candidates = []
            for a, b in all_matches:
                if a in used_today or b in used_today:
                    continue
                score = idle(a, day) + idle(b, day)
                if last_played[a] == 0:
                    score += 1
                if last_played[b] == 0:
                    score += 1
                candidates.append((score, a, b))

            if not candidates:
                break

            candidates.sort(reverse=True, key=lambda x: x[0])
            _, a, b = candidates[0]

            scheduled[day].append((a, b))
            used_today.add(a)
            used_today.add(b)
            last_played[a] = day
            last_played[b] = day
            all_matches.remove((a, b))

    # awaryjnie: jeśli coś zostało, dogrywamy do wolnych slotów
    leftovers = list(all_matches)
    day = 1
    guard = 0
    while leftovers and guard < 100000:
        guard += 1
        used_today = {p for match in scheduled[day] for p in match}
        if len(scheduled[day]) < max_matches_per_day:
            a, b = leftovers[0]
            if a not in used_today and b not in used_today:
                scheduled[day].append((a, b))
                leftovers.pop(0)
            else:
                leftovers.append(leftovers.pop(0))
        day = day + 1 if day < days else 1

    return scheduled


def _detail_to_fields(detail: str):
    fields = {"s1a": "", "s1b": "", "s2a": "", "s2b": "", "s3a": "", "s3b": ""}
    if not detail:
        return fields
    parts = [p.strip() for p in detail.split(",") if p.strip()]
    for i, part in enumerate(parts[:3], start=1):
        if "-" not in part:
            continue
        a, b = part.split("-", 1)
        fields[f"s{i}a"] = a.strip()
        fields[f"s{i}b"] = b.strip()
    return fields


def get_schedule(conn: sqlite3.Connection):
    return conn.execute("""
        SELECT s.id, s.day_no, s.order_in_day,
               s.p1_id, p1.name AS p1_name,
               s.p2_id, p2.name AS p2_name
        FROM schedule s
        JOIN players p1 ON p1.id = s.p1_id
        JOIN players p2 ON p2.id = s.p2_id
        ORDER BY s.day_no ASC, s.order_in_day ASC
    """).fetchall()



def _detail_to_fields(detail: str):
    # detail: "11-7,8-11,11-9"
    fields = {"s1a": "", "s1b": "", "s2a": "", "s2b": "", "s3a": "", "s3b": ""}
    if not detail:
        return fields
    parts = [p.strip() for p in detail.split(",") if p.strip()]
    for i, part in enumerate(parts[:3], start=1):
        if "-" not in part:
            continue
        a, b = part.split("-", 1)
        fields[f"s{i}a"] = a.strip()
        fields[f"s{i}b"] = b.strip()
    return fields


@app.get("/", response_class=HTMLResponse)
def home(request: Request, lang: str = "pl"):
    lang = (lang or "pl").lower()
    if lang not in ("pl", "de"):
        lang = "pl"

    conn = db()
    players = get_players(conn)
    matches = get_matches(conn)
    table = compute_table(players, matches)

    # map rozegranych wyników po parze (min_id, max_id)
    played_map = {}
    for m in matches:
        a = min(m["p1_id"], m["p2_id"])
        b = max(m["p1_id"], m["p2_id"])
        played_map[(a, b)] = m

    schedule_rows = get_schedule(conn)

    # grupowanie po dniach
    schedule_days = []
    current_day = None
    current_list = []

    for r in schedule_rows:
        if current_day is None:
            current_day = r["day_no"]

        if r["day_no"] != current_day:
            schedule_days.append((current_day, current_list))
            current_day = r["day_no"]
            current_list = []

        a = r["p1_id"]
        b = r["p2_id"]
        m = played_map.get((min(a, b), max(a, b)))

        item = {
            "day_no": r["day_no"],
            "p1_id": a, "p2_id": b,
            "p1_name": r["p1_name"], "p2_name": r["p2_name"],
            "played": bool(m),
            "p1_sets": m["p1_sets"] if m else "",
            "p2_sets": m["p2_sets"] if m else "",
            "p1_points": m["p1_points"] if m else "",
            "p2_points": m["p2_points"] if m else "",
            "sets_detail": m["sets_detail"] if m else "",
        }
        item.update(_detail_to_fields(item["sets_detail"]))
        current_list.append(item)

    if current_day is not None:
        schedule_days.append((current_day, current_list))

    conn.close()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "players": players,
        "matches": matches,
        "table": table,
        "lang": lang,
        "schedule_days": schedule_days,
    })

@app.post("/schedule/generate")
def schedule_generate(days: int = Form(20)):
    if days < 1:
        days = 20
    if days > 60:
        days = 60

    conn = db()
    players = get_players(conn)
    ids = [p["id"] for p in players]

    # czyścimy poprzedni terminarz
    conn.execute("DELETE FROM schedule")

    # jeśli mniej niż 2 zawodników, nie ma co generować
    if len(ids) >= 2:
        max_matches_per_day = len(ids) // 2  # dla 11 => 5
        sched = generate_schedule_fair(ids, days=days, max_matches_per_day=max_matches_per_day)

        for day_no in range(1, days + 1):
            for order, (a, b) in enumerate(sched.get(day_no, []), start=1):
                # w schedule trzymamy parę w kolejności rosnącej (jak w matches)
                p1, p2 = (a, b) if a < b else (b, a)
                conn.execute("""
                    INSERT INTO schedule(day_no, order_in_day, p1_id, p2_id)
                    VALUES (?, ?, ?, ?)
                """, (day_no, order, p1, p2))

    conn.commit()
    conn.close()
    return RedirectResponse("/?lang=pl", status_code=303)


@app.post("/players/add")
def add_player(name: str = Form(...)):
    name = name.strip()
    if not name:
        return RedirectResponse("/", status_code=303)

    conn = db()
    try:
        conn.execute("INSERT INTO players(name) VALUES (?)", (name,))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()
    return RedirectResponse("/", status_code=303)


@app.post("/matches/add")
def add_match(
    p1_id: int = Form(...),
    p2_id: int = Form(...),
    s1a: Optional[int] = Form(None),
    s1b: Optional[int] = Form(None),
    s2a: Optional[int] = Form(None),
    s2b: Optional[int] = Form(None),
    s3a: Optional[int] = Form(None),
    s3b: Optional[int] = Form(None),
):
    if p1_id == p2_id:
        return RedirectResponse("/", status_code=303)

    payload = parse_sets_best_of_3(s1a, s1b, s2a, s2b, s3a, s3b)
    if payload is None:
        return RedirectResponse("/", status_code=303)

    a, b, normalized = normalize_pair(p1_id, p2_id, payload)

    conn = db()
    conn.execute("""
        INSERT INTO matches(p1_id, p2_id, p1_sets, p2_sets, p1_points, p2_points, sets_detail)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(p1_id, p2_id)
        DO UPDATE SET
            p1_sets=excluded.p1_sets,
            p2_sets=excluded.p2_sets,
            p1_points=excluded.p1_points,
            p2_points=excluded.p2_points,
            sets_detail=excluded.sets_detail,
            played_at=CURRENT_TIMESTAMP
    """, (a, b,
          normalized["p1_sets"], normalized["p2_sets"],
          normalized["p1_points"], normalized["p2_points"],
          normalized["sets_detail"]))
    conn.commit()
    conn.close()

    return RedirectResponse("/", status_code=303)


@app.post("/matches/delete")
def delete_match(match_id: int = Form(...)):
    conn = db()
    conn.execute("DELETE FROM matches WHERE id=?", (match_id,))
    conn.commit()
    conn.close()
    return RedirectResponse("/", status_code=303)


@app.post("/reset")
def reset_all():
    conn = db()
    conn.execute("DELETE FROM matches")
    conn.execute("DELETE FROM players")
    conn.commit()
    conn.close()
    return RedirectResponse("/", status_code=303)
