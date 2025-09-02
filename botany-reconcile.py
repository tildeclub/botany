#!/usr/bin/env python3

from __future__ import annotations
import argparse, os, sys, glob, re, time, json, sqlite3, shutil, traceback, datetime
from typing import Optional, Tuple, Dict, Any

def env(key: str, default: Optional[str]=None) -> Optional[str]:
    v = os.environ.get(key)
    return v if (v is not None and v != "") else default

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reconcile Botany state for all users")
    p.add_argument("--dry-run", action="store_true", help="Do not write changes")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    p.add_argument("--user", help="Only reconcile this user")
    p.add_argument("--dead-after-hours", type=int, default=int(env("DEAD_AFTER_HOURS", "72")),
                   help="Hours after last_watered to consider dead in fallback path")
    p.add_argument("--home-prefix", default=env("BOTANY_HOME_PREFIX", "/home"),
                   help="Home directory root (default: /home)")
    p.add_argument("--db-path", default=env("BOTANY_DB_PATH", "/srv/botany/sqlite/garden_db.sqlite"),
                   help="Shared garden SQLite DB path")
    p.add_argument("--reinit-corrupt", action="store_true",
                   help="On corrupted/missing pickle, backup (if present) and create a fresh plant")
    p.add_argument("--audit", action="store_true",
                   help="Also audit what /~user/myplant would show and compare to actual Plant state")
    p.add_argument("--water-interval-hours", type=int,
                   default=int(env("WATER_INTERVAL_HOURS", "18")),
                   help="Used to compute page 'thirsty' like index.php (default 18)")
    return p.parse_args()

def log(msg: str) -> None:
    print(msg, flush=True)

def vlog(args: argparse.Namespace, msg: str) -> None:
    if args.verbose:
        log(msg)

SAFE_USER_RE = re.compile(r"^[A-Za-z0-9._-]+$")

def is_safe_username(u: str) -> bool:
    return bool(SAFE_USER_RE.match(u))

def list_users_with_plant(home_prefix: str, only_user: Optional[str]) -> list[str]:
    if only_user:
        return [only_user]
    users: set[str] = set()
    for dat in glob.glob(os.path.join(home_prefix, "*", ".botany", "*_plant.dat")):
        user = os.path.basename(os.path.dirname(os.path.dirname(dat)))
        if is_safe_username(user):
            users.add(user)
    return sorted(users)

def read_user_json(home_prefix: str, user: str) -> Optional[dict]:
    jp = f"{home_prefix}/{user}/.botany/{user}_plant_data.json"
    if not os.path.isfile(jp) or not os.access(jp, os.R_OK):
        return None
    try:
        with open(jp, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def write_user_json(args: argparse.Namespace, user: str, data: dict) -> bool:
    if args.dry_run:
        return False
    jp = f"{args.home_prefix}/{user}/.botany/{user}_plant_data.json"
    os.makedirs(os.path.dirname(jp), exist_ok=True)
    tmp = jp + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, jp)
    return True

def update_garden_db_is_dead(args: argparse.Namespace, user: str, is_dead: int) -> None:
    if args.dry_run:
        return
    if not os.path.exists(args.db_path):
        return
    try:
        con = sqlite3.connect(args.db_path)
        cur = con.cursor()
        cur.execute("UPDATE garden SET is_dead = ? WHERE owner = ?", (int(is_dead), user))
        con.commit()
    except Exception:
        pass
    finally:
        try:
            con.close()
        except Exception:
            pass

def backup_file(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    ts = time.strftime("%Y%m%d%H%M%S")
    dest = f"{path}.corrupt-{ts}"
    shutil.copy2(path, dest)
    return dest

def find_user_dat_path(home_prefix: str, user: str) -> Optional[str]:
    """Find the actual *_plant.dat file for this user (filename may not match <user>_plant.dat)."""
    bdir = f"{home_prefix}/{user}/.botany"
    candidates = glob.glob(os.path.join(bdir, "*_plant.dat"))
    if not candidates:
        return None
    canonical = os.path.join(bdir, f"{user}_plant.dat")
    if canonical in candidates:
        return canonical
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]

def fmt_time_ago(ts: Optional[int]) -> str:
    if not ts:
        return "unknown"
    delta = max(0, int(time.time()) - int(ts))
    d, r = divmod(delta, 86400)
    h, r = divmod(r, 3600)
    m, _ = divmod(r, 60)
    return (f"{d}d " if d else "") + f"{h}h {m}m ago"

try:
    from botany import DataManager, Plant  
except Exception as e:
    log(f"ERROR: cannot import botany.py/Plant: {e}")
    sys.exit(1)

def make_dm_for_user(args: argparse.Namespace, user: str, dat_path: Optional[str]) -> DataManager:
    dm = DataManager()
    botany_dir = f"{args.home_prefix}/{user}/.botany"
    dm.this_user = user
    dm.botany_dir = botany_dir
    if dat_path:
        dm.savefile_name = os.path.basename(dat_path)
        dm.savefile_path = dat_path
    else:
        dm.savefile_name = f"{user}_plant.dat"
        dm.savefile_path = f"{botany_dir}/{user}_plant.dat"
    dm.garden_db_path = args.db_path
    dm.harvest_file_path = f"{botany_dir}/harvest_file.dat"
    dm.harvest_json_path = f"{botany_dir}/harvest_file.json"
    return dm

def reconcile_normal(args: argparse.Namespace, user: str) -> Tuple[bool,str]:
    dat_path = find_user_dat_path(args.home_prefix, user)
    if not dat_path:
        raise FileNotFoundError(f"no *_plant.dat under {args.home_prefix}/{user}/.botany/")
    dm = make_dm_for_user(args, user, dat_path)
    plant = dm.load_plant()  
    before = (int(getattr(plant, "dead", 0)),
              int(getattr(plant, "watered_timestamp", 0) or 0),
              float(getattr(plant, "ticks", 0.0)))
    if not args.dry_run:
        dm.save_plant(plant)
        dm.data_write_json(plant)
        dm.update_garden_db(plant)
    after = (int(getattr(plant, "dead", 0)),
             int(getattr(plant, "watered_timestamp", 0) or 0),
             float(getattr(plant, "ticks", 0.0)))
    changed = (before != after)
    return changed, f"dead {before[0]}→{after[0]}, last_water {before[1]}→{after[1]}, ticks {before[2]}→{after[2]}"

def fallback_mark_dead(args: argparse.Namespace, user: str) -> str:
    data = read_user_json(args.home_prefix, user)
    if not data:
        return "no JSON; skipped"
    last = int(data.get("last_watered") or 0)
    if last <= 0:
        return "JSON missing last_watered; skipped"
    hours_since = (time.time() - last) / 3600.0
    if hours_since > args.dead_after_hours:
        if int(data.get("is_dead", 0)) != 1:
            data["is_dead"] = 1
            write_user_json(args, user, data)
            update_garden_db_is_dead(args, user, 1)
            return f"marked dead via JSON/DB (hours_since={hours_since:.1f})"
        else:
            return "already dead in JSON"
    return f"still within threshold ({hours_since:.1f}h)"

def reinit_corrupt(args: argparse.Namespace, user: str) -> str:
    dat_path = find_user_dat_path(args.home_prefix, user)
    dm = make_dm_for_user(args, user, dat_path)
    if not args.dry_run:
        if dat_path and os.path.exists(dat_path):
            b = backup_file(dat_path)
        else:
            b = None
        try:
            plant = Plant(dm.savefile_path)
            dm.save_plant(plant)
            dm.data_write_json(plant)
            dm.update_garden_db(plant)
        except Exception as e:
            return f"reinit failed: {e}"
        return f"reinitialized (backup={b})"
    else:
        return "DRY-RUN would reinitialize and backup"

def read_db_row(db_path: str, user: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(db_path):
        return None
    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT owner, description, age, score, is_dead FROM garden WHERE owner = ? ORDER BY rowid DESC LIMIT 1", (user,))
        row = cur.fetchone()
        con.close()
        if not row:
            return None
        return dict(row)
    except Exception:
        return None

def compute_page_view(args: argparse.Namespace, user: str) -> Dict[str, Any]:
    """
    Emulate index.php:
      - Prefer per-user JSON if present/readable.
      - Else fallback to SQLite (DB).
      - Alive iff is_dead == 0.
      - Thirsty iff (now - last_watered) >= WATER_INTERVAL_HOURS*3600 (JSON only).
    """
    page: Dict[str, Any] = {
        "source": "none",
        "is_dead": None,
        "alive": None,
        "thirsty": None,
        "age": None,
        "score": None,
        "last_watered": None,
    }
    js = read_user_json(args.home_prefix, user)
    if js:
        page["source"] = "json"
        page["is_dead"] = int(js.get("is_dead", 0))  
        page["alive"] = (page["is_dead"] == 0)
        lw = js.get("last_watered")
        page["last_watered"] = int(lw) if isinstance(lw, (int, float, str)) and str(lw).isdigit() else None
        page["thirsty"] = (page["last_watered"] is not None) and ((time.time() - page["last_watered"]) >= args.water_interval_hours * 3600)
        page["age"] = js.get("age")
        page["score"] = int(js.get("score", 0))
        return page

    db = read_db_row(args.db_path, user)
    if db:
        page["source"] = "db"
        page["is_dead"] = int(db.get("is_dead", 0))
        page["alive"] = (page["is_dead"] == 0)
        page["age"] = db.get("age")
        try:
            page["score"] = int(db.get("score", 0))
        except Exception:
            page["score"] = 0
        page["last_watered"] = None
        page["thirsty"] = None
    return page

def load_plant_state(args: argparse.Namespace, user: str) -> Dict[str, Any]:
    """
    What Botany says *now* if we load the plant (runs dead_check/water_check).
    Returns keys: ok(bool), dead(int or None), last_watered(int or None), error(str or None)
    """
    out = {"ok": False, "dead": None, "last_watered": None, "error": None}
    try:
        dat_path = find_user_dat_path(args.home_prefix, user)
        if not dat_path:
            raise FileNotFoundError("no *_plant.dat")
        dm = make_dm_for_user(args, user, dat_path)
        plant = dm.load_plant()
        out["ok"] = True
        out["dead"] = int(getattr(plant, "dead", 0))
        out["last_watered"] = int(getattr(plant, "watered_timestamp", 0) or 0)
        return out
    except Exception as e:
        out["error"] = f"{e.__class__.__name__}: {e}"
        return out

def audit_user(args: argparse.Namespace, user: str) -> Dict[str, Any]:
    page = compute_page_view(args, user)
    plant = load_plant_state(args, user)

    mismatch_alive = None
    if page["alive"] is not None and plant["dead"] is not None:
        mismatch_alive = (page["alive"] == (plant["dead"] == 0)) is False

    return {
        "user": user,
        "page_source": page["source"],
        "page_alive": page["alive"],
        "page_is_dead": page["is_dead"],
        "page_last_watered": page["last_watered"],
        "page_last_watered_ago": fmt_time_ago(page["last_watered"]) if page["last_watered"] else None,
        "page_thirsty": page["thirsty"],
        "plant_ok": plant["ok"],
        "plant_dead": plant["dead"],
        "plant_last_watered": plant["last_watered"],
        "plant_last_watered_ago": fmt_time_ago(plant["last_watered"]) if plant["last_watered"] else None,
        "mismatch_alive": mismatch_alive,
        "plant_error": plant["error"],
    }

def print_audit_table(rows: list[Dict[str, Any]], water_hours: int) -> None:
    def yn(v):
        return "yes" if v else ("no" if v is not None else "—")
    print(f"\nAudit of myplant rendering (WATER_INTERVAL_HOURS={water_hours}h)")
    print(f"{'user':<20} {'src':<5} {'page_alive':<11} {'plant_alive':<12} {'thirsty':<8} {'page_last_watered':<18} note")
    for r in rows:
        page_alive = r["page_alive"]
        plant_alive = (r["plant_dead"] == 0) if r["plant_dead"] is not None else None
        note = ""
        if r["mismatch_alive"]:
            note = "MISMATCH: page vs plant"
        elif not r["plant_ok"]:
            note = f"plant load error: {r['plant_error']}"
        print(f"{r['user']:<20} {r['page_source']:<5} {yn(page_alive):<11} {yn(plant_alive):<12} {yn(r['page_thirsty']):<8} {(r['page_last_watered_ago'] or '—'):<18} {note}")
    print()

def main() -> int:
    args = parse_args()
    home_prefix = args.home_prefix.rstrip("/")
    users = list_users_with_plant(home_prefix, args.user)

    if args.user and args.user not in users and is_safe_username(args.user):
        users.append(args.user)

    if not users:
        log("No users with plants found.")
        return 0

    log(f"Reconciling {len(users)} users (dry_run={args.dry_run}, threshold={args.dead_after_hours}h)")
    fixed = 0
    errors = 0
    for u in sorted(users):
        if not is_safe_username(u):
            vlog(args, f"skip unsafe username: {u}")
            continue
        try:
            changed, msg = reconcile_normal(args, u)
            status = "OK"
            if changed:
                fixed += 1
                status = "UPDATED" if not args.dry_run else "DRY-RUN"
            vlog(args, f"[{status}] {u}: {msg}")
        except (EOFError, FileNotFoundError, ValueError, ModuleNotFoundError, ImportError) as e:
            reason = f"{e.__class__.__name__}: {e}"
            if args.reinit_corrupt:
                action = reinit_corrupt(args, u)
            else:
                action = fallback_mark_dead(args, u)
            log(f"[CORRUPT/MISSING] {u}: {reason} -> {action}")
            if "reinitialized" in action or "marked dead" in action:
                fixed += 1
        except Exception as e:
            errors += 1
            log(f"[ERROR] {u}: {e.__class__.__name__}: {e}")
            if args.verbose:
                traceback.print_exc()

    log(f"Done. processed={len(users)} fixed={fixed} errors={errors} dry_run={args.dry_run}")

    if args.audit:
        rows = [audit_user(args, u) for u in sorted(users)]
        print_audit_table(rows, args.water_interval_hours)

        if any(r.get("mismatch_alive") for r in rows):
            return 2

    return 0

if __name__ == "__main__":
    sys.exit(main())
