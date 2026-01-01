from decimal import Decimal
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2 import sql

from text_parser import parse_command

# Monat → Basis-Spaltenpräfix
GERMAN_MONTHS_TO_COL = {
    "januar": "jan",
    "jan": "jan",
    "februar": "feb",
    "feb": "feb",
    "märz": "mrz",
    "maerz": "mrz",
    "mrz": "mrz",
    "april": "apr",
    "apr": "apr",
    "mai": "mai",
    "juni": "jun",
    "jun": "jun",
    "juli": "jul",
    "jul": "jul",
    "august": "aug",
    "aug": "aug",
    "september": "sep",
    "sep": "sep",
    "oktober": "okt",
    "okt": "okt",
    "november": "nov",
    "nov": "nov",
    "dezember": "dez",
    "dez": "dez",
}

VALID_PLAN_YEARS = {2026, 2027, 2028, 2029, 2030, 2031}
MONTH_ORDER = ["jan", "feb", "mrz", "apr", "mai", "jun", "jul", "aug", "sep", "okt", "nov", "dez"]


def monthname_to_basecol(month_name: str) -> str:
    key = month_name.lower().strip()
    col = GERMAN_MONTHS_TO_COL.get(key)
    if not col:
        raise ValueError(f"Unbekannter Monat: {month_name}")
    return col


def month_col_for_year(month_name: str, year: int) -> str:
    """
    Aus 'Januar' + 2026 → 'jan_2026'
    """
    base = monthname_to_basecol(month_name)
    return f"{base}_{year}"


def month_index(month_name: str) -> int:
    base = monthname_to_basecol(month_name)
    return MONTH_ORDER.index(base)


def _validate_table_name(table_name: str) -> sql.Identifier:
    """
    Sehr defensiv: nur a–z, 0–9 und _ zulassen, um SQL-Injection zu vermeiden.
    """
    if not table_name or not table_name.replace("_", "").isalnum():
        raise ValueError("Ungültiger Tabellenname.")
    return sql.Identifier(table_name)


# ---------- KONKRETE AKTIONEN ----------

def apply_adjust_person_fte_rel(conn, table_name: str, data: dict, year: int):
    """
    Aktionstyp: adjust_person_fte_rel
    Beispiel:
      'Setze Martin Kohn im Januar um 0,3 VK runter ...'
    Schreibt in Spalte wie jan_2026.
    """
    if year not in VALID_PLAN_YEARS:
        raise ValueError(f"Jahr {year} ist nicht in den erlaubten Planjahren {sorted(VALID_PLAN_YEARS)}.")

    name = data["name"].strip()
    month_name = data["month"]
    vk_str = data["vk"].replace(",", ".")
    direction = data["direction"].lower()

    delta = Decimal(vk_str)
    delta = -abs(delta) if direction == "runter" else abs(delta)

    colname = month_col_for_year(month_name, year)
    tbl_ident = _validate_table_name(table_name)
    col_ident = sql.Identifier(colname)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT id, {col} FROM {tbl} WHERE name = %s AND year = %s").format(
                col=col_ident, tbl=tbl_ident
            ),
            (name, year),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Kein Datensatz für {name} im Jahr {year} in {table_name} gefunden")

        emp_id, current_val = row
        current_val = Decimal(str(current_val or 0))
        new_val = current_val + delta

        cur.execute(
            sql.SQL("UPDATE {tbl} SET {col} = %s, updated_at = now() WHERE id = %s").format(
                col=col_ident, tbl=tbl_ident
            ),
            (new_val, emp_id),
        )

    conn.commit()
    return {
        "employee_id": str(emp_id),
        "table": table_name,
        "column": colname,
        "old_value": str(current_val),
        "new_value": str(new_val),
    }


def apply_adjust_person_fte_abs(conn, table_name: str, data: dict, year: int):
    """
    Aktionstyp: adjust_person_fte_abs / *_full
    Beispiel: 'Setze Frau Schulz ab März 2028 auf 0,8 VK.'
    """
    if year not in VALID_PLAN_YEARS:
        raise ValueError(f"Jahr {year} ist nicht in den erlaubten Planjahren {sorted(VALID_PLAN_YEARS)}.")

    name = data["name"].strip()
    month_name = data.get("month") or ""
    vk_str = (data.get("vk") or "0").replace(",", ".")
    target_val = Decimal(vk_str)

    colname = month_col_for_year(month_name, year) if month_name else None
    if not colname:
        raise ValueError("Monat fehlt für die VK-Setzung.")

    tbl_ident = _validate_table_name(table_name)
    col_ident = sql.Identifier(colname)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT id, {col} FROM {tbl} WHERE name = %s AND year = %s").format(
                col=col_ident, tbl=tbl_ident
            ),
            (name, year),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Kein Datensatz für {name} im Jahr {year} in {table_name} gefunden")

        emp_id, current_val = row
        current_val = Decimal(str(current_val or 0))

        cur.execute(
            sql.SQL("UPDATE {tbl} SET {col} = %s, updated_at = now() WHERE id = %s").format(
                col=col_ident, tbl=tbl_ident
            ),
            (target_val, emp_id),
        )

    conn.commit()
    return {
        "employee_id": str(emp_id),
        "table": table_name,
        "column": colname,
        "old_value": str(current_val),
        "new_value": str(target_val),
    }


def apply_adjust_person_fte_range(conn, table_name: str, data: dict):
    """
    Reduziert VK für einen Zeitraum innerhalb eines Jahres (vereinfachte Variante: from/to im selben Jahr).
    """
    name = data["name"].strip()
    dt_from = datetime.strptime(data["from"], "%d.%m.%Y").date()
    dt_to = datetime.strptime(data["to"], "%d.%m.%Y").date()
    if dt_from.year != dt_to.year:
        raise ValueError("Zeitraum über mehrere Jahre wird aktuell nicht unterstützt.")
    year = dt_from.year
    if year not in VALID_PLAN_YEARS:
        raise ValueError(f"Jahr {year} ist nicht in den erlaubten Planjahren {sorted(VALID_PLAN_YEARS)}.")
    delta = Decimal(data["vk"].replace(",", "."))
    tbl_ident = _validate_table_name(table_name)

    start_idx = dt_from.month - 1
    end_idx = dt_to.month - 1
    target_cols = [sql.Identifier(f"{m}_{year}") for m in MONTH_ORDER[start_idx : end_idx + 1]]
    target_col_names = [f"{m}_{year}" for m in MONTH_ORDER[start_idx : end_idx + 1]]

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT id, {} FROM {} WHERE name = %s AND year = %s").format(
                sql.SQL(", ").join(target_cols), tbl_ident
            ),
            (name, year),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Kein Datensatz für {name} im Jahr {year} in {table_name} gefunden")
        emp_id = row[0]
        current_vals = [Decimal(str(v or 0)) for v in row[1:]]
        new_vals = [val - delta for val in current_vals]
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(col) for col in target_cols
        )
        cur.execute(
            sql.SQL("UPDATE {tbl} SET {assign}, updated_at = now() WHERE id = %s").format(
                tbl=tbl_ident, assign=assignments
            ),
            (*new_vals, emp_id),
        )
    conn.commit()
    return {
        "employee_id": str(emp_id),
        "table": table_name,
        "columns": target_col_names,
        "old_values": [str(v) for v in current_vals],
        "new_values": [str(v) for v in new_vals],
    }


def apply_transfer_staff_unit(conn, table_name: str, data: dict):
    """
    Aktionstyp: transfer_staff_unit
    Beispiel:
      'Zum 01.04.2026 Hans Möller auf Station 5 versetzen.'
    → setzt dept in der entsprechenden Jahreszeile.
    """
    name = data["name"].strip()
    target_dept = data["unit"].strip()
    # accept either a date or a bare year (from move_employee_to_station_year)
    eff_date = None
    year = None
    if data.get("date"):
        eff_date = datetime.strptime(data["date"], "%d.%m.%Y").date()
        year = eff_date.year
    elif data.get("year"):
        year = int(data["year"])
    else:
        raise ValueError("Kein Datum oder Jahr angegeben.")

    tbl_ident = _validate_table_name(table_name)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT id, dept FROM {tbl} WHERE name = %s AND year = %s").format(
                tbl=tbl_ident
            ),
            (name, year),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Kein Datensatz für {name} im Jahr {year} in {table_name} gefunden")

        emp_id, old_dept = row

        cur.execute(
            sql.SQL("UPDATE {tbl} SET dept = %s, updated_at = now() WHERE id = %s").format(
                tbl=tbl_ident
            ),
            (target_dept, emp_id),
        )

    conn.commit()
    return {
        "employee_id": str(emp_id),
        "table": table_name,
        "old_dept": old_dept,
        "new_dept": target_dept,
        "effective_from": eff_date.isoformat() if eff_date else str(year),
    }


def apply_exclude_employee_year(conn, table_name: str, data: dict):
    name = data["name"].strip()
    year = int(data["year"])
    tbl_ident = _validate_table_name(table_name)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("UPDATE {tbl} SET include = false, updated_at = now() WHERE name = %s AND year = %s RETURNING id").format(
                tbl=tbl_ident
            ),
            (name, year),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Kein Datensatz für {name} im Jahr {year} in {table_name} gefunden")
        emp_id = row[0]
    conn.commit()
    return {"employee_id": str(emp_id), "table": table_name, "include": False}


def _fetch_employee_rows(conn, table_name: str, where_clause: str, params: tuple):
    tbl_ident = _validate_table_name(table_name)
    query = sql.SQL(
        f"SELECT id, name, year, dept, include, personal_number FROM {{tbl}} WHERE {where_clause}"
    ).format(tbl=tbl_ident)
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall(), [c[0] for c in cur.description]


def query_employee_exists(conn, table_name: str, data: dict, year: Optional[int] = None):
    name = data["name"].strip()
    params = [name]
    where = "LOWER(name) = LOWER(%s)"
    if year:
        where += " AND year = %s"
        params.append(year)
    rows, cols = _fetch_employee_rows(conn, table_name, where, tuple(params))
    if not rows:
        return {"exists": False, "name": name}
    results = []
    for row in rows:
        rec = dict(zip(cols, row))
        results.append(rec)
    return {"exists": True, "matches": results}


def query_employee_station(conn, table_name: str, data: dict, year: Optional[int]):
    name = data["name"].strip()
    params = [name]
    where = "LOWER(name) = LOWER(%s)"
    if year:
        where += " AND year = %s"
        params.append(year)
    rows, cols = _fetch_employee_rows(conn, table_name, where, tuple(params))
    if not rows:
        raise ValueError(f"Keine Station für {name} gefunden.")
    recs = [dict(zip(cols, r)) for r in rows]
    return {"stations": recs}


def query_employees_on_station(conn, table_name: str, data: dict):
    dept = data["dept"].strip()
    year = data.get("year")
    params = [dept]
    where = "LOWER(dept) = LOWER(%s)"
    if year:
        where += " AND year = %s"
        params.append(year)
    rows, cols = _fetch_employee_rows(conn, table_name, where, tuple(params))
    return {"dept": dept, "year": year, "employees": [dict(zip(cols, r)) for r in rows]}


def _month_cols_for_year(year: int):
    return [f"{m}_{year}" for m in MONTH_ORDER]


def query_employee_vks_year(conn, table_name: str, data: dict):
    name = data["name"].strip()
    year = int(data["year"])
    cols = _month_cols_for_year(year)
    tbl_ident = _validate_table_name(table_name)
    col_ident_list = [sql.Identifier(c) for c in cols]
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT {} FROM {} WHERE LOWER(name)=LOWER(%s) AND year=%s").format(
                sql.SQL(", ").join(col_ident_list), tbl_ident
            ),
            (name, year),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Keine Daten für {name} in {year}")
        vals = [Decimal(str(v or 0)) for v in row]
    avg = sum(vals) / Decimal(len(vals))
    return {"name": name, "year": year, "avg_vk": str(round(avg, 4)), "months": dict(zip(cols, map(str, vals)))}


def query_station_vks_year(conn, table_name: str, data: dict):
    dept = data["dept"].strip()
    year = int(data["year"])
    cols = _month_cols_for_year(year)
    tbl_ident = _validate_table_name(table_name)
    col_sum = sql.SQL(" + ").join(sql.Identifier(c) for c in cols)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT SUM({sum_expr}) FROM {tbl} WHERE LOWER(dept)=LOWER(%s) AND year=%s").format(
                sum_expr=col_sum, tbl=tbl_ident
            ),
            (dept, year),
        )
        total = cur.fetchone()[0] or 0
    avg = Decimal(str(total)) / Decimal(len(cols)) if cols else Decimal("0")
    return {"dept": dept, "year": year, "total_vk": str(total), "avg_vk": str(round(avg, 4))}


def query_employee_by_pnr(conn, table_name: str, data: dict):
    pnr = data["pnr"].strip()
    year = data.get("year")
    params = [pnr]
    where = "(personal_number = %s OR personalnumber = %s)"
    params.append(pnr)
    if year:
        where += " AND year = %s"
        params.append(year)
    rows, cols = _fetch_employee_rows(conn, table_name, where, tuple(params))
    return {"pnr": pnr, "year": year, "matches": [dict(zip(cols, r)) for r in rows]}


def query_station_by_pnr(conn, table_name: str, data: dict):
    pnr = data["pnr"].strip()
    rows, cols = _fetch_employee_rows(conn, table_name, "(personal_number = %s OR personalnumber = %s)", (pnr, pnr))
    if not rows:
        return {"pnr": pnr, "found": False}
    recs = [dict(zip(cols, r)) for r in rows]
    return {"pnr": pnr, "found": True, "matches": recs}


def query_employees_site_year(conn, table_name: str, data: dict):
    year = int(data["year"])
    rows, cols = _fetch_employee_rows(conn, table_name, "year = %s", (year,))
    return {"site_table": table_name, "year": year, "employees": [dict(zip(cols, r)) for r in rows]}


def apply_action(conn, table_name: str, parsed: dict, year: Optional[int] = None):
    """
    Dispatcher: ruft je nach action-Typ die passende Funktion auf.
    year:
      - für Monatsaktionen (VK-Anpassungen) nötig
      - wenn None → heuristisch aktuelles Jahr
    """
    action = parsed["action"]
    data = parsed["data"]

    if year is None:
        year = datetime.today().year

    if action in {"adjust_person_fte_rel", "adjust_person_fte_rel_full"}:
        return apply_adjust_person_fte_rel(conn, table_name, data, year)
    if action in {"adjust_person_fte_abs", "adjust_person_fte_abs_full"}:
        return apply_adjust_person_fte_abs(conn, table_name, data, year)
    if action in {"transfer_staff_unit", "move_employee_to_station_year"}:
        return apply_transfer_staff_unit(conn, table_name, data)
    if action == "adjust_person_fte_rel_missing_name":
        raise ValueError("Name fehlt: Für welchen Mitarbeiter soll der Stellenanteil angepasst werden?")
    if action == "adjust_person_fte_range":
        return apply_adjust_person_fte_range(conn, table_name, data)
    if action == "exclude_employee_year":
        return apply_exclude_employee_year(conn, table_name, data)
    if action == "get_employee_vks_year":
        return query_employee_vks_year(conn, table_name, data)
    if action == "get_station_vks_year":
        return query_station_vks_year(conn, table_name, data)
    if action == "check_employee_works_here":
        return query_employee_exists(conn, table_name, data, year)
    if action == "get_employee_station":
        return query_employee_station(conn, table_name, data, year)
    if action == "list_employees_on_station":
        return query_employees_on_station(conn, table_name, data)
    if action == "check_employee_by_personal_number":
        return query_employee_by_pnr(conn, table_name, data)
    if action == "get_station_by_personal_number":
        return query_station_by_pnr(conn, table_name, data)
    if action == "list_employees_site_year":
        return query_employees_site_year(conn, table_name, data)
    if action == "assistant_help":
        return {"help": True}

    raise ValueError(f"Aktionstyp noch nicht implementiert: {action}")


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os

    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL fehlt (siehe .env.example)")

    conn = psycopg2.connect(db_url)
    table = "stellenplan_employees_gfodin"  # Beispiel
    year = 2026

    print("🔌 Verbunden. Teste Textbefehle (exit zum Beenden).")
    while True:
        cmd = input("📝 Textbefehl: ")
        if cmd.lower() in {"exit", "quit"}:
            break

        parsed = parse_command(cmd)
        if not parsed:
            print("❌ Befehl nicht erkannt.")
            continue

        print("Erkannt:", parsed)
        try:
            result = apply_action(conn, table, parsed, year=year)
            print("✅ Ausgeführt:", result)
        except Exception as exc:
            print("⚠️ Fehler:", exc)

    conn.close()
