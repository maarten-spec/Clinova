import re
from typing import Dict, Optional

# Intent-Patterns (re.UNICODE / IGNORECASE)
INTENT_PATTERNS = [
    (
        "adjust_person_fte_rel_full",
        re.compile(
            r"mitarbeiter\s+(?P<name>[\wÄÖÜäöüß\s\-]+)\s+möchte\s+zum\s+(?P<month>\w+)\s+(?P<year>\d{4})\s+seinen\s+stellenanteil\s+um\s+(?P<vk>[0-9\.,]+)\s+vk\s+(?P<direction>reduzieren|erhöhen|erhoehen)",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "adjust_person_fte_rel_missing_name",
        re.compile(
            r"ein\s+mitarbeiter\s+möchte\s+zum\s+(?P<month>\w+)\s+(?P<year>\d{4})\s+seinen\s+stellenanteil\s+um\s+(?P<vk>[0-9\.,]+)\s+vk\s+(?P<direction>reduzieren|erhöhen|erhoehen)",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "adjust_person_fte_abs_full",
        re.compile(
            r"setze\s+(?P<name>[\wÄÖÜäöüß\s\-]+)\s+ab\s+(?P<month>\w+)\s+(?P<year>\d{4})\s+auf\s+(?P<vk>[0-9\.,]+)\s+vk",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "check_employee_works_here",
        re.compile(
            r"(arbeitet|ist)\s+(?:ein[e]?\s+)?(?P<name>[\wÄÖÜäöüß\s\-]+)\s+(hier|bei\s+uns)\s*(\?)?",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "get_employee_station",
        re.compile(
            r"(auf\s+welcher\s+station\s+arbeitet|wo\s+ist)\s+(?P<name>[\wÄÖÜäöüß\s\-]+)\s*(eingeteilt|tätig|taetig)?\s*(\?)?",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "list_employees_on_station",
        re.compile(
            r"(welche\s+mitarbeiter\s+arbeiten\s+auf|wer\s+ist\s+auf)\s+(?P<dept>station\s*\d+|intensivstation|imc|[\wÄÖÜäöüß0-9\s\-]+)\s+(im\s+jahr\s+(?P<year>\d{4}))?\s*(\?)?",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "get_employee_vks_year",
        re.compile(
            r"wie\s+viele\s+vk\s+hat\s+(?P<name>[\wÄÖÜäöüß\s\-]+)\s+im\s+jahr\s+(?P<year>\d{4})\s*(\?)?",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "get_station_vks_year",
        re.compile(
            r"wie\s+viele\s+vk\s+sind\s+auf\s+(?P<dept>station\s*\d+|[\wÄÖÜäöüß0-9\s\-]+)\s+im\s+jahr\s+(?P<year>\d{4})\s+geplant\s*(\?)?",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "move_employee_to_station_year",
        re.compile(
            r"(verschiebe|versetze)\s+(?P<name>[\wÄÖÜäöüß\s\-]+)\s+ab\s+(?P<year>\d{4})\s+auf\s+(?P<dept>station\s*\d+|bereich\s+[\wÄÖÜäöüß0-9\s\-]+|[\wÄÖÜäöüß0-9\s\-]+)",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "adjust_person_fte_range",
        re.compile(
            r"reduziere\s+(?P<name>[\wÄÖÜäöüß\s\-]+)\s+vom\s+(?P<from>\d{1,2}\.\d{1,2}\.\d{4})\s+bis\s+(?P<to>\d{1,2}\.\d{1,2}\.\d{4})\s+um\s+(?P<vk>[0-9\.,]+)\s+vk(?:\s+wegen\s+(?P<reason>.+))?",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "exclude_employee_year",
        re.compile(
            r"(nimm|setze)\s+(?P<name>[\wÄÖÜäöüß\s\-]+)\s+im\s+jahr\s+(?P<year>\d{4})\s+aus\s+der\s+planung\s+raus|auf\s+nicht\s+einplanen",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "check_employee_by_personal_number",
        re.compile(
            r"(gibt\s+es\s+einen\s+mitarbeiter\s+mit\s+der\s+personalnummer|existiert\s+die\s+personalnummer)\s+(?P<pnr>\d+)(\s+im\s+stellenplan\s+(?P<year>\d{4}))?\s*(\?)?",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "get_station_by_personal_number",
        re.compile(
            r"auf\s+welcher\s+station\s+arbeitet\s+der\s+mitarbeiter\s+mit\s+der\s+personalnummer\s+(?P<pnr>\d+)\s*(\?)?",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "list_employees_site_year",
        re.compile(
            r"(zeig\s+mir|liste)\s+alle\s+mitarbeiter\s+vom\s+standort\s+(?P<site>[A-Za-z0-9_]+)\s+im\s+jahr\s+(?P<year>\d{4})",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
    (
        "assistant_help",
        re.compile(
            r"(was\s+kann\s+der\s+stellenplan[-\s]*assistent|welche\s+befehle\s+kann\s+ich\s+benutzen|hilfe\s+stellenplan)",
            re.IGNORECASE | re.UNICODE,
        ),
    ),
]


def parse_command(text: str) -> Optional[Dict[str, Dict[str, str]]]:
    """
    Nimmt einen Textbefehl und gibt {intent/action, data} zurück oder None.
    """
    cleaned = text.strip()
    for intent, pattern in INTENT_PATTERNS:
        match = pattern.search(cleaned)
        if match:
            data = match.groupdict()
            return {"intent": intent, "action": intent, "data": data}
    return None


if __name__ == "__main__":
    while True:
        cmd = input("📝 Befehl (exit zum Beenden): ")
        if cmd.lower() in {"exit", "quit"}:
            break
        result = parse_command(cmd)
        print("Ergebnis:", result)
