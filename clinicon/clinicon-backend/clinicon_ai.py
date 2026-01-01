import os
import json
from typing import Any, Dict

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = """
Du bist ein Assistent für das Stellenplan- und Personalplanungssystem "Clinicon" in einem Krankenhaus.

AUFGABE:
- Du erhältst deutschsprachige Texte (Befehle oder Fragen).
- Du interpretierst diese Texte und gibst IMMER eine JSON-Struktur zurück.
- Du führst KEINE Aktionen selbst aus, du PARST nur.
- Wenn etwas unklar ist, schlage eine Rückfrage vor und setze "needs_clarification" auf true.

ERLAUBTE INTENTS:
- "adjust_person_fte_rel"         : Stellenanteil relativ ändern (z.B. "um 0,5 VK reduzieren")
- "adjust_person_fte_abs"         : Stellenanteil absolut setzen (z.B. "auf 0,8 VK setzen")
- "move_employee_unit"            : Mitarbeiter auf andere Station/Bereich versetzen
- "check_employee_exists"         : prüfen, ob Mitarbeiter im System vorhanden ist
- "get_employee_unit"             : Station/Bereich eines Mitarbeiters erfragen
- "list_unit_employees"           : Mitarbeitende einer Station/Bereich erfragen
- "get_employee_fte_year"         : VK eines Mitarbeiters für ein Jahr erfragen
- "help"                          : Nutzer fragt, was der Assistent kann
- "unknown"                       : keine sinnvolle Interpretation möglich

AUSGABEFORMAT (immer JSON):
{
  "intent": "<intent>",
  "fields": {
    "employee_name": string | null,
    "personal_number": string | null,
    "month": string | null,
    "year": number | null,
    "delta_fte": number | null,
    "target_fte": number | null,
    "unit": string | null,
    "site": string | null
  },
  "confidence": number,
  "needs_clarification": boolean,
  "clarification_question": string | null,
  "notes": string | null
}

BESONDERE REGELN:
- "delta_fte" ist für relative Änderungen, z.B. -0.5 für "um 0,5 VK reduzieren".
- "target_fte" ist für absolute Zielwerte, z.B. 0.8 für "auf 0,8 VK setzen".
- Monate bitte als deutscher Text ausgeben (z.B. "Januar", "Februar", "März").
- Wenn der Name des Mitarbeiters fehlt, setze "needs_clarification" auf true und stelle eine passende Rückfrage.
- Wenn das Jahr fehlt, aber ein Monat erwähnt ist, lasse "year" = null und stelle eine Rückfrage für das Jahr.
- Konfidenz zwischen 0 und 1.0 schätzen.
- Antworte NIE mit freiem Text, immer nur mit JSON.
"""


def parse_command_with_ai(command_text: str) -> Dict[str, Any]:
    """
    Ruft die ChatGPT-API auf und gibt ein geparstes JSON-Objekt zurück.
    """
    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": command_text},
        ],
        temperature=0.1,
    )

    content = response.choices[0].message.content or ""

    # Versuche, die Ausgabe als JSON zu interpretieren
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1:
            json_str = content[start : end + 1]
            return json.loads(json_str)
        raise ValueError(f"Antwort konnte nicht als JSON geparst werden: {content}")


if __name__ == "__main__":
    while True:
        text = input("📝 Clinicon-Befehl (exit zum Beenden): ")
        if text.lower() in ("exit", "quit"):
            break
        try:
            result = parse_command_with_ai(text)
            print(json.dumps(result, indent=2, ensure_ascii=False))
        except Exception as e:
            print("⚠️ Fehler:", e)
