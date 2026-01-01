// worker.js
// Endpunkte:
//  - POST /login
//  - GET  /api/audit?site=...&limit=...
//  - POST /api/ai-command          (nur KI-Parsing)
//  - POST /api/command             (KI-Parsing + DB-Aktion)
//  - POST /api/rollover            (Stellenanteile in Jahres-Spalten vorwärts kopieren)
//
// Erwartete Secrets/Vars:
//  - OPENAI_API_KEY (Secret)
//  - SUPABASE_SERVICE_ROLE_KEY (Secret)
//  - SUPABASE_URL (Plaintext)

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
};
const json = (d, s = 200) =>
  new Response(JSON.stringify(d), {
    status: s,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });

// ---------- KI-Prompt ----------
const SYSTEM_PROMPT = `
Du bist ein Assistent für das Stellenplan- und Personalplanungssystem "Clinicon" in einem Krankenhaus.

AUFGABE:
- Du erhältst deutschsprachige Texte (Befehle oder Fragen).
- Du interpretierst diese Texte und gibst IMMER eine JSON-Struktur zurück.
- Du führst KEINE Aktionen selbst aus, du PARST nur.
- Wenn etwas unklar ist, schlage eine Rückfrage vor und setze "needs_clarification" auf true.

ERLAUBTE INTENTS:
- "adjust_person_fte_rel"
- "adjust_person_fte_abs"
- "move_employee_unit"
- "check_employee_exists"
- "get_employee_unit"
- "list_unit_employees"
- "get_employee_fte_year"
- "help"
- "unknown"

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
- Monate als deutschen Text.
- Fehlende Pflichtinfos -> needs_clarification=true + passende Rückfrage.
- Immer nur JSON, kein Fließtext.
`;

// ---------- KI-Aufruf ----------
async function callOpenAI(env, prompt) {
  const apiKey = env.OPENAI_API_KEY;
  if (!apiKey) throw new Error("OPENAI_API_KEY/Clinicon-Assistent fehlt");
  const resp = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: "gpt-4.1-mini",
      messages: [
        { role: "system", content: SYSTEM_PROMPT },
        { role: "user", content: prompt },
      ],
      temperature: 0.1,
    }),
  });
  if (!resp.ok) {
    const t = await resp.text();
    throw new Error(`OpenAI error ${resp.status}: ${t}`);
  }
  const data = await resp.json();
  const content = data.choices?.[0]?.message?.content || "";
  try {
    return JSON.parse(content);
  } catch {
    const start = content.indexOf("{");
    const end = content.lastIndexOf("}");
    if (start !== -1 && end !== -1) {
      return JSON.parse(content.slice(start, end + 1));
    }
    throw new Error("OpenAI-Antwort nicht als JSON lesbar");
  }
}

// ---------- Supabase Helpers ----------
async function supaFetch(env, path, opts = {}) {
  const url = `${env.SUPABASE_URL}/rest/v1${path}`;
  const res = await fetch(url, {
    ...opts,
    headers: {
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      ...(opts.headers || {}),
    },
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Supabase error ${res.status}: ${txt}`);
  }
  const ct = res.headers.get("content-type") || "";
  return ct.includes("application/json") ? res.json() : res.text();
}
async function queryTable(env, table, params) {
  const qs = new URLSearchParams(params);
  return supaFetch(env, `/${table}?${qs.toString()}`);
}
async function updateColumns(env, table, id, updates) {
  await supaFetch(env, `/${table}?id=eq.${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json", Prefer: "return=minimal" },
    body: JSON.stringify(updates),
  });
}
async function logAudit(env, payload) {
  try {
    await supaFetch(env, "/assistant_audit", {
      method: "POST",
      headers: { "Content-Type": "application/json", Prefer: "return=minimal" },
      body: JSON.stringify(payload),
    });
  } catch (e) {
    /* ignore */
  }
}

// ---------- Login ----------
async function handleLogin(body, env) {
  const { username, password } = body || {};
  if (!username || !password) return json({ success: false, error: "Missing credentials" }, 400);
  const url = new URL(`${env.SUPABASE_URL}/rest/v1/app_users`);
  url.searchParams.set("username", `eq.${encodeURIComponent(username)}`);
  url.searchParams.set("select", "username,password,site_code");
  url.searchParams.set("limit", "1");
  const res = await fetch(url.toString(), {
    headers: { Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`, apikey: env.SUPABASE_SERVICE_ROLE_KEY },
  });
  if (!res.ok) return json({ success: false, error: "REST error" }, 500);
  const rows = await res.json();
  if (!rows.length) return json({ success: false, error: "User not found" }, 401);
  const user = rows[0];
  if (user.password !== password) return json({ success: false, error: "Wrong password" }, 401);
  return json({ success: true, siteCode: (user.site_code || user.username || "").toUpperCase() });
}

// ---------- Audit lesen ----------
async function handleAuditFetch(env, url) {
  const site = url.searchParams.get("site") || "";
  const limit = Math.max(1, Math.min(parseInt(url.searchParams.get("limit") || "15", 10), 100));
  const params = new URLSearchParams();
  if (site) params.set("site", `eq.${site}`);
  params.set("order", "created_at.desc");
  params.set("limit", String(limit));
  const data = await supaFetch(env, "/assistant_audit?" + params.toString());
  return json({ success: true, audit: data });
}

// ---------- DB-Aktionen ----------
const MONTH_MAP = {
  "januar": "jan", "jan": "jan",
  "februar": "feb", "feb": "feb",
  "märz": "mrz", "maerz": "mrz", "mrz": "mrz",
  "april": "apr", "apr": "apr",
  "mai": "mai",
  "juni": "jun", "jun": "jun",
  "juli": "jul", "jul": "jul",
  "august": "aug", "aug": "aug",
  "september": "sep", "sep": "sep",
  "oktober": "okt", "okt": "okt",
  "november": "nov", "nov": "nov",
  "dezember": "dez", "dez": "dez",
};
const MONTHS = ["jan","feb","mrz","apr","mai","jun","jul","aug","sep","okt","nov","dez"];
const YEAR_MIN = 2026;
const YEAR_MAX = 2099;
const monthCol = (m, y) => {
  const key = (m || "").toLowerCase().replace("ä", "ae").replace("ö", "oe").replace("ü", "ue");
  const base = MONTH_MAP[key] || key.slice(0,3);
  return `${base}_${y}`;
};
const num = v => {
  const n = parseFloat((v ?? "").toString().replace(",", "."));
  return Number.isFinite(n) ? n : 0;
};

async function fetchEmployee(env, table, name, year){
  const params={ name:`ilike.*${name}*`, limit:"1" };
  if(year) params.year=`eq.${year}`;
  return (await queryTable(env,table,params))[0];
}
function ensureYearInRange(y){
  if (y < YEAR_MIN || y > YEAR_MAX) {
    throw new Error(`Jahr ${y} nicht verfügbar (erwartet ${YEAR_MIN}-${YEAR_MAX}).`);
  }
}
function ensureHasColumn(record, colName, table){
  if (record && Object.prototype.hasOwnProperty.call(record, colName)) return;
  throw new Error(`Spalte ${colName} fehlt in Tabelle ${table}`);
}
async function actAdjustRel(env, table, fields){
  const y = parseInt(fields.year||"0",10); if(!y) throw new Error("Jahr fehlt.");
  ensureYearInRange(y);
  const col = monthCol(fields.month, y);
  const delta = num(fields.delta_fte);
  const emp = await fetchEmployee(env, table, fields.employee_name||"", y);
  if(!emp) throw new Error(`Kein Datensatz für ${fields.employee_name} in ${y}`);
  ensureHasColumn(emp, col, table);
  const cur = num(emp[col]), neu = cur + delta;
  await updateColumns(env, table, emp.id, { [col]: neu, updated_at: new Date().toISOString() });
  return { employee_id: emp.id, table, column: col, old_value: cur, new_value: neu };
}
async function actAdjustAbs(env, table, fields){
  const y = parseInt(fields.year||"0",10); if(!y) throw new Error("Jahr fehlt.");
  ensureYearInRange(y);
  const col = monthCol(fields.month, y);
  const target = num(fields.target_fte);
  const emp = await fetchEmployee(env, table, fields.employee_name||"", y);
  if(!emp) throw new Error(`Kein Datensatz für ${fields.employee_name} in ${y}`);
  ensureHasColumn(emp, col, table);
  const cur = num(emp[col]);
  await updateColumns(env, table, emp.id, { [col]: target, updated_at: new Date().toISOString() });
  return { employee_id: emp.id, table, column: col, old_value: cur, new_value: target };
}
async function actTransfer(env, table, fields){
  const y = parseInt(fields.year||"0",10); if(!y) throw new Error("Jahr fehlt.");
  ensureYearInRange(y);
  const emp = await fetchEmployee(env, table, fields.employee_name||"", y);
  if(!emp) throw new Error(`Kein Datensatz für ${fields.employee_name} in ${y}`);
  await updateColumns(env, table, emp.id, { dept: fields.unit||"", updated_at: new Date().toISOString() });
  return { employee_id: emp.id, table, old_dept: emp.dept, new_dept: fields.unit||"", year: y };
}
async function actQueryExists(env, table, fields){
  const params={ name:`ilike.*${fields.employee_name||""}*`, select:"id,name,dept,year" };
  if(fields.year) params.year=`eq.${fields.year}`;
  const rows=await queryTable(env,table,params);
  return { exists: rows.length>0, matches: rows };
}
async function actQueryStation(env, table, fields){
  const params={ name:`ilike.*${fields.employee_name||""}*`, select:"id,name,dept,year" };
  if(fields.year) params.year=`eq.${fields.year}`;
  const rows=await queryTable(env,table,params);
  return { stations: rows };
}
async function actListUnit(env, table, fields){
  const params={ dept:`ilike.*${fields.unit||""}*`, select:"id,name,dept,year" };
  if(fields.year) params.year=`eq.${fields.year}`;
  const rows=await queryTable(env,table,params);
  return { dept: fields.unit||"", year: fields.year||null, employees: rows };
}
async function actEmployeeFteYear(env, table, fields){
  const y=parseInt(fields.year||"0",10); if(!y) throw new Error("Jahr fehlt.");
  ensureYearInRange(y);
  const baseQuery = { name:`ilike.*${fields.employee_name||""}*`, year:`eq.${y}`, select:"*", limit:"1" };
  const rows=await queryTable(env,table,baseQuery);
  if(!rows.length){
    return {
      found:false,
      name: fields.employee_name || "",
      year: y,
      month: fields.month || null,
      month_column: fields.month ? monthCol(fields.month, y) : null,
      month_value: null,
      avg_vk: null,
      avg_year: null,
      months: {},
    };
  }
  const record = rows[0];
  const effectiveYear = y; // erzwinge das angefragte Jahr
  const cols=MONTHS.map(m=>`${m}_${effectiveYear}`);
  // prüfen, ob alle Spalten existieren
  cols.forEach(c => ensureHasColumn(record, c, table));
  const selectVals = cols.map(c=>num(record[c]));
  const avg=selectVals.reduce((a,b)=>a+b,0)/cols.length;
  let month_column = null, month_value = null;
  if (fields.month) {
    month_column = monthCol(fields.month, effectiveYear);
    ensureHasColumn(record, month_column, table);
    month_value = num(record[month_column]);
  }
  return {
    found:true,
    name: fields.employee_name || "",
    year: effectiveYear,
    month: fields.month || null,
    month_column,
    month_value,
    avg_vk: month_value !== null ? month_value : avg,
    avg_year: avg,
    months: Object.fromEntries(cols.map((c,i)=>[c, selectVals[i]])),
  };
}

// ROLLOVER: Werte von fromYear nach toYear kopieren, optional gefiltert auf ids/dept, mode: fill (nur leere Ziele) oder overwrite (immer)
async function actRollover(env, table, fromYear, toYear, dept, ids = [], mode = "fill") {
  if(fromYear < YEAR_MIN || toYear > YEAR_MAX || fromYear >= toYear){
    throw new Error(`Jahresbereich ungültig (${fromYear}-${toYear}, erlaubt ${YEAR_MIN}-${YEAR_MAX}, from<to)`);
  }
  if(!Array.isArray(ids) || ids.length === 0){
    throw new Error("Bitte ids angeben (Array von IDs), um einen gezielten Rollover auszuführen.");
  }
  const colsFrom = MONTHS.map(m => `${m}_${fromYear}`);
  const colsTo = MONTHS.map(m => `${m}_${toYear}`);
  const results = [];

  for (const id of ids) {
    // Datensatz holen
    const qs = new URLSearchParams();
    qs.set("id", `eq.${id}`);
    if (dept) qs.set("dept", `eq.${dept}`);
    const row = (await supaFetch(env, `/${table}?${qs.toString()}`))[0];
    if(!row){
      results.push({ id, status:"not_found" });
      continue;
    }
    // Mapping bauen
    const update = {};
    colsTo.forEach((colTo, idx) => {
      const valFrom = row[colsFrom[idx]];
      const currentTo = row[colTo];
      if (mode === "fill") {
        if (currentTo === null || currentTo === undefined) update[colTo] = valFrom;
      } else {
        update[colTo] = valFrom;
      }
    });
    if (Object.keys(update).length === 0) {
      results.push({ id, status:"skipped" });
      continue;
    }
    await supaFetch(env, `/${table}?id=eq.${id}`, {
      method:"PATCH",
      headers:{ "Content-Type":"application/json", Prefer:"return=minimal" },
      body: JSON.stringify(update),
    });
    results.push({ id, status:"ok", updated: Object.keys(update) });
  }
  return { table, fromYear, toYear, dept: dept || null, mode, results };
}

// ---------- KI-Parser + Aktion ----------
async function handleAiCommand(body, env, executeDb = false) {
  const { command, table, site } = body || {};
  if (!command) return json({ success: false, error: "Missing command" }, 400);

  const parsed = await callOpenAI(env, command);
  if (parsed.needs_clarification) {
    return json({ success: true, parsed, note: "Clarification needed" });
  }
  if (!executeDb) {
    return json({ success: true, parsed });
  }

  // DB-Ausführung
  const intent = parsed.intent || "unknown";
  const f = parsed.fields || {};
  let result;
  try {
    switch (intent) {
      case "adjust_person_fte_rel":
        result = await actAdjustRel(env, table, f);
        break;
      case "adjust_person_fte_abs":
        result = await actAdjustAbs(env, table, f);
        break;
      case "move_employee_unit":
        result = await actTransfer(env, table, f);
        break;
      case "check_employee_exists":
        result = await actQueryExists(env, table, f);
        break;
      case "get_employee_unit":
        result = await actQueryStation(env, table, f);
        break;
      case "list_unit_employees":
        result = await actListUnit(env, table, f);
        break;
      case "get_employee_fte_year":
        result = await actEmployeeFteYear(env, table, f);
        break;
      case "help":
        result = { help: true };
        break;
      default:
        throw new Error(`Intent '${intent}' nicht implementiert.`);
    }
  } catch (err) {
    await logAudit(env, {
      site: site || "unknown",
      command,
      action: intent,
      target_table: table || "",
      plan_year: f.year || null,
      status: "error",
      result: { error: err.message },
    });
    return json({ success: false, error: err.message, parsed });
  }

  await logAudit(env, {
    site: site || "unknown",
    command,
    action: intent,
    target_table: table || "",
    plan_year: f.year || null,
    status: "ok",
    result,
  });
  return json({ success: true, parsed, applied: result });
}

// ---------- Rollover Handler ----------
async function handleRollover(body, env){
  const { table, fromYear, toYear, dept, ids, mode } = body || {};
  if(!table || !fromYear || !toYear){
    return json({ success:false, error:"table, fromYear, toYear erforderlich" }, 400);
  }
  try{
    const res = await actRollover(env, table, Number(fromYear), Number(toYear), dept, ids, mode || "fill");
    return json({ success:true, result:res });
  }catch(err){
    return json({ success:false, error: err.message }, 500);
  }
}

// ---------- Router ----------
export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders });
    const url = new URL(request.url);

    // Login
    if (request.method === "POST" && (url.pathname === "/" || url.pathname === "/login")) {
      let body; try { body = await request.json(); } catch { return json({ success:false, error:"Invalid JSON" },400); }
      return handleLogin(body, env);
    }

    // Audit lesen
    if (url.pathname === "/api/audit" && request.method === "GET") {
      try { return await handleAuditFetch(env, url); } catch (err) { return json({ success:false, error: err.message },500); }
    }

    // KI-Parser nur (kein DB)
    if (url.pathname === "/api/ai-command" && request.method === "POST") {
      let body; try { body = await request.json(); } catch { return json({ success:false, error:"Invalid JSON" },400); }
      try { return await handleAiCommand(body, env, false); } catch (err) { return json({ success:false, error: err.message },500); }
    }

    // KI-Parser + DB-Ausführung
    if (url.pathname === "/api/command" && request.method === "POST") {
      let body; try { body = await request.json(); } catch { return json({ success:false, error:"Invalid JSON" },400); }
      try { return await handleAiCommand(body, env, true); } catch (err) { return json({ success:false, error: err.message },500); }
    }

    // Rollover (Werte von fromYear nach toYear kopieren, nur leere Ziele werden gefüllt)
    if (url.pathname === "/api/rollover" && request.method === "POST") {
      let body; try { body = await request.json(); } catch { return json({ success:false, error:"Invalid JSON" },400); }
      return await handleRollover(body, env);
    }

    return new Response("Not found", { status: 404, headers: corsHeaders });
  },
};
