from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from backend.app.services.verticals import get_vertical_pack

router = APIRouter()


@router.get("/spec/overview", response_class=HTMLResponse)
def spec_overview():
    # We keep this lightweight + human-readable.
    # The machine-readable truth lives in /spec/{vertical}.json
    aviation = get_vertical_pack("aviation")
    pharma = get_vertical_pack("pharma")

    def _v_card(v):
        vid = v.get("id", "")
        name = v.get("name", "")
        ver = v.get("version", "")
        policy_id = (v.get("policy") or {}).get("id", "")
        return f"""
        <div class="card">
          <div class="kicker">{vid.upper()}</div>
          <h2>{name}</h2>
          <p class="muted">Spec v{ver} • Policy: <code>{policy_id}</code></p>

          <div class="links">
            <a class="btn" href="/spec/{vid}">Human spec</a>
            <a class="btn" href="/spec/{vid}.json">Machine spec (JSON)</a>
          </div>

          <div class="mini">
            <div><b>Proof examples:</b> <a href="/spec/{vid}.json">See <code>examples[]</code> list</a></div>
            <div><b>Offline verify:</b> <a href="/spec/{vid}.json">See <code>offline_verification</code></a></div>
          </div>
        </div>
        """

    return HTMLResponse(f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>XERTIFY — Spec Overview</title>
  <style>
    body {{
      font-family: ui-sans-serif, system-ui, -apple-system;
      margin: 0;
      background: #0b1220;
      color: #e5e7eb;
    }}
    .wrap {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 28px 18px 56px;
    }}
    .hero {{
      border: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.03);
      border-radius: 22px;
      padding: 22px;
      box-shadow: 0 12px 30px rgba(0,0,0,0.25);
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 32px;
      letter-spacing: -0.02em;
    }}
    .one {{
      font-size: 18px;
      line-height: 1.45;
      color: #cbd5e1;
      margin: 0 0 12px;
    }}
    .pillrow {{
      display: flex; flex-wrap: wrap; gap: 10px;
      margin-top: 14px;
    }}
    .pill {{
      border: 1px solid rgba(255,255,255,0.10);
      background: rgba(255,255,255,0.02);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 13px;
      color: #cbd5e1;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 14px;
      margin-top: 18px;
    }}
    .card {{
      grid-column: span 6;
      border: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.03);
      border-radius: 20px;
      padding: 18px;
    }}
    .kicker {{
      font-size: 12px;
      letter-spacing: 0.12em;
      color: #93c5fd;
      margin-bottom: 8px;
    }}
    h2 {{
      margin: 0 0 6px;
      font-size: 20px;
    }}
    .muted {{
      margin: 0 0 12px;
      color: #a3b0c2;
      font-size: 13px;
    }}
    .links {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 10px;
    }}
    .btn {{
      display: inline-block;
      border: 1px solid rgba(255,255,255,0.10);
      background: rgba(255,255,255,0.02);
      color: #e5e7eb;
      padding: 10px 12px;
      border-radius: 14px;
      text-decoration: none;
      font-size: 13px;
    }}
    .btn:hover {{ background: rgba(255,255,255,0.05); }}
    code {{
      background: rgba(255,255,255,0.06);
      padding: 2px 6px;
      border-radius: 10px;
      font-size: 12px;
    }}
    .mini {{
      margin-top: 12px;
      font-size: 13px;
      color: #cbd5e1;
      line-height: 1.5;
    }}
    .mini a {{ color: #93c5fd; text-decoration: none; }}
    .mini a:hover {{ text-decoration: underline; }}

    .full {{
      grid-column: span 12;
      border: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.03);
      border-radius: 20px;
      padding: 18px;
    }}
    .flow {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 12px;
      align-items: center;
    }}
    .step {{
      border: 1px solid rgba(255,255,255,0.10);
      background: rgba(255,255,255,0.02);
      border-radius: 14px;
      padding: 10px 12px;
      font-size: 13px;
      color: #cbd5e1;
    }}
    .arrow {{
      color: #64748b;
      font-size: 14px;
    }}
    ul {{
      margin: 10px 0 0;
      padding-left: 18px;
      color: #cbd5e1;
      line-height: 1.55;
    }}
    a {{ color: #93c5fd; }}
    @media (max-width: 900px) {{
      .card {{ grid-column: span 12; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>We turn real-world facts into verifiable, machine-readable truth.</h1>
      <p class="one">
        XERTIFY provides a verifiable source of truth for assets, ownership, and compliance —
        so systems can trust data instead of people.
      </p>

      <div class="pillrow">
        <div class="pill">No new engine per industry</div>
        <div class="pill">New vertical = schema + policy + event vocabulary</div>
        <div class="pill">Proof bundles are verifiable offline</div>
        <div class="pill">Time-based truth (evaluated_at)</div>
      </div>
    </div>

    <div class="grid">
      <div class="full">
        <div class="kicker">HOW IT WORKS</div>
        <h2>One engine. Many verticals. Your truth.</h2>
        <div class="flow">
          <div class="step">Event comes in (human / sensor / signature)</div>
          <div class="arrow">→</div>
          <div class="step">Authority is checked</div>
          <div class="arrow">→</div>
          <div class="step">Policy evaluates truth at <code>evaluated_at</code></div>
          <div class="arrow">→</div>
          <div class="step">Verdict + state update</div>
          <div class="arrow">→</div>
          <div class="step">Proof bundle updates</div>
          <div class="arrow">→</div>
          <div class="step">Partners consume via API / UI / offline verify</div>
        </div>

        <ul>
          <li><b>Partners don’t trust us.</b> They verify hashes, schemas, and policy identifiers.</li>
          <li><b>Regulators get auditability.</b> “Who said what, when, under which policy?”</li>
          <li><b>Enterprises get automation.</b> Systems can gate actions based on verifiable truth.</li>
        </ul>
      </div>

      {_v_card(aviation)}
      {_v_card(pharma)}

      <div class="full">
        <div class="kicker">OFFLINE VERIFICATION (HIGH LEVEL)</div>
        <h2>Verify truth without the UI, without trusting XERTIFY</h2>
        <ul>
          <li>Download a proof bundle (<code>/v/&lt;pass_id&gt;/proof.json</code> or a published example).</li>
          <li>Validate the snapshot against the published JSON schema (<code>/spec/&lt;vertical&gt;.json</code>).</li>
          <li>Confirm the policy hash (<code>policy_sha256</code>) matches the published policy doc.</li>
          <li>Recompute the proof hashes (<code>bundle_sha256</code>, <code>verdict_inputs_sha256</code>).</li>
          <li>Optionally verify the proof chain linkage (<code>proof_chain</code>) for tamper evidence.</li>
        </ul>
        <p class="muted" style="margin-top:12px;">
          Full offline steps and example links live inside each vertical spec JSON:
          <a href="/spec/aviation.json">/spec/aviation.json</a> and <a href="/spec/pharma.json">/spec/pharma.json</a>.
        </p>
      </div>

      <div class="full">
        <div class="kicker">WHY THIS SCALES</div>
        <h2>Infrastructure, not bespoke software</h2>
        <ul>
          <li>New industry ≠ new engine.</li>
          <li>New industry = new <b>object schema</b>, <b>event vocabulary</b>, and <b>policy pack</b>.</li>
          <li>That means faster onboarding, consistent verification, and long-term interoperability.</li>
        </ul>
      </div>
    </div>
  </div>
</body>
</html>
""" )
