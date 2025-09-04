# requirements: pip install requests pandas python-dotenv
import os, csv, requests, pandas as pd
from dotenv import load_dotenv

load_dotenv()
APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")  # put your key in .env
BASE = "https://api.apollo.io"

INPUT_CSV = "input_leads.csv"   # coloans: email OR (first_name,last_name,company)
OUT_CSV   = "apollo_leads_export.csv"
OUT_TXT   = "apollo_leads_export.txt"

headers = {"Content-Type":"application/json","Cache-Control":"no-cache","X-Api-Key": APOLLO_API_KEY}

def enrich_person(payload):
    # People Enrichment (match by email or name+company) – API oficial
    # Docs: /api/v1/people/match
    url = f"{BASE}/api/v1/people/match"
    r = requests.post(url, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

rows = []
with open(INPUT_CSV, newline='', encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for i, rec in enumerate(reader, start=1):
        payload = {}
        if rec.get("email"):
            payload["email"] = rec["email"]
        else:
            payload = {
                "first_name": rec.get("first_name",""),
                "last_name": rec.get("last_name",""),
                "organization_name": rec.get("company","")
            }
        try:
            data = enrich_person(payload)
            p = data.get("person", {}) or {}
            org = p.get("organization", {}) or {}
            rows.append({
                "full_name": p.get("name") or f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                "job_title": p.get("title"),
                "email": (p.get("email") or p.get("email_status")), 
                "company_name": org.get("name"),
                "employer_name": org.get("name"),           
                "employer_job_title": p.get("title"),       
                "linkedin_url": p.get("linkedin_url"),
                "company_website": org.get("website_url"),
            })
        except requests.HTTPError as e:
            rows.append({"full_name":"","job_title":"","email":"","company_name":"","employer_name":"","employer_job_title":"","error":str(e)})

# write CSV + TXT
df = pd.DataFrame(rows)
df.to_csv(OUT_CSV, index=False, encoding="utf-8")
with open(OUT_TXT, "w", encoding="utf-8") as f:
    for r in rows:
        f.write(f"{r.get('full_name','')} | {r.get('job_title','')} | {r.get('email','')} | {r.get('company_name','')}\n")

print(f"Done. Wrote {len(rows)} rows to {OUT_CSV} and {OUT_TXT}.")
