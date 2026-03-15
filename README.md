# INOVUES NYC Ownership Intelligence Tool

Pierce LLC ownership structures on NYC commercial properties using 100% public records.

## What it does

**Property Lookup:**
- Input: Address + Borough OR BBL (10-digit)
- Resolves address → BBL via MapPLUTO / LocateNYC
- Fetches full deed history from ACRIS (NYC Open Data)
- Gets building characteristics from MapPLUTO
- Attempts LLC piercing via NYS DOS public inquiry
- Export everything as CSV

**Weekly Transfer Feed:**
- Returns all deed transfers in last N days from ACRIS
- Filterable by borough + minimum sale amount
- Click any BBL to drill into full ownership lookup
- Export full list as CSV for CRM import

## Data Sources

| Source | What | API |
|---|---|---|
| NYC ACRIS (Open Data) | Deed documents, parties | data.cityofnewyork.us SODA API |
| MapPLUTO | Building info, owner names | data.cityofnewyork.us SODA API |
| NYS DOS | LLC registered agent, CEO, address | apps.dos.ny.gov (public) |
| LocateNYC | Address to BBL geocoding | locatenyc.io (free) |

## Local Development

```bash
pip install -r backend/requirements.txt
uvicorn backend.main:app --reload --port 8000
# Open http://localhost:8000
```

Optional: set NYC_APP_TOKEN env var from data.cityofnewyork.us for higher rate limits.

## Railway Deployment

1. Push this repo to GitHub (StephanBK/inovues-ownership-tool)
2. Go to railway.app → New Project → Deploy from GitHub
3. Select this repo — Railway auto-detects railway.toml
4. Add optional env var: NYC_APP_TOKEN=your_token
5. Deploy and get public URL

## Known Limitations

- NYS DOS LLC piercing: shows registered agent + CEO if biennial statement filed.
  Single-member LLCs often only show the registered agent (law firm).
  As of 1/1/26 foreign LLCs must disclose beneficial owners.
- ACRIS records start 1966.
- Staten Island deeds in Richmond County Clerk, not ACRIS — partial coverage.
- SODA API: ~1000 req/hr unauthenticated. Token raises to ~10,000/hr.
