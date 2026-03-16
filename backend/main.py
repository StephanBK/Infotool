"""
INOVUES NYC Commercial Property Ownership Tool
Exhaustive public records lookup: ACRIS → NYS DOS LLC Pierce → Weekly Feed
"""
from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
import httpx
import asyncio
import pandas as pd
import io
import json
import re
import os
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

app = FastAPI(title="INOVUES Ownership Tool", version="1.0.0")

@app.on_event("startup")
async def startup_event():
    import sys
    print(f"=== INOVUES starting ===", flush=True)
    print(f"cwd: {Path.cwd()}", flush=True)
    for p in [Path(__file__).parent.parent / "frontend", Path("/app/frontend")]:
        print(f"frontend {p}: {p.exists()}", flush=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── CONSTANTS ────────────────────────────────────────────────────────────────
# Field names confirmed via /api/debug/acris on 2026-03-15:
#   master:  document_id, doc_type, recorded_datetime, document_amt, recorded_borough
#   parties: document_id, party_type, name, address_1, city, state, zip (use _mod)
#   legals:  broken - use PLUTO for BBL lookup instead
ACRIS_MASTER_URL   = "https://data.cityofnewyork.us/resource/bnx9-e6tj.json"
ACRIS_LEGALS_URL   = "https://data.cityofnewyork.us/resource/8h5j-fqxa.json"  # broken, avoid
ACRIS_PARTIES_URL  = "https://data.cityofnewyork.us/resource/8yfw-gfkq.json"  # parties_mod - has addresses
ACRIS_PARTIES_BASIC_URL = "https://data.cityofnewyork.us/resource/636b-3b5g.json"  # fallback
NYC_GEOCLIENT_URL  = "https://api.nyc.gov/geo/geoclient/v1"
LOCATE_NYC_URL     = "https://geocoding.geo.census.gov/geocoder/locations/address"
DOS_SEARCH_URL     = "https://apps.dos.ny.gov/publicInquiry/"
PLUTO_URL          = "https://data.cityofnewyork.us/resource/64uk-42ks.json"
# NOTE: ACRIS Open Data lags ~2-4 weeks. recorded_datetime max is ~Feb 28 as of Mar 15.
# NOTE: field is "document_id" NOT "documentid" - critical for joins

BOROUGH_MAP = {
    "manhattan": "1", "bronx": "2", "brooklyn": "3", "queens": "4", "staten island": "5",
    "mn": "1", "bx": "2", "bk": "3", "qn": "4", "si": "5",
    "new york": "1", "ny": "1",
    "1": "1", "2": "2", "3": "3", "4": "4", "5": "5",
}

DEED_DOC_TYPES = ["DEED", "DEEDO", "DEED, BARGAIN AND SALE", "DEED IN LIEU", "EXECUTOR DEED",
                  "ADMINISTRATOR DEED", "REFEREE DEED", "COMMISSIONER DEED", "TRUSTEES DEED",
                  "DEED, GIFT", "DEED, CORRECTION", "DEED, LIFE ESTATE", "QUITCLAIM DEED",
                  "GRANT DEED"]


# ─── HELPERS ──────────────────────────────────────────────────────────────────
def normalize_bbl(borough: str, block: str, lot: str) -> str:
    b = BOROUGH_MAP.get(borough.lower().strip(), borough)
    return f"{b}{block.zfill(5)}{lot.zfill(4)}"


def parse_bbl_string(bbl: str) -> dict:
    """Parse a 10-digit BBL string into components."""
    bbl = bbl.strip().replace("-", "").replace(" ", "")
    if len(bbl) == 10:
        return {"borough": bbl[0], "block": bbl[1:6], "lot": bbl[6:10]}
    raise ValueError(f"Invalid BBL: {bbl}")


async def soda_get(client: httpx.AsyncClient, url: str, params: dict) -> list:
    """Query NYC Open Data Socrata API with $limit."""
    params.setdefault("$limit", 1000)
    token = os.getenv("NYC_APP_TOKEN", "")
    if token:
        params["$$app_token"] = token
    try:
        r = await client.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"SODA error {url}: {e}")
        return []


# ─── STEP 1: ADDRESS → BBL ────────────────────────────────────────────────────
async def address_to_bbl(address: str, borough: str, client: httpx.AsyncClient) -> Optional[dict]:
    """Try multiple geocoders to resolve address → BBL."""
    
    # Strategy 1: NYC PLUTO via Open Data (address lookup)
    parts = address.strip().split()
    if parts:
        house_num = parts[0]
        street = " ".join(parts[1:])
        boro_code = BOROUGH_MAP.get(borough.lower().strip(), "")
        
        pluto_rows = await soda_get(client, PLUTO_URL, {
            "$where": f"address like '{house_num} {street.upper()[:20]}%' AND borough='{boro_code}'",
            "$limit": 5,
            "$select": "bbl,address,borough,zipcode,bldgclass,ownertype"
        })
        if pluto_rows:
            row = pluto_rows[0]
            bbl = row.get("bbl", "")
            if bbl and len(str(bbl)) >= 10:
                return {"bbl": str(bbl).zfill(10), "address": row.get("address"), 
                        "borough": row.get("borough"), "source": "PLUTO"}

    # Strategy 2: LocateNYC (no auth required)
    try:
        r = await client.get(
            "https://locatenyc.io/arcgis/rest/services/Locators/LocateNYC/GeocodeServer/findAddressCandidates",
            params={"SingleLine": f"{address}, {borough}, NY", "outFields": "bbl,bblBoroughCode,bblTaxBlock,bblTaxLot", "f": "json"},
            timeout=15
        )
        data = r.json()
        candidates = data.get("candidates", [])
        if candidates:
            attrs = candidates[0].get("attributes", {})
            bbl = attrs.get("bbl", "")
            if bbl:
                return {"bbl": str(bbl).zfill(10), "address": candidates[0].get("address"),
                        "source": "LocateNYC"}
    except Exception as e:
        print(f"LocateNYC error: {e}")

    return None


# ─── STEP 2: BBL → ACRIS DEED HISTORY ────────────────────────────────────────
async def get_deed_history(bbl_str: str, client: httpx.AsyncClient) -> list:
    """Get all deed transfers for a BBL from ACRIS."""
    parts = parse_bbl_string(bbl_str)
    
    # Query legals table to find all doc_ids for this BBL
    legals = await soda_get(client, ACRIS_LEGALS_URL, {
        "$where": f"borough='{parts['borough']}' AND block='{parts['block'].lstrip('0') or '0'}' AND lot='{parts['lot'].lstrip('0') or '0'}'",
        "$select": "document_id,doc_type",
        "$limit": 500,
    })
    
    if not legals:
        # Try with zero-padded
        legals = await soda_get(client, ACRIS_LEGALS_URL, {
            "$where": f"borough='{parts['borough']}' AND block='{parts['block']}' AND lot='{parts['lot']}'",
            "$select": "document_id,doc_type",
            "$limit": 500,
        })

    deed_doc_ids = [r["document_id"] for r in legals 
                    if r.get("doc_type", "").upper() in [d.upper() for d in DEED_DOC_TYPES]]
    
    if not deed_doc_ids:
        # Fall back: get ALL doc IDs and filter by master
        deed_doc_ids = [r["document_id"] for r in legals]

    if not deed_doc_ids:
        return []

    # Batch fetch master records for deed doc_ids
    results = []
    batch_size = 50
    for i in range(0, min(len(deed_doc_ids), 200), batch_size):
        batch = deed_doc_ids[i:i+batch_size]
        id_list = ",".join(f"'{d}'" for d in batch)
        masters = await soda_get(client, ACRIS_MASTER_URL, {
            "$where": f"document_id in ({id_list}) AND doc_type in ('DEED','DEEDO','DEED, BARGAIN AND SALE','DEED IN LIEU','QUITCLAIM DEED','EXECUTOR DEED','TRUSTEES DEED','ADMINISTRATOR DEED','REFEREE DEED')",
            "$select": "document_id,doc_type,document_amt,recorded_datetime,doc_date,crfn",
            "$order": "recorded_datetime DESC",
            "$limit": batch_size,
        })
        results.extend(masters)

    return results


# ─── STEP 3: DOC_IDs → PARTIES (Grantor/Grantee) ─────────────────────────────
async def get_parties_for_docs(doc_ids: list, client: httpx.AsyncClient) -> list:
    """Fetch grantor and grantee names for given document IDs."""
    if not doc_ids:
        return []
    
    all_parties = []
    batch_size = 50
    for i in range(0, min(len(doc_ids), 100), batch_size):
        batch = doc_ids[i:i+batch_size]
        id_list = ",".join(f"'{d}'" for d in batch)
        parties = await soda_get(client, ACRIS_PARTIES_URL, {
            "$where": f"document_id in ({id_list})",
            "$select": "document_id,party_type,name,address_1,address_2,city,state,zip,country",
            "$limit": 500,
        })
        all_parties.extend(parties)
    return all_parties


# ─── STEP 4: LLC PIERCE via NYS DOS ───────────────────────────────────────────
async def pierce_llc(entity_name: str, client: httpx.AsyncClient) -> dict:
    """
    Attempt to pierce LLC veil via NYS DOS public inquiry.
    Returns registered agent, address, CEO, principal office.
    """
    result = {
        "entity_name": entity_name,
        "dos_status": None,
        "registered_agent": None,
        "ceo_name": None,
        "principal_address": None,
        "process_address": None,
        "date_formed": None,
        "dos_id": None,
        "pierced_data": []
    }
    
    if not entity_name or len(entity_name.strip()) < 3:
        return result

    # Check if it looks like an LLC/Corp
    is_entity = any(kw in entity_name.upper() for kw in 
                    ["LLC", "L.L.C", "INC", "CORP", "LP", "LTD", "TRUST", "ASSOCIATES", "REALTY", "PROPERTIES"])
    
    try:
        # NYS DOS public search API (undocumented but public)
        search_url = "https://apps.dos.ny.gov/publicInquiry/api/search"
        r = await client.post(search_url, 
            json={"searchTerm": entity_name.strip(), "searchType": "EntityName"},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=20
        )
        if r.status_code == 200:
            data = r.json()
            entities = data.get("entityList", data.get("results", []))
            if entities:
                best = entities[0]
                result["dos_id"] = best.get("dosId") or best.get("entityId")
                result["dos_status"] = best.get("entityStatus") or best.get("status")
                result["date_formed"] = best.get("initialDosFilingDate") or best.get("dateFormed")
                result["process_address"] = best.get("processAddress") or best.get("principalAddress")
                
                # Try to get detail
                if result["dos_id"]:
                    detail_url = f"https://apps.dos.ny.gov/publicInquiry/api/entity/{result['dos_id']}"
                    dr = await client.get(detail_url, timeout=15)
                    if dr.status_code == 200:
                        detail = dr.json()
                        result["ceo_name"] = detail.get("ceoName") or detail.get("chiefExecutiveOfficer")
                        result["principal_address"] = detail.get("principalOfficeAddress")
                        result["registered_agent"] = detail.get("registeredAgent")
                        result["pierced_data"] = detail
    except Exception as e:
        print(f"DOS API error: {e}")

    # Fallback: scrape DOS public search page
    if not result["dos_id"]:
        try:
            r = await client.get(
                "https://apps.dos.ny.gov/publicInquiry/",
                params={"SEARCH_TYPE": "ENT", "ENT_NAME": entity_name},
                timeout=20,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; INOVUES/1.0)"}
            )
            if r.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(r.text, "lxml")
                
                # Look for table rows with entity data
                rows = soup.find_all("tr")
                for row in rows[:10]:
                    cells = row.find_all("td")
                    if len(cells) >= 3:
                        cell_text = [c.get_text(strip=True) for c in cells]
                        if any(entity_name.upper()[:10] in t.upper() for t in cell_text):
                            result["dos_status"] = "Found (scrape)"
                            result["process_address"] = " ".join(cell_text)
                            # Try to find link to details
                            link = row.find("a")
                            if link and link.get("href"):
                                result["dos_id"] = link.get("href")
                            break
        except Exception as e:
            print(f"DOS scrape fallback error: {e}")

    return result


# ─── STEP 5: PLUTO enrichment ─────────────────────────────────────────────────
async def get_pluto_data(bbl: str, client: httpx.AsyncClient) -> dict:
    """Get building characteristics from MapPLUTO."""
    rows = await soda_get(client, PLUTO_URL, {
        "$where": f"bbl='{bbl}'",
        "$select": "bbl,address,borough,zipcode,bldgclass,landuse,yearbuilt,numfloors,unitsres,unitstotal,lotarea,bldgarea,ownertype,ownername",
        "$limit": 1,
    })
    return rows[0] if rows else {}


# ─── MAIN LOOKUP ENDPOINT ─────────────────────────────────────────────────────
@app.get("/api/lookup")
async def lookup_property(
    address: Optional[str] = Query(None, description="Full address e.g. '350 Fifth Avenue'"),
    borough:  Optional[str] = Query(None, description="Borough e.g. 'manhattan'"),
    bbl:      Optional[str] = Query(None, description="10-digit BBL e.g. '1008340001'"),
):
    if not bbl and not address:
        raise HTTPException(400, "Provide either 'bbl' or 'address' + 'borough'")
    
    async with httpx.AsyncClient() as client:
        resolved_bbl = bbl
        geo_info = {}

        # Resolve address → BBL if needed
        if not bbl and address:
            boro = borough or "manhattan"
            geo_info = await address_to_bbl(address, boro, client)
            if not geo_info:
                raise HTTPException(404, f"Could not resolve address '{address}' to a BBL. Try providing BBL directly.")
            resolved_bbl = geo_info["bbl"]

        # Validate BBL
        try:
            bbl_parts = parse_bbl_string(resolved_bbl)
        except ValueError as e:
            raise HTTPException(400, str(e))

        # Run all lookups concurrently
        deed_history, pluto_data = await asyncio.gather(
            get_deed_history(resolved_bbl, client),
            get_pluto_data(resolved_bbl, client),
        )

        # Get parties for top deeds
        top_doc_ids = [d["documentid"] for d in deed_history[:20] if d.get("document_id")]
        parties = await get_parties_for_docs(top_doc_ids, client)

        # Organize parties by document
        parties_by_doc = {}
        for p in parties:
            did = p.get("document_id")
            if did not in parties_by_doc:
                parties_by_doc[did] = {"grantors": [], "grantees": []}
            ptype = str(p.get("party_type", "")).strip()
            name = p.get("name", "").strip()
            addr = " ".join(filter(None, [p.get("address_1"), p.get("city"), p.get("state"), p.get("zip")]))
            entry = {"name": name, "address": addr}
            if ptype == "1":
                parties_by_doc[did]["grantors"].append(entry)
            elif ptype == "2":
                parties_by_doc[did]["grantees"].append(entry)

        # Enrich deed history with parties
        enriched_deeds = []
        for deed in deed_history[:20]:
            did = deed.get("document_id")
            doc_parties = parties_by_doc.get(did, {"grantors": [], "grantees": []})
            enriched_deeds.append({**deed, **doc_parties})

        # Current owner = most recent grantee
        current_owner_name = None
        current_owner_raw = pluto_data.get("ownername", "")
        
        if enriched_deeds:
            latest = enriched_deeds[0]
            grantees = latest.get("grantees", [])
            if grantees:
                current_owner_name = grantees[0]["name"]

        if not current_owner_name:
            current_owner_name = current_owner_raw

        # Pierce the LLC
        llc_data = {}
        if current_owner_name:
            llc_data = await pierce_llc(current_owner_name, client)

        # Also try piercing any other LLC names found in recent deeds
        additional_entities = []
        for deed in enriched_deeds[:5]:
            for g in deed.get("grantees", []) + deed.get("grantors", []):
                n = g.get("name", "")
                if n and n != current_owner_name and any(kw in n.upper() for kw in ["LLC","INC","CORP","LP","TRUST"]):
                    additional_entities.append(n)
        
        additional_pierced = []
        for name in list(set(additional_entities))[:3]:
            pierced = await pierce_llc(name, client)
            additional_pierced.append(pierced)

        return {
            "bbl": resolved_bbl,
            "bbl_parts": bbl_parts,
            "address_resolved": geo_info.get("address") or pluto_data.get("address") or address,
            "building_info": pluto_data,
            "current_owner": {
                "name_raw": current_owner_name,
                "llc_pierce": llc_data,
            },
            "deed_history": enriched_deeds,
            "additional_entities": additional_pierced,
            "data_sources": ["NYC ACRIS", "MapPLUTO", "NYS DOS"],
            "timestamp": datetime.utcnow().isoformat(),
        }


# ─── CSV EXPORT ───────────────────────────────────────────────────────────────
@app.get("/api/lookup/export")
async def export_property_csv(
    address: Optional[str] = Query(None),
    borough: Optional[str] = Query(None),
    bbl:     Optional[str] = Query(None),
):
    data = await lookup_property(address=address, borough=borough, bbl=bbl)
    
    rows = []
    current = data["current_owner"]
    llc = current.get("llc_pierce", {})
    
    for deed in data.get("deed_history", []):
        for grantee in deed.get("grantees", []):
            rows.append({
                "BBL": data["bbl"],
                "Address": data.get("address_resolved"),
                "Building Class": data["building_info"].get("bldgclass"),
                "Year Built": data["building_info"].get("yearbuilt"),
                "Floors": data["building_info"].get("numfloors"),
                "Lot Area (sqft)": data["building_info"].get("lotarea"),
                "Building Area (sqft)": data["building_info"].get("bldgarea"),
                "Transaction Date": deed.get("recorded_datetime", deed.get("doc_date")),
                "Document Type": deed.get("doc_type"),
                "Sale Amount": deed.get("document_amt"),
                "Grantee (Buyer)": grantee.get("name"),
                "Grantee Address": grantee.get("address"),
                "Grantor (Seller)": ", ".join(g["name"] for g in deed.get("grantors", [])),
                "DOS Status": llc.get("dos_status"),
                "DOS ID": llc.get("dos_id"),
                "Registered Agent": llc.get("registered_agent"),
                "CEO / Principal": llc.get("ceo_name"),
                "Principal Address": llc.get("principal_address") or llc.get("process_address"),
                "Date Entity Formed": llc.get("date_formed"),
                "CRFN": deed.get("crfn"),
                "Data Sources": "ACRIS, PLUTO, NYS DOS",
                "Export Timestamp": data["timestamp"],
            })
    
    if not rows:
        rows.append({
            "BBL": data["bbl"],
            "Address": data.get("address_resolved"),
            "Building Class": data["building_info"].get("bldgclass"),
            "Current Owner (Raw)": current.get("name_raw"),
            "DOS Status": llc.get("dos_status"),
            "DOS ID": llc.get("dos_id"),
            "Registered Agent": llc.get("registered_agent"),
            "CEO / Principal": llc.get("ceo_name"),
            "Principal Address": llc.get("principal_address") or llc.get("process_address"),
        })
    
    df = pd.DataFrame(rows)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    
    filename = f"inovues_ownership_{data['bbl']}_{datetime.now().strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ─── WEEKLY OWNERSHIP CHANGES FEED ───────────────────────────────────────────
@app.get("/api/weekly-feed")
async def get_weekly_feed(
    days: int = Query(7, description="Number of days back to look"),
    doc_type: str = Query("DEED", description="Document type filter"),
    borough: Optional[str] = Query(None, description="Filter by borough (1-5)"),
    min_amount: Optional[int] = Query(None, description="Minimum sale price"),
):
    """
    Returns a list of NYC commercial property transfers from the last N days.
    ACRIS Open Data lags ~2-4 weeks, so we search back further to guarantee results.
    """
    # Guarantee at least 45 days back to cover ACRIS lag
    effective_days = max(days, 45)
    since_date = (datetime.utcnow() - timedelta(days=effective_days)).strftime("%Y-%m-%dT%H:%M:%S.000")

    async with httpx.AsyncClient() as client:
        where = f"recorded_datetime >= '{since_date}'"
        if min_amount:
            where += f" AND document_amt >= {min_amount}"
        if borough:
            where += f" AND recorded_borough='{borough}'"

        masters = await soda_get(client, ACRIS_MASTER_URL, {
            "$where": where,
            "$order": "recorded_datetime DESC",
            "$limit": 500,
            "$select": "document_id,doc_type,document_amt,recorded_datetime,document_date,crfn,recorded_borough"
        })

        # Filter to deed types in Python (no doc_class field in this dataset)
        deed_types = {"DEED","DEEDO","DEED, BARGAIN AND SALE","QUITCLAIM DEED",
                     "DEED IN LIEU","EXECUTOR DEED","TRUSTEES DEED","REFEREE DEED",
                     "ADMINISTRATOR DEED","DEED, GIFT","DEED, CORRECTION"}
        masters = [m for m in masters if m.get("doc_type","").upper() in deed_types]

        if not masters:
            return {"count": 0, "transactions": [],
                    "note": f"No deed records found. Searched back {effective_days} days from {since_date}.",
                    "generated_at": datetime.utcnow().isoformat()}

        doc_ids = [m["document_id"] for m in masters if m.get("document_id")]

        # Fetch legals (BBL) and parties in parallel
        legals_list, parties_list = await asyncio.gather(
            _batch_fetch_legals(doc_ids, client, borough),
            _batch_fetch_parties(doc_ids, client),
        )

        # Index by document ID
        legals_by_doc = {}
        for l in legals_list:
            did = l.get("document_id")
            if did:
                legals_by_doc.setdefault(did, []).append(l)

        parties_by_doc = {}
        for p in parties_list:
            did = p.get("document_id")
            if did:
                parties_by_doc.setdefault(did, {"grantors": [], "grantees": []})
                ptype = str(p.get("party_type", "")).strip()
                name = p.get("name", "").strip()
                addr = " ".join(filter(None, [p.get("address_1"), p.get("city"), p.get("state")]))
                entry = {"name": name, "address": addr}
                if ptype == "1":
                    parties_by_doc[did]["grantors"].append(entry)
                elif ptype == "2":
                    parties_by_doc[did]["grantees"].append(entry)

        # Build final list
        transactions = []
        for master in masters:
            did = master["document_id"]
            legs = legals_by_doc.get(did, [{}])
            leg = legs[0] if legs else {}
            parts = parties_by_doc.get(did, {"grantors": [], "grantees": []})
            
            # Construct BBL — prefer legals data, fall back to recorded_borough from master
            b  = leg.get("borough","") or master.get("recorded_borough","")
            bl = leg.get("block","")
            lt = leg.get("lot","")
            bbl_str = normalize_bbl(b, bl.zfill(5), lt.zfill(4)) if b and bl and lt else ""

            transactions.append({
                "document_id": did,
                "bbl": bbl_str,
                "borough": b,
                "borough_name": ["","Manhattan","Bronx","Brooklyn","Queens","Staten Island"][int(b)] if b and b.isdigit() and int(b) <= 5 else b,
                "block": bl,
                "lot": lt,
                "property_type": leg.get("property_type",""),
                "doc_type": master.get("doc_type"),
                "sale_amount": master.get("document_amt"),
                "recorded_date": master.get("recorded_datetime"),
                "document_date": master.get("document_date"),
                "crfn": master.get("crfn"),
                "buyer": parts["grantees"][0]["name"] if parts["grantees"] else None,
                "buyer_address": parts["grantees"][0]["address"] if parts["grantees"] else None,
                "seller": parts["grantors"][0]["name"] if parts["grantors"] else None,
                "seller_address": parts["grantors"][0]["address"] if parts["grantors"] else None,
                "acris_url": f"https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentImageView?doc_id={did}" if did else None,
            })

        return {
            "count": len(transactions),
            "date_range": {"from": since_date, "to": datetime.utcnow().isoformat()},
            "transactions": transactions,
            "generated_at": datetime.utcnow().isoformat(),
        }


async def _batch_fetch_legals(doc_ids: list, client: httpx.AsyncClient, borough_filter: Optional[str]) -> list:
    """
    ACRIS legals dataset (8h5j-fqxa) returns 0 rows — appears broken/deprecated.
    Fallback: query the ACRIS real property legals via a different known-good approach.
    We try multiple dataset IDs for legals.
    """
    LEGALS_URLS = [
        "https://data.cityofnewyork.us/resource/8h5j-fqxa.json",   # original
        "https://data.cityofnewyork.us/resource/i6gc-xnbv.json",   # alternate view
    ]
    all_legals = []
    batch_size = 50
    for i in range(0, min(len(doc_ids), 500), batch_size):
        batch = doc_ids[i:i+batch_size]
        id_list = ",".join(f"'{d}'" for d in batch)
        where = f"document_id in ({id_list})"
        if borough_filter:
            where += f" AND borough='{borough_filter}'"
        found = False
        for url in LEGALS_URLS:
            rows = await soda_get(client, url, {
                "$where": where,
                "$select": "document_id,borough,block,lot,property_type",
                "$limit": batch_size * 2,
            })
            if rows:
                all_legals.extend(rows)
                found = True
                break
        # If still nothing, try without select (maybe field names differ)
        if not found:
            rows = await soda_get(client, LEGALS_URLS[0], {
                "$where": where,
                "$limit": batch_size * 2,
            })
            all_legals.extend(rows)
    return all_legals


async def _batch_fetch_parties(doc_ids: list, client: httpx.AsyncClient) -> list:
    all_parties = []
    batch_size = 50  # smaller batches — parties_mod has more fields
    for i in range(0, min(len(doc_ids), 500), batch_size):
        batch = doc_ids[i:i+batch_size]
        id_list = ",".join(f"'{d}'" for d in batch)
        # Try parties_mod first (has full address), fall back to basic
        rows = await soda_get(client, ACRIS_PARTIES_URL, {
            "$where": f"document_id in ({id_list})",
            "$select": "document_id,party_type,name,address_1,city,state,zip",
            "$limit": batch_size * 4,
        })
        if not rows:
            rows = await soda_get(client, ACRIS_PARTIES_BASIC_URL, {
                "$where": f"document_id in ({id_list})",
                "$select": "document_id,party_type,name",
                "$limit": batch_size * 4,
            })
        all_parties.extend(rows)
    return all_parties


# ─── WEEKLY FEED CSV EXPORT ───────────────────────────────────────────────────
@app.get("/api/weekly-feed/export")
async def export_weekly_feed_csv(
    days: int = Query(7),
    borough: Optional[str] = Query(None),
    min_amount: Optional[int] = Query(None),
):
    data = await get_weekly_feed(days=days, borough=borough, min_amount=min_amount)
    
    df = pd.DataFrame(data["transactions"])
    if df.empty:
        df = pd.DataFrame(columns=["documentid","bbl","borough","block","lot","doc_type",
                                    "sale_amount","recorded_date","buyer","seller","acris_url"])
    
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    
    filename = f"inovues_nyc_transfers_{datetime.now().strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ─── HEALTH CHECK ─────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "1.0.0", "tool": "INOVUES Ownership Tool"}


# ─── DEBUG: raw ACRIS query ───────────────────────────────────────────────────
@app.get("/api/debug/acris")
async def debug_acris(days: int = 7):
    """
    Tests all known ACRIS dataset IDs and returns raw results.
    Use this to find which datasets are live and what fields they have.
    """
    since_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000")

    # All known dataset IDs — old and new
    datasets = {
        "master_old":   "https://data.cityofnewyork.us/resource/bnx9-e6tj.json",
        "master_v2":    "https://data.cityofnewyork.us/resource/9uxe-2pis.json",
        "legals_old":   "https://data.cityofnewyork.us/resource/8h5j-fqxa.json",
        "parties_old":  "https://data.cityofnewyork.us/resource/636b-3b5g.json",
        "parties_mod":  "https://data.cityofnewyork.us/resource/8yfw-gfkq.json",  # modified view
    }

    results = {}
    async with httpx.AsyncClient() as client:
        for name, url in datasets.items():
            # No ordering — just get fields first
            rows = await soda_get(client, url, {"$limit": 3})
            fields = list(rows[0].keys()) if rows else []
            results[name] = {
                "url": url,
                "row_count_returned": len(rows),
                "field_names": fields,
                "sample_row": rows[0] if rows else None,
            }
            # Find date field and test filtering
            date_field = next((f for f in ["recorded_datetime","doc_date","modified_date","good_through_date"] if f in fields), None)
            if date_field:
                dated = await soda_get(client, url, {
                    "$where": f"{date_field} >= '{since_date}'",
                    "$order": f"{date_field} DESC",
                    "$limit": 3,
                })
                results[name]["date_field"] = date_field
                results[name]["rows_with_date_filter"] = len(dated)
                results[name]["date_sample"] = dated[0] if dated else None
            else:
                results[name]["date_field"] = "NOT FOUND"

    return {
        "since_date": since_date,
        "datasets": results,
    }


# ─── SERVE FRONTEND (MUST BE LAST) ────────────────────────────────────────────
_possible = [
    Path(__file__).parent.parent / "frontend",
    Path("/app/frontend"),
    Path(__file__).parent / "frontend",
]
_frontend = next((p for p in _possible if p.exists()), None)
if _frontend:
    app.mount("/", StaticFiles(directory=str(_frontend), html=True), name="static")
else:
    @app.get("/")
    async def root():
        return {"message": "INOVUES API running. Frontend not found.", "docs": "/docs"}
