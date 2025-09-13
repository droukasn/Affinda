import pandas as pd
import json
import requests
from tqdm import tqdm
import time

API_KEY = "REPLACE_ME"              # Affinda API key
COLLECTION_ID = "REPLACE_COLLECTION" # The target collection identifier (not workspace)
EXCEL_FILE_PATH = "your_cv_data.xlsx" # Path to your Excel file
BASE_URL = "https://api.affinda.com"  # Change if using a regional hostname

def map_row(row):
    resume = {}
    if pd.notna(row.get('fullName')): resume['candidateName'] = str(row['fullName'])
    if pd.notna(row.get('email')): resume['email'] = str(row['email'])
    if pd.notna(row.get('citizenship')): resume['nationality'] = str(row['citizenship'])
    if pd.notna(row.get('totalExperience')):
        try: resume['totalYearsExperience'] = int(row['totalExperience'])
        except ValueError: pass
    skills = []
    for col in ['Skills','IT_Skills','Themes']:
        if pd.notna(row.get(col)):
            skills += [s.strip() for s in str(row[col]).replace(',', ';').split(';') if s.strip()]
    if skills: resume['skill'] = skills
    if pd.notna(row.get('Languages')):
        langs = [l.strip() for l in str(row['Languages']).replace(',', ';').split(';') if l.strip()]
        if langs: resume['language'] = langs
    edu = []
    if pd.notna(row.get('education_highestLevel')):
        degree_field = {
            'degree': str(row['education_highestLevel']),
            'field': str(row.get('education_mainFieldOfStudy', '')) if pd.notna(row.get('education_mainFieldOfStudy')) else None
        }
        edu.append(degree_field)
    if edu: resume['education'] = edu
    parts = []
    for col in ['summary_short','summary_professional','summary_roleTitle']:
        if pd.notna(row.get(col)): parts.append(str(row[col]))
    if parts: resume['summary'] = ' | '.join(parts)
    custom = {}
    for col in [
        'narrative_skillsAndExpertise','narrative_projectAndDonorExperience',
        'narrative_sectorAndGeographicFocus','narrative_languagesAndEducation',
        'Sectors','SubSectors','Countries','Donors','mostProminentSector','mainCountryOverall',
        'summary_bySector','summary_byCountry','summary_bySkills','summary_byDonors',
        'education_bachelorSummary','education_masterSummary','education_phdSummary',
        'cvId','personId','cvGroupId','cvRankKeep','hasDonorExperience','cvLanguage',
        'ai_parsingConfidence','ai_identityConfidence'
    ]:
        if pd.notna(row.get(col)): custom[col] = str(row[col])
    if custom: resume['customFields'] = custom
    return resume

def create_from_data(resume_json_str, collection_id, file_name):
    url = f"{BASE_URL}/v3/documents/create_from_data"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "data": resume_json_str,
        "collection": collection_id,
        "fileName": file_name
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code == 201:
        j = r.json()
        return True, j.get("identifier"), None
    else:
        return False, None, f"{r.status_code}: {r.text}"

def test_single():
    df = pd.read_excel(EXCEL_FILE_PATH)
    if df.empty:
        print("Error: Excel file is empty. Cannot run single test.")
        return
    row = df.iloc[0]
    resume = map_row(row)
    file_name = str(row.get('sourceFilename', 'test_cv_from_excel.json'))
    ok, doc_id, err = create_from_data(json.dumps(resume), COLLECTION_ID, file_name)
    if ok:
        print(f"Test OK, document id: {doc_id}")
    else:
        print(f"Test failed: {err}")

def upload_all(batch_sleep=0.1):
    df = pd.read_excel(EXCEL_FILE_PATH)
    if df.empty:
        print("Error: Excel file is empty. No documents to upload.")
        return
    ok_cnt = 0
    fail_cnt = 0
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Uploading"):
        if 'error' in row and pd.notna(row['error']) and str(row['error']).strip():
            continue
        resume = map_row(row)
        fname = str(row.get('sourceFilename', f"cv_{_+1}.json"))
        ok, doc_id, err = create_from_data(json.dumps(resume), COLLECTION_ID, fname)
        if ok:
            ok_cnt += 1
        else:
            fail_cnt += 1
            print(f"Failed {fname}: {err}")
        time.sleep(batch_sleep)
    print(f"Done. Success: {ok_cnt}, Failed: {fail_cnt}")

if __name__ == "__main__":
    print("Run test_single() first to test upload of the first row.")
    print("Then run upload_all() to process the entire file.")