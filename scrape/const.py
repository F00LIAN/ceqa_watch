table_name = 'ceqa_data'
BASE_URL = "https://ceqanet.opr.ca.gov/Search?LeadAgency="

# Query to create table
# Query to create table
INSERT_QUERY = f"""
INSERT INTO public.{table_name} (
    entry_id, 
    sch_number, 
    lead_agency_title, 
    document_title, 
    document_type, 
    received, 
    posted, 
    document_description, 
    cities, 
    counties, 
    location_cross_streets, 
    location_total_acres, 
    noc_project_issues, 
    noc_public_review_start_date, 
    noc_public_review_end_date, 
    noe_exempt_status, 
    noe_exempt_citation, 
    noe_reasons_for_exemption, 
    nod_agency, 
    nod_approved_by_lead_agency, 
    nod_approved_date, 
    nod_significant_environmental_impact, 
    nod_environmental_impact_report_prepared, 
    nod_negative_declaration_prepared, 
    nod_other_document_type, 
    nod_mitigation_measures, 
    nod_mitigation_reporting_or_monitoring_plan, 
    nod_statement_of_overriding_considerations_adopted, 
    nod_findings_made_pursuant, 
    nod_final_eir_available_location, 
    date_gathered, 
    location_parcel_number,
    document_type_details
) VALUES %s
ON CONFLICT (entry_id) 
DO UPDATE SET 
    sch_number = EXCLUDED.sch_number,
    lead_agency_title = EXCLUDED.lead_agency_title,
    document_title = EXCLUDED.document_title,
    document_type = EXCLUDED.document_type,
    received = EXCLUDED.received,
    posted = EXCLUDED.posted,
    document_description = EXCLUDED.document_description,
    cities = EXCLUDED.cities,
    counties = EXCLUDED.counties,
    location_cross_streets = EXCLUDED.location_cross_streets,
    location_total_acres = EXCLUDED.location_total_acres,
    noc_project_issues = EXCLUDED.noc_project_issues,
    noc_public_review_start_date = EXCLUDED.noc_public_review_start_date,
    noc_public_review_end_date = EXCLUDED.noc_public_review_end_date,
    noe_exempt_status = EXCLUDED.noe_exempt_status,
    noe_exempt_citation = EXCLUDED.noe_exempt_citation,
    noe_reasons_for_exemption = EXCLUDED.noe_reasons_for_exemption,
    nod_agency = EXCLUDED.nod_agency,
    nod_approved_by_lead_agency = EXCLUDED.nod_approved_by_lead_agency,
    nod_approved_date = EXCLUDED.nod_approved_date,
    nod_significant_environmental_impact = EXCLUDED.nod_significant_environmental_impact,
    nod_environmental_impact_report_prepared = EXCLUDED.nod_environmental_impact_report_prepared,
    nod_negative_declaration_prepared = EXCLUDED.nod_negative_declaration_prepared,
    nod_other_document_type = EXCLUDED.nod_other_document_type,
    nod_mitigation_measures = EXCLUDED.nod_mitigation_measures,
    nod_mitigation_reporting_or_monitoring_plan = EXCLUDED.nod_mitigation_reporting_or_monitoring_plan,
    nod_statement_of_overriding_considerations_adopted = EXCLUDED.nod_statement_of_overriding_considerations_adopted,
    nod_findings_made_pursuant = EXCLUDED.nod_findings_made_pursuant,
    nod_final_eir_available_location = EXCLUDED.nod_final_eir_available_location,
    date_gathered = EXCLUDED.date_gathered,
    location_parcel_number = EXCLUDED.location_parcel_number,
    document_type_details = EXCLUDED.document_type_details;
"""


# List of columns to keep
KEEPS = [
        'SCH Number', 'Lead Agency Title', 'Document Title', 'Document Type', 'Received', 
        'Posted', 'Document Description', 'Cities', 'Counties', 'Location Cross Streets', 
        'Location Parcel Number', 'Location Total Acres', 'NOC Project Issues', 
        'NOC Public Review Start Date', 'NOC Public Review End Date', 'NOE Exempt Status',
        'NOE Exempt Citation', 'NOE Reasons for Exemption', 'NOD Agency', 
        'NOD Approved By Lead Agency', 'NOD Approved Date', 'NOD Significant Environmental Impact', 
        'NOD Environmental Impact Report Prepared', 'NOD Negative Declaration Prepared', 
        'NOD Other Document Type', 'NOD Mitigation Measures', 
        'NOD Mitigation Reporting Or Monitoring Plan', 
        'NOD Statement Of Overriding Considerations Adopted', 
        'NOD Findings Made Pursuant', 'NOD Final EIR Available Location'
    ]

cities = ["Lancaster, City of", "Los Angeles, City of", "San Diego, City of"]

DOCUMENT_TYPE = {
    "NOE": "Notice of Exemption",
    "NOD": "Notice of Determination",
    "MND": "Mitigated Negative Declaration",
    "NDE": "Notice of Determination",
    "ADM": "Addendum",
    "BIA": "Tribal Notice of Decision",
    "BLA": "Bureau of Indian Affairs Notice of Land Acquisition",
    "CON": "Early Consultation",
    "EA": "Environmental Assessment",
    "EIR": "Draft Environmental Impact Report",
    "EIS": "Draft EIS",
    "FED": "Fuctional Equivalent Document",
    "FIN": "Final Document",
    "FIS": "Final Environmental Statement",
    "FON": "Finding of No Significant Impact",
    "FOT": "Federal Other Document",
    "FYI": "Informational Only",
    "JD": "Joint Document",
    "MEA": "Master Environmental Assessment",
    "NEG": "Negative Declaration",
    "NOC": "Other Notice of Completion",
    "NOI": "Notice of Intent",
    "NOP": "Notice of Preparation of a Draft EIR",
    "OTH": "Other Document",
    "RAN": "Request for Advanced Notification",
    "RC": "Response to Comments",
    "REV": "Revised Document",
    "RIR": "Revised Environmental Impact Rep",
    "ROD": "Record of Decision",
    "SBE": "Subsequent EIR",
    "SCA": "Sustainable Communities Environmental Assessment",
    "SEA": "SEA Supplemental EIR",
    "SIR": "Supplemental EIR",
    "SIS": "Revised/Supplemental EIS",
    "TRI": "Tribal Environmental Evaluation"
}


