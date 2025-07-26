import pandas as pd
from fuzzywuzzy import fuzz
from collections import defaultdict
import re
import logging
import random
import string

# === Setup Logging ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('school_matching.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# === Configuration ===
CONFIG = {
    'SCHOOL_FILE': "PRY_State_data.xlsx",
    'WARD_FILE': "Wards_OU_List_03.07.25.xlsx",
    'SCHOOL_SHEET': "Sheet1",
    'WARD_SHEET': "Wards_OU_List",
    'UID_COLUMN': "warduid",
    'OUTPUT_FILE': "PRY_orgunits_import_generated.csv",
    'UNMATCHED_FILE': "unmatched_schools.csv",
    'MAPPING_REPORTS': {
        'states': "mapped_states.xlsx",
        'lgas': "mapped_lgas.xlsx",
        'ward_matches': "ward_matching_report.xlsx"
    },
    'FUZZY_MATCH_THRESHOLD': 75,
    'LGA_CENTER_MATCH_THRESHOLD': 65,
    'MAX_SCHOOLS_PER_WARD': 50,
    'YEAR_RANGE': (1800, 2025),
    'SPECIAL_LGA_MAPPINGS': {
        'TAI': 'ri Tai Local Government Area'
    }
}

# === UID Generator ===
class UIDGenerator:
    @staticmethod
    def generate_uid():
        """Generate a DHIS2-compliant 11-character UID"""
        first_char = random.choice(string.ascii_uppercase)
        rest_chars = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        return first_char + rest_chars

    @staticmethod
    def generate_unique_uids(count):
        """Generate a set of unique UIDs"""
        uids = set()
        while len(uids) < count:
            uid = UIDGenerator.generate_uid()
            uids.add(uid)
        return list(uids)

# === Enhanced Text Processing ===
class TextProcessor:
    @staticmethod
    def clean_text(text):
        """Enhanced text normalization with special handling for TAI LGA"""
        if pd.isna(text):
            return ""
            
        text = str(text).upper().strip()
        text = re.sub(r'^(TA|RI|ETC)\s+', '', text)
        text = re.sub(r'[-/]', ' ', text)
        text = re.sub(r'[^\w\s&]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        text = text.replace('LGA', '').replace('WARD', '').strip()
        
        if 'TAI' in text:
            text = 'TAI'
        return text

    @staticmethod
    def to_proper_case(text):
        """Convert text to Proper Case (first letter of each word capitalized)"""
        if not text or pd.isna(text):
            return ""
        return ' '.join(word.capitalize() for word in str(text).split())

    @staticmethod
    def clean_school_name(name, prefix='', code=''):
        """Clean school name with prefix and code formatting"""
        if pd.isna(name):
            return ""
            
        name = re.sub(r'[.,]', '', str(name).strip())
        name = re.sub(r'\s+', ' ', name)
        name = TextProcessor.to_proper_case(name)
        
        prefix = str(prefix).strip() if pd.notna(prefix) else ''
        code = str(code).strip() if pd.notna(code) else ''
        
        if prefix and code:
            return f"{prefix} {name} ({code})"
        elif prefix:
            return f"{prefix} {name}"
        elif code:
            return f"{name} ({code})"
        return name

    @staticmethod
    def clean_dataframe(df, text_columns):
        """Apply cleaning to multiple columns with validation"""
        for col in text_columns:
            if col in df.columns:
                df[f"{col}_clean"] = df[col].apply(TextProcessor.clean_text)
        return df

# === Data Cleaners ===
class SchoolDataCleaner:
    @staticmethod
    def clean(schools):
        """Enhanced school data cleaning"""
        logger.info("Cleaning school data...")
        
        schools['school_level'] = (
            schools['school_level']
            .str.upper()
            .str.replace('PRRIMARY', 'PRIMARY', regex=False)
            .str.replace('ECCDE AND PRIMARY', 'PRIMARY', regex=False)
        )
        
        schools['year'] = pd.to_numeric(schools['year'], errors='coerce')
        
        location_map = {
            'RURAl': 'RURAL',
            'URBAN ': 'URBAN',
            '': 'UNKNOWN'
        }
        schools['location'] = (
            schools['location']
            .str.upper()
            .str.strip()
            .replace(location_map)
            .fillna('UNKNOWN')
        )
        
        schools['school_name'] = (
            schools['school_name']
            .str.strip()
            .str.replace(r'\s+', ' ', regex=True)
            .str.replace(r'[^\w\s&-]', '', regex=True)
        )
        return schools

class WardDataCleaner:
    @staticmethod
    def clean(wards):
        """Enhanced ward data cleaning"""
        logger.info("Cleaning ward data...")
        
        for col in ['state', 'lga', 'ward']:
            if col in wards.columns:
                wards[col] = (
                    wards[col]
                    .str.upper()
                    .str.strip()
                    .str.replace(f'{col.upper()}', '', regex=False)
                    .str.strip()
                )
        
        if 'lga' in wards.columns:
            wards['lga'] = wards['lga'].apply(
                lambda x: re.sub(r'^(LOCAL GOVERNMENT AREA|LGA)\s*', '', str(x)))
        return wards

# === Enhanced Matching Engine ===
class WardMatcher:
    def __init__(self, wards):
        self.ward_records = self._prepare_ward_records(wards)
        self.exact_match = self._create_exact_match_dict(wards)
        self.unknown_lookup = self._create_unknown_lookup_dict(wards)
        self.ward_counts = defaultdict(int)
        
    def _prepare_ward_records(self, wards):
        return [
            {
                'state': self._normalize_state_name(row["state_clean"]),
                'lga': self._normalize_lga_name(row["lga_clean"]),
                'ward': row["ward_clean"],
                'uid': row[CONFIG['UID_COLUMN']]
            }
            for _, row in wards.iterrows()
        ]
    
    def _normalize_state_name(self, name):
        if pd.isna(name):
            return ""
        name = str(name).upper()
        return re.sub(r'^(TA|RI|ETC)\s+', '', name).strip()
    
    def _normalize_lga_name(self, name):
        if pd.isna(name):
            return ""
        name = str(name).upper()
        name = re.sub(r'^(LOCAL GOVERNMENT AREA|LGA)\s*', '', name)
        name = re.sub(r'[-/]', ' ', name)
        if 'TAI' in name:
            return 'TAI'
        return re.sub(r'\s+', ' ', name).strip()
    
    def _create_exact_match_dict(self, wards):
        return {
            (self._normalize_state_name(row["state_clean"]), 
             self._normalize_lga_name(row["lga_clean"]), 
             row["ward_clean"]): row[CONFIG['UID_COLUMN']]
            for _, row in wards.iterrows()
        }
    
    def _create_unknown_lookup_dict(self, wards):
        return {
            (self._normalize_state_name(row["state_clean"]), 
             self._normalize_lga_name(row["lga_clean"])): row[CONFIG['UID_COLUMN']]
            for _, row in wards.iterrows()
            if "UNKNOWN" in row["ward_clean"]
        }
    
    def robust_fuzzy_match(self, state, lga, ward):
        best_match = None
        best_score = 0
        processed_lga = self._normalize_lga_name(lga)
        
        if processed_lga == "TAI":
            logger.debug(f"Processing TAI LGA match for school in {state}")
        
        if not ward or pd.isna(ward):
            return self._match_to_lga_center(state, processed_lga)
        
        for record in self.ward_records:
            if not (state in record['state'] or record['state'] in state):
                continue
                
            record_lga = self._normalize_lga_name(record['lga'])
            if processed_lga == "TAI":
                if "TAI" not in record_lga:
                    continue
            elif not (processed_lga in record_lga or record_lga in processed_lga):
                continue
                
            if not record['ward'] or pd.isna(record['ward']):
                continue
                
            ratio = fuzz.ratio(ward, record['ward'])
            partial_ratio = fuzz.partial_ratio(ward, record['ward'])
            token_ratio = fuzz.token_set_ratio(ward, record['ward'])
            
            weighted_score = (ratio * 0.4 + partial_ratio * 0.3 + token_ratio * 0.3)
            
            if (weighted_score > best_score and 
                weighted_score >= CONFIG['FUZZY_MATCH_THRESHOLD'] and
                self.ward_counts.get(record['uid'], 0) < CONFIG['MAX_SCHOOLS_PER_WARD']):
                
                best_score = weighted_score
                best_match = record['uid']
        
        return best_match, best_score
    
    def _match_to_lga_center(self, state, lga):
        center_names = [
            f"{lga} CENTRAL",
            f"{lga} TOWN",
            f"{lga} WARD 1",
            lga
        ]
        
        for name in center_names:
            for record in self.ward_records:
                if (state in record['state'] and 
                    lga in self._normalize_lga_name(record['lga']) and 
                    name in record['ward'] and
                    self.ward_counts.get(record['uid'], 0) < CONFIG['MAX_SCHOOLS_PER_WARD']):
                    
                    logger.debug(f"Matched to LGA center: {record['ward']}")
                    return record['uid'], 100
        return None, 0
    
    def match_ward(self, state, lga, ward):
        processed_state = self._normalize_state_name(state)
        processed_lga = self._normalize_lga_name(lga)
        
        if processed_lga == "TAI":
            logger.debug(f"Matching school in TAI LGA (original: {lga})")
        
        key = (processed_state, processed_lga, ward)
        parent_uid = self.exact_match.get(key)
        match_type = 'exact' if parent_uid else None
        
        if not parent_uid:
            parent_uid, score = self.robust_fuzzy_match(processed_state, processed_lga, ward)
            match_type = 'fuzzy' if parent_uid else None
        
        if not parent_uid and (not ward or pd.isna(ward)):
            parent_uid, score = self._match_to_lga_center(processed_state, processed_lga)
            match_type = 'lga_center' if parent_uid else None
        
        if not parent_uid:
            fallback_key = (processed_state, processed_lga)
            parent_uid = self.unknown_lookup.get(fallback_key)
            match_type = 'unknown_fallback' if parent_uid else 'unmatched'
        
        if parent_uid:
            self.ward_counts[parent_uid] += 1
            
        return parent_uid, match_type

# === Data Processor ===
class DataProcessor:
    def __init__(self):
        self.schools = None
        self.wards = None
        self.matcher = None
        self.orgunits = []
        self.unmatched = []
        self.ward_matches = []
        self.generated_uids = []
    
    def load_data(self):
        logger.info("Loading data files...")
        try:
            self.schools = pd.read_excel(CONFIG['SCHOOL_FILE'], sheet_name=CONFIG['SCHOOL_SHEET'])
            self.wards = pd.read_excel(CONFIG['WARD_FILE'], sheet_name=CONFIG['WARD_SHEET'])
            
            self._validate_columns()
            
            self.schools = SchoolDataCleaner.clean(self.schools)
            self.wards = WardDataCleaner.clean(self.wards)
            
            self.schools = TextProcessor.clean_dataframe(self.schools, ["state", "lga", "ward"])
            self.wards = TextProcessor.clean_dataframe(self.wards, ["state", "lga", "ward"])
            
            self.generated_uids = UIDGenerator.generate_unique_uids(len(self.schools))
            
            logger.info("Sample school data:\n" + str(self.schools[['state', 'state_clean', 'lga', 'lga_clean', 'ward', 'ward_clean']].head()))
            logger.info("Sample ward data:\n" + str(self.wards[['state', 'state_clean', 'lga', 'lga_clean', 'ward', 'ward_clean']].head()))
            
            return True
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return False
    
    def _validate_columns(self):
        required_school = ['state', 'lga', 'ward', 'school_name', 'prefix', 'school_code']
        required_ward = ['state', 'lga', 'ward', CONFIG['UID_COLUMN']]
        
        missing_school = [col for col in required_school if col not in self.schools.columns]
        missing_ward = [col for col in required_ward if col not in self.wards.columns]
        
        if missing_school or missing_ward:
            error_msg = []
            if missing_school:
                error_msg.append(f"Missing in schools: {missing_school}")
            if missing_ward:
                error_msg.append(f"Missing in wards: {missing_ward}")
            raise ValueError(" | ".join(error_msg))
    
    def generate_mappings(self):
        logger.info("Generating state and LGA mappings...")
        
        school_states = set(self.schools['state_clean'].dropna())
        ward_states = set(self.wards['state_clean'].dropna())
        
        school_lgas = set(self.schools['lga_clean'].dropna())
        ward_lgas = set(self.wards['lga_clean'].dropna())
        
        state_map = {}
        for s in school_states:
            for w in ward_states:
                if s in w or w in s:
                    state_map[s] = w
                    break
        
        lga_map = {}
        for special_lga, ward_pattern in CONFIG['SPECIAL_LGA_MAPPINGS'].items():
            if special_lga in school_lgas:
                for w in ward_lgas:
                    if ward_pattern.upper() in w:
                        lga_map[special_lga] = w
                        logger.info(f"Special mapping: {special_lga} → {w}")
                        break
        
        for s in school_lgas:
            if s not in lga_map:
                for w in ward_lgas:
                    if (s in w or w in s or 
                        fuzz.ratio(s, w) > 80 or
                        s.replace('-', ' ') == w.replace('-', ' ')):
                        lga_map[s] = w
                        break
        
        self.schools["state_clean"] = self.schools["state_clean"].map(lambda x: state_map.get(x, x))
        self.schools["lga_clean"] = self.schools["lga_clean"].map(lambda x: lga_map.get(x, x))
        
        if 'TAI' in school_lgas:
            logger.info(f"TAI LGA mapped to: {lga_map.get('TAI', 'UNMAPPED')}")
        
        return state_map, lga_map
    
    def process_matches(self):
        logger.info("Initializing ward matcher...")
        self.matcher = WardMatcher(self.wards)
        
        logger.info(f"Matching {len(self.schools)} schools to wards...")
        for idx, row in self.schools.iterrows():
            state = row["state_clean"]
            lga = row["lga_clean"]
            ward = row["ward_clean"] if pd.notna(row["ward_clean"]) else ""
            
            match_info = {
                'school_name': row['school_name'],
                'school_code': row.get('school_code', ''),
                'state': state,
                'lga': lga,
                'original_ward': ward,
                'match_type': None,
                'match_score': None,
                'matched_ward': None,
                'parent_uid': None
            }
            
            parent_uid, match_type = self.matcher.match_ward(state, lga, ward)
            match_info['match_type'] = match_type
            
            if match_type == 'fuzzy':
                matched_record = next(
                    (r for r in self.matcher.ward_records 
                     if r['uid'] == parent_uid), None
                )
                if matched_record:
                    match_info['matched_ward'] = matched_record['ward']
                    match_info['match_score'] = fuzz.ratio(ward, matched_record['ward'])
            
            if not parent_uid:
                self.unmatched.append(row)
                self.ward_matches.append(match_info)
                continue
            
            match_info['parent_uid'] = parent_uid
            
            # Format opening date with validation
            opening_date = "1900-01-01"  # Default value
            try:
                if pd.notnull(row['year']):
                    year = int(float(row['year']))  # Handle both string and numeric years
                    if year >= 1900:
                        opening_date = f"{year}-01-01"
            except (ValueError, TypeError):
                pass
            
            formatted_name = TextProcessor.clean_school_name(
                row["school_name"],
                row.get("prefix", ""),
                row.get("school_code", "")
            )
            
            short_name = formatted_name[:49]
            
            ou = {
                "OrgUnit": formatted_name,
                "UID": self.generated_uids[idx],
                "Code": row.get("school_code", ""),
                "Parent": parent_uid,
                "Short name": short_name,
                "Description": "",
                "Opening date": opening_date,
                "Closed date": "",
                "comment": "",
                "Feature type": "",
                "Coordinates": "",
                "URL": "",
                "Contact person": "",
                "Address": row.get("town", ""),
                "Email": "",
                "Phone number": ""
            }
            
            self.orgunits.append(ou)
            self.ward_matches.append(match_info)
        
        logger.info(f"Matched {len(self.orgunits)} schools successfully")
        if self.unmatched:
            logger.warning(f"{len(self.unmatched)} schools could not be matched")
        
        self._validate_matches()
    
    def _validate_matches(self):
        ward_counts = defaultdict(int)
        for ou in self.orgunits:
            ward_counts[ou['Parent']] += 1
        
        overloaded = {w: c for w, c in ward_counts.items() 
                     if c > CONFIG['MAX_SCHOOLS_PER_WARD']}
        
        if overloaded:
            logger.warning(f"\nWards exceeding school limit ({CONFIG['MAX_SCHOOLS_PER_WARD']}):")
            for ward, count in sorted(overloaded.items(), key=lambda x: -x[1]):
                logger.warning(f"- {ward}: {count} schools")
                sample = [ou['OrgUnit'] for ou in self.orgunits 
                         if ou['Parent'] == ward][:3]
                logger.warning(f"  Sample schools: {sample}")
    
    def generate_reports(self, state_map, lga_map):
        logger.info("Generating output files...")
        
        output_columns = [
            "OrgUnit", "UID", "Code", "Parent", "Short name", "Description", 
            "Opening date", "Closed date", "comment", "Feature type", 
            "Coordinates", "URL", "Contact person", "Address", "Email", "Phone number"
        ]
        
        pd.DataFrame(self.orgunits, columns=output_columns).to_csv(
            CONFIG['OUTPUT_FILE'], index=False
        )
        
        if self.unmatched:
            unmatched_df = pd.DataFrame(self.unmatched)
            if self.ward_matches:
                reasons = pd.DataFrame(self.ward_matches)[['school_name', 'match_type']]
                unmatched_df = unmatched_df.merge(reasons, on='school_name', how='left')
            unmatched_df.to_csv(CONFIG['UNMATCHED_FILE'], index=False)
        
        pd.DataFrame.from_dict(state_map, orient='index', 
                             columns=['Mapped_State']).to_excel(
                                 CONFIG['MAPPING_REPORTS']['states']
                             )
        
        pd.DataFrame.from_dict(lga_map, orient='index', 
                             columns=['Mapped_LGA']).to_excel(
                                 CONFIG['MAPPING_REPORTS']['lgas']
                             )
        
        match_report = pd.DataFrame(self.ward_matches)
        match_report['normalization_rules_applied'] = match_report.apply(
            lambda row: "Hyphen handling" if '-' in row['original_ward'] else 
                       "Slash handling" if '/' in row['original_ward'] else
                       "Standard normalization",
            axis=1
        )
        match_report.to_excel(
            CONFIG['MAPPING_REPORTS']['ward_matches'], index=False
        )
        
        self._log_quality_metrics()
    
    def _log_quality_metrics(self):
        match_rate = len(self.orgunits) / len(self.schools)
        logger.info(f"\nMatch Rate: {match_rate:.1%} ({len(self.orgunits)}/{len(self.schools)} schools matched)")
        
        if self.ward_matches:
            match_types = pd.DataFrame(self.ward_matches)['match_type'].value_counts()
            logger.info("\nMatching Types:")
            for match_type, count in match_types.items():
                logger.info(f"- {match_type}: {count}")
        
        ward_counts = defaultdict(int)
        for ou in self.orgunits:
            ward_counts[ou['Parent']] += 1
        
        outlier_wards = {
            ward: count for ward, count in ward_counts.items() 
            if count > 15
        }
        
        if outlier_wards:
            logger.warning("\nWards with high school counts (potential issues):")
            for ward, count in sorted(outlier_wards.items(), key=lambda x: -x[1]):
                logger.warning(f"- {ward}: {count} schools")

# === Main Execution ===
def main():
    logger.info("Starting school-ward matching process")
    
    processor = DataProcessor()
    
    if not processor.load_data():
        logger.error("Failed to load data. Exiting.")
        return
    
    state_map, lga_map = processor.generate_mappings()
    processor.process_matches()
    processor.generate_reports(state_map, lga_map)
    
    logger.info("Processing complete!")
    logger.info(f"Output files generated: {CONFIG['OUTPUT_FILE']}")
    if processor.unmatched:
        logger.info(f"Unmatched schools saved to: {CONFIG['UNMATCHED_FILE']}")
    logger.info(f"Mapping reports: {', '.join(CONFIG['MAPPING_REPORTS'].values())}")

if __name__ == "__main__":
    main()