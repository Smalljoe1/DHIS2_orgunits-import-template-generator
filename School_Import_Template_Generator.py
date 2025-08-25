import streamlit as st
import pandas as pd
from fuzzywuzzy import fuzz
from collections import defaultdict
import re
import logging
import random
import string
import io
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('school_matching.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# App Configuration
class Config:
    UID_COLUMN = "warduid"
    FUZZY_MATCH_THRESHOLD = 75
    LGA_CENTER_MATCH_THRESHOLD = 65
    MAX_SCHOOLS_PER_WARD = 50
    SPECIAL_LGA_MAPPINGS = {'TAI': 'ri Tai Local Government Area'}
    
    @staticmethod
    def get_output_filenames(state):
        base = f"PRY_{state}_"
        return {
            'output_file': f"{base}import_template.csv",
            'unmatched_file': f"{base}unmatched_schools.csv",
            'states_report': f"{base}mapped_states.xlsx",
            'lgas_report': f"{base}mapped_lgas.xlsx",
            'ward_report': f"{base}ward_matching_report.xlsx"
        }

# UID Generator
class UIDGenerator:
    @staticmethod
    def generate_uid():
        first_char = random.choice(string.ascii_uppercase)
        rest_chars = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        return first_char + rest_chars

    @staticmethod
    def generate_unique_uids(count):
        uids = set()
        while len(uids) < count:
            uid = UIDGenerator.generate_uid()
            uids.add(uid)
        return list(uids)

# Text Processing
class TextProcessor:
    @staticmethod
    def clean_text(text):
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
        if not text or pd.isna(text):
            return ""
        return ' '.join(word.capitalize() for word in str(text).split())

    @staticmethod
    def clean_school_name(name, prefix='', code=''):
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

# Data Cleaners
class DataCleaner:
    @staticmethod
    def clean_schools(schools):
        # Ensure string columns are properly converted
        for col in ['school_level', 'location', 'school_name']:
            if col in schools.columns:
                schools[col] = schools[col].astype(str).replace('nan', '').replace('None', '')
        
        schools['school_level'] = (
            schools['school_level']
            .str.upper()
            .str.replace('PRRIMARY', 'PRIMARY', regex=False)
            .str.replace('ECCDE AND PRIMARY', 'PRIMARY', regex=False)
        )
        
        schools['year'] = pd.to_numeric(schools['year'], errors='coerce')
        
        location_map = {'RURAl': 'RURAL', 'URBAN ': 'URBAN', '': 'UNKNOWN'}
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

    @staticmethod
    def clean_wards(wards):
        # Ensure string columns are properly converted
        for col in ['state', 'lga', 'ward']:
            if col in wards.columns:
                wards[col] = wards[col].astype(str).replace('nan', '').replace('None', '')
        
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

# Matching Engine
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
                'uid': row[Config.UID_COLUMN]
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
    
    def match_ward(self, state, lga, ward):
        processed_state = self._normalize_state_name(state)
        processed_lga = self._normalize_lga_name(lga)
        
        # Try exact match first
        key = (processed_state, processed_lga, ward)
        parent_uid = self.exact_match.get(key)
        match_type = 'exact' if parent_uid else None
        
        # Try fuzzy match if exact fails
        if not parent_uid:
            parent_uid, score = self._fuzzy_match(processed_state, processed_lga, ward)
            match_type = 'fuzzy' if parent_uid else None
        
        # Try LGA center match if no ward specified
        if not parent_uid and (not ward or pd.isna(ward)):
            parent_uid, score = self._match_to_lga_center(processed_state, processed_lga)
            match_type = 'lga_center' if parent_uid else None
        
        # Fallback to UNKNOWN ward
        if not parent_uid:
            fallback_key = (processed_state, processed_lga)
            parent_uid = self.unknown_lookup.get(fallback_key)
            match_type = 'unknown_fallback' if parent_uid else 'unmatched'
        
        if parent_uid:
            self.ward_counts[parent_uid] += 1
            
        return parent_uid, match_type

    def _fuzzy_match(self, state, lga, ward):
        best_match = None
        best_score = 0
        processed_lga = self._normalize_lga_name(lga)
        
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
                weighted_score >= Config.FUZZY_MATCH_THRESHOLD and
                self.ward_counts.get(record['uid'], 0) < Config.MAX_SCHOOLS_PER_WARD):
                best_score = weighted_score
                best_match = record['uid']
        
        return best_match, best_score
    
    def _match_to_lga_center(self, state, lga):
        center_names = [f"{lga} CENTRAL", f"{lga} TOWN", f"{lga} WARD 1", lga]
        for name in center_names:
            for record in self.ward_records:
                if (state in record['state'] and 
                    lga in self._normalize_lga_name(record['lga']) and 
                    name in record['ward'] and
                    self.ward_counts.get(record['uid'], 0) < Config.MAX_SCHOOLS_PER_WARD):
                    return record['uid'], 100
        return None, 0
    
    def _create_exact_match_dict(self, wards):
        return {
            (self._normalize_state_name(row["state_clean"]), 
             self._normalize_lga_name(row["lga_clean"]), 
             row["ward_clean"]): row[Config.UID_COLUMN]
            for _, row in wards.iterrows()
        }
    
    def _create_unknown_lookup_dict(self, wards):
        return {
            (self._normalize_state_name(row["state_clean"]), 
             self._normalize_lga_name(row["lga_clean"])): row[Config.UID_COLUMN]
            for _, row in wards.iterrows()
            if "UNKNOWN" in row["ward_clean"]
        }

# Main Processor
class SchoolWardMatcher:
    def __init__(self):
        self.schools = None
        self.wards = None
        self.state = None
        self.orgunits = []
        self.unmatched = []
        self.ward_matches = []
        self.generated_uids = []
        self.log_messages = []
    
    def log(self, message, level="info"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.log_messages.append(log_entry)
        if level == "info":
            logger.info(message)
            st.session_state.logs.append(f"INFO: {log_entry}")
        elif level == "warning":
            logger.warning(message)
            st.session_state.logs.append(f"WARNING: {log_entry}")
        elif level == "error":
            logger.error(message)
            st.session_state.logs.append(f"ERROR: {log_entry}")
    
    def load_data(self, school_file, ward_file, state):
        try:
            self.state = state.upper()
            self.schools = pd.read_excel(school_file)
            self.wards = pd.read_excel(ward_file)
            
            self._validate_columns()
            
            self.schools = DataCleaner.clean_schools(self.schools)
            self.wards = DataCleaner.clean_wards(self.wards)
            
            text_columns = ["state", "lga", "ward"]
            self.schools = self._clean_dataframe(self.schools, text_columns)
            self.wards = self._clean_dataframe(self.wards, text_columns)
            
            self.generated_uids = UIDGenerator.generate_unique_uids(len(self.schools))
            
            self.log("Data loaded successfully")
            return True
        except Exception as e:
            self.log(f"Error loading data: {str(e)}", "error")
            return False
    
    def _clean_dataframe(self, df, text_columns):
        for col in text_columns:
            if col in df.columns:
                # Convert to string first, then apply cleaning
                df[f"{col}_clean"] = df[col].astype(str).apply(TextProcessor.clean_text)
        return df
    
    def _validate_columns(self):
        required_school = ['state', 'lga', 'ward', 'school_name', 'prefix', 'school_code']
        required_ward = ['state', 'lga', 'ward', Config.UID_COLUMN]
        
        missing_school = [col for col in required_school if col not in self.schools.columns]
        missing_ward = [col for col in required_ward if col not in self.wards.columns]
        
        if missing_school or missing_ward:
            error_msg = []
            if missing_school:
                error_msg.append(f"Missing in schools: {missing_school}")
            if missing_ward:
                error_msg.append(f"Missing in wards: {missing_ward}")
            raise ValueError(" | ".join(error_msg))
    
    def process(self):
        try:
            self.log("Starting matching process...")
            state_map, lga_map = self._generate_mappings()
            
            matcher = WardMatcher(self.wards)
            self.log(f"Matching {len(self.schools)} schools to wards...")
            
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
                
                parent_uid, match_type = matcher.match_ward(state, lga, ward)
                match_info['match_type'] = match_type
                
                if match_type == 'fuzzy':
                    matched_record = next(
                        (r for r in matcher.ward_records if r['uid'] == parent_uid), None)
                    if matched_record:
                        match_info['matched_ward'] = matched_record['ward']
                        match_info['match_score'] = fuzz.ratio(ward, matched_record['ward'])
                
                if not parent_uid:
                    self.unmatched.append(row)
                    self.ward_matches.append(match_info)
                    continue
                
                match_info['parent_uid'] = parent_uid
                
                opening_date = "1900-01-01"
                try:
                    if pd.notnull(row['year']):
                        year = int(float(row['year']))
                        if year >= 1900:
                            opening_date = f"{year}-01-01"
                except (ValueError, TypeError):
                    pass
                
                formatted_name = TextProcessor.clean_school_name(
                    row["school_name"],
                    row.get("prefix", ""),
                    row.get("school_code", "")
                )
                
                ou = {
                    "OrgUnit": formatted_name,
                    "UID": self.generated_uids[idx],
                    "Code": row.get("school_code", ""),
                    "Parent": parent_uid,
                    "Short name": formatted_name[:49],
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
            
            self.log(f"Successfully matched {len(self.orgunits)} schools")
            if self.unmatched:
                self.log(f"{len(self.unmatched)} schools could not be matched", "warning")
            
            self._validate_matches()
            return True
        except Exception as e:
            self.log(f"Error during processing: {str(e)}", "error")
            return False
    
    def _generate_mappings(self):
        self.log("Generating state and LGA mappings...")
        
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
        for special_lga, ward_pattern in Config.SPECIAL_LGA_MAPPINGS.items():
            if special_lga in school_lgas:
                for w in ward_lgas:
                    if ward_pattern.upper() in w:
                        lga_map[special_lga] = w
                        self.log(f"Special mapping: {special_lga} → {w}")
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
            self.log(f"TAI LGA mapped to: {lga_map.get('TAI', 'UNMAPPED')}")
        
        return state_map, lga_map
    
    def _validate_matches(self):
        ward_counts = defaultdict(int)
        for ou in self.orgunits:
            ward_counts[ou['Parent']] += 1
        
        overloaded = {w: c for w, c in ward_counts.items() 
                     if c > Config.MAX_SCHOOLS_PER_WARD}
        
        if overloaded:
            self.log("\nWards exceeding school limit:", "warning")
            for ward, count in sorted(overloaded.items(), key=lambda x: -x[1]):
                self.log(f"- {ward}: {count} schools", "warning")
    
    def get_output_files(self):
        filenames = Config.get_output_filenames(self.state)
        
        # Main output
        output_columns = [
            "OrgUnit", "UID", "Code", "Parent", "Short name", "Description", 
            "Opening date", "Closed date", "comment", "Feature type", 
            "Coordinates", "URL", "Contact person", "Address", "Email", "Phone number"
        ]
        output_df = pd.DataFrame(self.orgunits, columns=output_columns)
        output_csv = output_df.to_csv(index=False).encode('utf-8')
        
        # Unmatched schools
        unmatched_csv = None
        if self.unmatched:
            unmatched_df = pd.DataFrame(self.unmatched)
            if self.ward_matches:
                reasons = pd.DataFrame(self.ward_matches)[['school_name', 'match_type']]
                unmatched_df = unmatched_df.merge(reasons, on='school_name', how='left')
            unmatched_csv = unmatched_df.to_csv(index=False).encode('utf-8')
        
        return {
            'import_template': (output_csv, filenames['output_file']),
            'unmatched_schools': (unmatched_csv, filenames['unmatched_file']) if unmatched_csv else None
        }

# Streamlit App
def main():
    st.set_page_config(page_title="School-Import Template Generating Tool", layout="wide")
    st.title("📊 School-Import Template Generating Tool")
    
    # Initialize session state
    if 'logs' not in st.session_state:
        st.session_state.logs = []
    if 'processor' not in st.session_state:
        st.session_state.processor = None
    if 'output_files' not in st.session_state:
        st.session_state.output_files = None
    if 'show_guide' not in st.session_state:
        st.session_state.show_guide = True
    
    # User Guide Expansion
    with st.expander("📘 User Guide - Click to Show/Hide", expanded=st.session_state.show_guide):
        st.markdown("""
        ### How to Use This Tool
        
        1. **Upload Files** (in the sidebar):
           - **School Data**: Excel file from your state containing school information
           - **Ward Data**: Excel file from DHIS2 with ward organizational units
        
        2. **Enter State Name**: Used for naming output files (e.g., "Lagos")
        
        3. **Click 'Process Data'**: The system will:
           - Validate your files
           - Match schools to wards
           - Generate DHIS2-compatible import template.
        
        4. **Download Results**:
           - **Import Template**: For uploading to DHIS2
           - **Unmatched Schools**: For reviewing unmatched records
        
        ### Required File Formats
        - **School Data** must contain these columns:
          ```state, lga, ward, school_name, prefix, school_code```
        - **Ward Data** must contain:
          ```state, lga, ward, warduid```
        
        ### Tips
        - Check the Activity Log for processing details
        - Review unmatched schools to improve data quality
        - For large datasets, processing may take a few minutes
        """)
        
        if st.button("× Close Guide", key="close_guide"):
            st.session_state.show_guide = False
    
    # Sidebar for file uploads
    with st.sidebar:
        st.header("📂 Upload Files")
        school_file = st.file_uploader("School List From State", type=["xlsx"], 
                                     help="Excel file containing school information from your state")
        ward_file = st.file_uploader("Ward List from DHIS2", type=["xlsx"],
                                   help="Excel file with ward organizational units from DHIS2")
        state = st.text_input("State Name (for output filenames)", "", 
                             help="Will be used to name output files (e.g., 'Lagos')").strip()
        process_btn = st.button("⚡ Process Data", type="primary")
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("🔧 Data Processing")
        
        if process_btn:
            if not school_file or not ward_file:
                st.error("Please upload both School and Ward files to proceed.")
            elif not state:
                st.error("Please enter a state name for output files.")
            else:
                # Initialize progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                processor = SchoolWardMatcher()
                st.session_state.processor = processor
                
                # Load data with progress updates
                status_text.info("Loading and validating data...")
                if processor.load_data(school_file, ward_file, state):
                    progress_bar.progress(25)
                    
                    # Process data with progress updates
                    status_text.info("Matching schools to wards...")
                    if processor.process():
                        progress_bar.progress(75)
                        
                        # Generate outputs
                        status_text.info("Preparing download files...")
                        st.session_state.output_files = processor.get_output_files()
                        progress_bar.progress(100)
                        status_text.success("✅ Processing completed successfully!")
                        
                        # Show metrics dashboard
                        st.subheader("📈 Results Summary")
                        col1, col2, col3 = st.columns(3)
                        
                        total_schools = len(processor.schools)
                        matched = len(processor.orgunits)
                        unmatched = len(processor.unmatched)
                        
                        col1.metric("Total Schools Processed", total_schools)
                        col2.metric("Successfully Matched", f"{matched} ({matched/total_schools*100:.1f}%)")
                        col3.metric("Unmatched Schools", f"{unmatched} ({unmatched/total_schools*100:.1f}%)", 
                                   delta_color="inverse" if unmatched > 0 else "off")
                        
                        # Ward distribution visualization
                        if processor.orgunits:
                            ward_counts = defaultdict(int)
                            for ou in processor.orgunits:
                                ward_counts[ou['Parent']] += 1
                            
                            ward_df = pd.DataFrame.from_dict(ward_counts, orient='index', columns=['Schools'])
                            st.write("**Schools per Ward Distribution**")
                            st.bar_chart(ward_df['Schools'].value_counts().head(15))
                    else:
                        status_text.error("❌ Processing failed. Check logs for details.")
                else:
                    status_text.error("❌ Data loading failed. Check logs for details.")
                
                progress_bar.empty()
        
        if st.session_state.output_files:
            st.subheader("📥 Download Results")
            
            # Download buttons with icons
            output_csv, output_filename = st.session_state.output_files['import_template']
            st.download_button(
                label="⬇️ Download Import Template",
                data=output_csv,
                file_name=output_filename,
                mime="text/csv",
                help="DHIS2-compatible import file with all matched schools"
            )
            
            if st.session_state.output_files['unmatched_schools']:
                unmatched_csv, unmatched_filename = st.session_state.output_files['unmatched_schools']
                st.download_button(
                    label="⬇️ Download Unmatched Schools",
                    data=unmatched_csv,
                    file_name=unmatched_filename,
                    mime="text/csv",
                    help="Review these schools to improve matching"
                )
    
    with col2:
        st.header("📝 Activity Log")
        
        # Log display with enhanced filtering
        log_filter = st.selectbox("Filter logs:", ["All", "Info", "Warnings", "Errors"])
        log_container = st.container(height=400)
        
        if st.session_state.logs:
            with log_container:
                for log in st.session_state.logs[-50:]:  # Show last 50 log entries
                    if log_filter == "All" or \
                       (log_filter == "Info" and log.startswith("INFO:")) or \
                       (log_filter == "Warnings" and log.startswith("WARNING:")) or \
                       (log_filter == "Errors" and log.startswith("ERROR:")):
                        
                        if log.startswith("ERROR:"):
                            st.error(log)
                        elif log.startswith("WARNING:"):
                            st.warning(log)
                        else:
                            st.text(log)
        
        if st.button("🗑️ Clear Logs"):
            st.session_state.logs = []
            st.rerun()

if __name__ == "__main__":
    main()