import streamlit as st
import pandas as pd
import re
from datetime import datetime, timedelta
from typing import List, Dict, Set
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session

# Set page config
st.set_page_config(
    page_title="Expert Finder",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .expert-card {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
        margin: 1rem 0;
    }
    .skill-tag {
        background-color: #e3f2fd;
        color: #1565c0;
        padding: 0.25rem 0.5rem;
        border-radius: 15px;
        font-size: 0.8rem;
        margin: 0.2rem;
        display: inline-block;
    }
    .high-skill-tag {
        background-color: #e8f5e8;
        color: #2e7d32;
        padding: 0.25rem 0.5rem;
        border-radius: 15px;
        font-size: 0.8rem;
        margin: 0.2rem;
        display: inline-block;
    }
    .cert-tag {
        background-color: #fff3e0;
        color: #ef6c00;
        padding: 0.25rem 0.5rem;
        border-radius: 15px;
        font-size: 0.8rem;
        margin: 0.2rem;
        display: inline-block;
    }
    .metric-card {
        background-color: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

class ExpertFinderSiS:
    def __init__(self):
        # Get Snowflake session (automatically available in Streamlit in Snowflake)
        self.session = get_active_session()
        
    @st.cache_data(ttl=300)
    def search_freestyle_experts(_self, search_terms: List[str]) -> pd.DataFrame:
        """Search for experts in Freestyle data based on skills and specialties"""
        if not search_terms:
            return pd.DataFrame()
        
        # Create a simpler, more reliable search approach using FLATTEN
        search_conditions = []
        
        for term in search_terms:
            term_upper = term.upper().replace("'", "''")  # Escape single quotes
            
            # Use FLATTEN-based search for each skill array type
            skill_conditions = []
            
            # Self-assessment skill arrays
            for skill_level in ['NULL', '0', '100', '200', '300', '400']:
                skill_conditions.append(
                    f"EXISTS(SELECT 1 FROM TABLE(FLATTEN(SELF_ASSESMENT_SKILL_{skill_level})) f "
                    f"WHERE UPPER(f.VALUE::STRING) LIKE '%{term_upper}%')"
                )
            
            # Manager score skill arrays  
            for skill_level in ['NULL', '0', '100', '200', '300', '400']:
                skill_conditions.append(
                    f"EXISTS(SELECT 1 FROM TABLE(FLATTEN(MGR_SCORE_SKILL_{skill_level})) f "
                    f"WHERE UPPER(f.VALUE::STRING) LIKE '%{term_upper}%')"
                )
            
            # Specialties, certifications
            skill_conditions.extend([
                f"EXISTS(SELECT 1 FROM TABLE(FLATTEN(SPECIALTIES)) f WHERE UPPER(f.VALUE::STRING) LIKE '%{term_upper}%')",
                f"EXISTS(SELECT 1 FROM TABLE(FLATTEN(CERT_INTERNAL)) f WHERE UPPER(f.VALUE::STRING) LIKE '%{term_upper}%')",
                f"EXISTS(SELECT 1 FROM TABLE(FLATTEN(CERT_EXTERNAL)) f WHERE UPPER(f.VALUE::STRING) LIKE '%{term_upper}%')"
            ])
            
            search_conditions.append(f"({' OR '.join(skill_conditions)})")
        
        where_clause = " OR ".join(search_conditions)
        
        query = f"""
        SELECT 
            EMPLOYEE_ID,
            USER_ID,
            NAME,
            EMAIL,
            SELF_ASSESMENT_SKILL_NULL,
            SELF_ASSESMENT_SKILL_0,
            SELF_ASSESMENT_SKILL_100,
            SELF_ASSESMENT_SKILL_200,
            SELF_ASSESMENT_SKILL_300,
            SELF_ASSESMENT_SKILL_400,
            MGR_SCORE_SKILL_NULL,
            MGR_SCORE_SKILL_0,
            MGR_SCORE_SKILL_100,
            MGR_SCORE_SKILL_200,
            MGR_SCORE_SKILL_300,
            MGR_SCORE_SKILL_400,
            CERT_INTERNAL,
            CERT_EXTERNAL,
            SPECIALTIES,
            EMPLOYERS
        FROM SALES.SE_REPORTING.FREESTYLE_SUMMARY 
        WHERE {where_clause}
        ORDER BY NAME
        """
        
        try:
            result = _self.session.sql(query).to_pandas()
            return result
        except Exception as e:
            # Silently fallback to simpler search
            return _self._search_freestyle_simple(search_terms)
    
    @st.cache_data(ttl=300)
    def _search_freestyle_simple(_self, search_terms: List[str]) -> pd.DataFrame:
        """Simplified search for Freestyle data when complex search fails"""
        if not search_terms:
            return pd.DataFrame()
        
        # Very simple search approach - just look for terms in array representations
        search_conditions = []
        
        for term in search_terms:
            term_upper = term.upper().replace("'", "''")
            # Simple string search across skill arrays converted to text
            search_conditions.append(f"""
                (UPPER(ARRAY_TO_STRING(SELF_ASSESMENT_SKILL_300, ',')) LIKE '%{term_upper}%' OR
                 UPPER(ARRAY_TO_STRING(SELF_ASSESMENT_SKILL_400, ',')) LIKE '%{term_upper}%' OR
                 UPPER(ARRAY_TO_STRING(MGR_SCORE_SKILL_300, ',')) LIKE '%{term_upper}%' OR
                 UPPER(ARRAY_TO_STRING(MGR_SCORE_SKILL_400, ',')) LIKE '%{term_upper}%' OR
                 UPPER(ARRAY_TO_STRING(SPECIALTIES, ',')) LIKE '%{term_upper}%' OR
                 UPPER(ARRAY_TO_STRING(CERT_EXTERNAL, ',')) LIKE '%{term_upper}%')
            """)
        
        where_clause = " OR ".join(search_conditions)
        
        query = f"""
        SELECT 
            EMPLOYEE_ID,
            USER_ID,
            NAME,
            EMAIL,
            SELF_ASSESMENT_SKILL_NULL,
            SELF_ASSESMENT_SKILL_0,
            SELF_ASSESMENT_SKILL_100,
            SELF_ASSESMENT_SKILL_200,
            SELF_ASSESMENT_SKILL_300,
            SELF_ASSESMENT_SKILL_400,
            MGR_SCORE_SKILL_NULL,
            MGR_SCORE_SKILL_0,
            MGR_SCORE_SKILL_100,
            MGR_SCORE_SKILL_200,
            MGR_SCORE_SKILL_300,
            MGR_SCORE_SKILL_400,
            CERT_INTERNAL,
            CERT_EXTERNAL,
            SPECIALTIES,
            EMPLOYERS
        FROM SALES.SE_REPORTING.FREESTYLE_SUMMARY 
        WHERE {where_clause}
        ORDER BY NAME
        LIMIT 1000
        """
        
        try:
            result = _self.session.sql(query).to_pandas()
            st.info(f"Using simplified search - found {len(result)} results")
            return result
        except Exception as e:
            st.error(f"Both search methods failed: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def search_salesforce_experts(_self, search_terms: List[str]) -> pd.DataFrame:
        """Search for experts based on Salesforce opportunity involvement with specific competitors"""
        if not search_terms:
            return pd.DataFrame()
        
        # Create search conditions for primary competitor
        search_conditions = []
        for term in search_terms:
            search_conditions.append(f"UPPER(O.PRIMARY_COMPETITOR_C) LIKE UPPER('%{term}%')")
        
        where_clause = " OR ".join(search_conditions)
        
        query = f"""
        SELECT 
            o.ID,
            o.NAME as OPPORTUNITY_NAME,
            O.PRIMARY_COMPETITOR_C,
            o.CLOSE_DATE,
            o.STAGE_NAME,
            o.AMOUNT,
            o.LEAD_SALES_ENGINEER_C,
            o.OWNER_ID,
            o.ACCOUNT_ID,
            a.INDUSTRY as ACCOUNT_INDUSTRY,
            COUNT(*) OVER (PARTITION BY o.LEAD_SALES_ENGINEER_C) as se_opportunity_count,
            COUNT(*) OVER (PARTITION BY o.OWNER_ID) as owner_opportunity_count
        FROM FIVETRAN.SALESFORCE.OPPORTUNITY o
        LEFT JOIN FIVETRAN.SALESFORCE.ACCOUNT a ON o.ACCOUNT_ID = a.ID
        WHERE {where_clause}
            AND o.CLOSE_DATE >= DATEADD(year, -3, CURRENT_DATE())
            AND (o.LEAD_SALES_ENGINEER_C IS NOT NULL OR o.OWNER_ID IS NOT NULL)
        ORDER BY o.CLOSE_DATE DESC
        """
        
        try:
            result = _self.session.sql(query).to_pandas()
            return result
        except Exception as e:
            st.error(f"Error querying Salesforce data: {str(e)}")
            return pd.DataFrame()
    
    def extract_relevant_skills(self, row: pd.Series, search_terms: List[str]) -> Dict[str, List[str]]:
        """Extract skills that match search terms, organized by proficiency level"""
        skills_found = {
            'high_proficiency': [],  # 300-400 level or manager endorsed
            'medium_proficiency': [], # 100-200 level
            'basic_proficiency': [],  # 0 level or null
            'certifications': [],
            'specialties': []
        }
        
        search_terms_upper = [term.upper() for term in search_terms]
        
        # Check high proficiency skills (300-400 level and manager scores)
        for col in ['SELF_ASSESMENT_SKILL_300', 'SELF_ASSESMENT_SKILL_400', 
                   'MGR_SCORE_SKILL_200', 'MGR_SCORE_SKILL_300', 'MGR_SCORE_SKILL_400']:
            if pd.notna(row[col]) and row[col]:
                # Parse array format like other fields
                skill_data = row[col]
                if isinstance(skill_data, str):
                    skill_str = str(skill_data)
                    if skill_str.startswith('[') and skill_str.endswith(']'):
                        clean_skills = skill_str.strip('[]')
                        if '"' in clean_skills:
                            skill_matches = re.findall(r'"([^"]*)"', clean_skills)
                        elif "'" in clean_skills:
                            skill_matches = re.findall(r"'([^']*)'", clean_skills)
                        else:
                            skill_matches = [clean_skills.strip()] if clean_skills.strip() else []
                    else:
                        skill_matches = [skill_str.strip()] if skill_str.strip() else []
                else:
                    skill_matches = skill_data if isinstance(skill_data, list) else []
                
                for skill in skill_matches:
                    if skill and any(term in str(skill).upper() for term in search_terms_upper):
                        skills_found['high_proficiency'].append(str(skill))
        
        # Check medium proficiency skills
        for col in ['SELF_ASSESMENT_SKILL_100', 'SELF_ASSESMENT_SKILL_200', 'MGR_SCORE_SKILL_100']:
            if pd.notna(row[col]) and row[col]:
                # Parse array format like other fields
                skill_data = row[col]
                if isinstance(skill_data, str):
                    skill_str = str(skill_data)
                    if skill_str.startswith('[') and skill_str.endswith(']'):
                        clean_skills = skill_str.strip('[]')
                        if '"' in clean_skills:
                            skill_matches = re.findall(r'"([^"]*)"', clean_skills)
                        elif "'" in clean_skills:
                            skill_matches = re.findall(r"'([^']*)'", clean_skills)
                        else:
                            skill_matches = [clean_skills.strip()] if clean_skills.strip() else []
                    else:
                        skill_matches = [skill_str.strip()] if skill_str.strip() else []
                else:
                    skill_matches = skill_data if isinstance(skill_data, list) else []
                
                for skill in skill_matches:
                    if skill and any(term in str(skill).upper() for term in search_terms_upper):
                        skills_found['medium_proficiency'].append(str(skill))
        
        # Check basic proficiency skills
        for col in ['SELF_ASSESMENT_SKILL_0', 'SELF_ASSESMENT_SKILL_NULL', 'MGR_SCORE_SKILL_0', 'MGR_SCORE_SKILL_NULL']:
            if pd.notna(row[col]) and row[col]:
                # Parse array format like other fields
                skill_data = row[col]
                if isinstance(skill_data, str):
                    skill_str = str(skill_data)
                    if skill_str.startswith('[') and skill_str.endswith(']'):
                        clean_skills = skill_str.strip('[]')
                        if '"' in clean_skills:
                            skill_matches = re.findall(r'"([^"]*)"', clean_skills)
                        elif "'" in clean_skills:
                            skill_matches = re.findall(r"'([^']*)'", clean_skills)
                        else:
                            skill_matches = [clean_skills.strip()] if clean_skills.strip() else []
                    else:
                        skill_matches = [skill_str.strip()] if skill_str.strip() else []
                else:
                    skill_matches = skill_data if isinstance(skill_data, list) else []
                
                for skill in skill_matches:
                    if skill and any(term in str(skill).upper() for term in search_terms_upper):
                        skills_found['basic_proficiency'].append(str(skill))
        
        # Check certifications
        for col in ['CERT_INTERNAL', 'CERT_EXTERNAL']:
            if pd.notna(row[col]) and row[col]:
                # Parse array format like other fields
                cert_data = row[col]
                if isinstance(cert_data, str):
                    cert_str = str(cert_data)
                    if cert_str.startswith('[') and cert_str.endswith(']'):
                        clean_certs = cert_str.strip('[]')
                        if '"' in clean_certs:
                            cert_matches = re.findall(r'"([^"]*)"', clean_certs)
                        elif "'" in clean_certs:
                            cert_matches = re.findall(r"'([^']*)'", clean_certs)
                        else:
                            cert_matches = [clean_certs.strip()] if clean_certs.strip() else []
                    else:
                        cert_matches = [cert_str.strip()] if cert_str.strip() else []
                else:
                    cert_matches = cert_data if isinstance(cert_data, list) else []
                
                for cert in cert_matches:
                    if cert and any(term in str(cert).upper() for term in search_terms_upper):
                        skills_found['certifications'].append(str(cert))
        
        # Check specialties
        if pd.notna(row['SPECIALTIES']) and row['SPECIALTIES']:
            # Parse array format like other fields
            specialty_data = row['SPECIALTIES']
            if isinstance(specialty_data, str):
                specialty_str = str(specialty_data)
                if specialty_str.startswith('[') and specialty_str.endswith(']'):
                    clean_specialties = specialty_str.strip('[]')
                    if '"' in clean_specialties:
                        specialty_matches = re.findall(r'"([^"]*)"', clean_specialties)
                    elif "'" in clean_specialties:
                        specialty_matches = re.findall(r"'([^']*)'", clean_specialties)
                    else:
                        specialty_matches = [clean_specialties.strip()] if clean_specialties.strip() else []
                else:
                    specialty_matches = [specialty_str.strip()] if specialty_str.strip() else []
            else:
                specialty_matches = specialty_data if isinstance(specialty_data, list) else []
            
            for specialty in specialty_matches:
                if specialty and any(term in str(specialty).upper() for term in search_terms_upper):
                    skills_found['specialties'].append(str(specialty))
        
        # Remove duplicates
        for key in skills_found:
            skills_found[key] = list(set(skills_found[key]))
        
        return skills_found
    
    def calculate_relevance_score(self, expert_data: Dict) -> float:
        """Calculate relevance score for expert ranking"""
        score = 0.0
        
        # Skill proficiency score (40% weight)
        skill_score = 0
        if expert_data.get('skills', {}).get('high_proficiency'):
            skill_score += 25  # High proficiency skills
        if expert_data.get('skills', {}).get('medium_proficiency'):
            skill_score += 12  # Medium proficiency skills
        if expert_data.get('skills', {}).get('basic_proficiency'):
            skill_score += 3   # Basic proficiency skills
        
        score += min(skill_score, 40)  # Cap at 40%
        
        # Certification bonus (15% weight)
        if expert_data.get('skills', {}).get('certifications'):
            score += min(len(expert_data['skills']['certifications']) * 3, 15)
        
        # Specialties bonus (10% weight) 
        if expert_data.get('skills', {}).get('specialties'):
            score += min(len(expert_data['skills']['specialties']) * 2, 10)
        
        # Enhanced Salesforce opportunity scoring (35% weight)
        opportunities = expert_data.get('opportunities', [])
        if opportunities:
            opp_score = 0
            total_acv = 0
            closed_won_count = 0
            
            for opp in opportunities:
                # Base points for any opportunity
                opp_score += 2
                
                # Bonus for Closed Won opportunities
                if opp.get('stage', '').upper().find('CLOSED WON') != -1:
                    closed_won_count += 1
                    opp_score += 5  # Extra points for wins
                
                # ACV-based scoring
                amount = opp.get('amount', 0)
                if amount and amount > 0:
                    total_acv += amount
                    # Bonus points based on deal size
                    if amount >= 1000000:  # $1M+
                        opp_score += 8
                    elif amount >= 500000:  # $500K+
                        opp_score += 5
                    elif amount >= 100000:  # $100K+
                        opp_score += 2
            
            # Additional bonus for multiple wins
            if closed_won_count >= 3:
                opp_score += 10
            elif closed_won_count >= 2:
                opp_score += 5
            
            # ACV portfolio bonus
            if total_acv >= 5000000:  # $5M+ portfolio
                opp_score += 10
            elif total_acv >= 2000000:  # $2M+ portfolio
                opp_score += 5
            
            score += min(opp_score, 35)  # Cap at 35%
        
        return round(score, 1)
    
    @st.cache_data(ttl=600)
    def get_all_sales_engineers(_self) -> pd.DataFrame:
        """Get all Sales Engineers with their college information and skills"""
        query = """
        SELECT 
            EMPLOYEE_ID,
            USER_ID,
            NAME,
            EMAIL,
            COLLEGE,
            SELF_ASSESMENT_SKILL_300,
            SELF_ASSESMENT_SKILL_400,
            MGR_SCORE_SKILL_300,
            MGR_SCORE_SKILL_400,
            SPECIALTIES,
            CERT_EXTERNAL,
            CERT_INTERNAL,
            EMPLOYERS
        FROM SALES.SE_REPORTING.FREESTYLE_SUMMARY 
        WHERE NAME IS NOT NULL
        ORDER BY NAME
        """
        
        try:
            result = _self.session.sql(query).to_pandas()
            return result
        except Exception as e:
            st.error(f"Error querying Sales Engineer data: {str(e)}")
            return pd.DataFrame()
    
    def extract_se_skills(self, row: pd.Series) -> Dict[str, List[str]]:
        """Extract skills for SE Directory display"""
        skills = {
            'high_skills': [],
            'medium_skills': [],
            'specialties': [],
            'certifications': []
        }
        
        # High proficiency skills (300-400 level)
        for col in ['SELF_ASSESMENT_SKILL_400', 'SELF_ASSESMENT_SKILL_300', 'MGR_SCORE_SKILL_400', 'MGR_SCORE_SKILL_300']:
            if pd.notna(row[col]) and row[col]:
                # Parse array format like other fields
                skill_data = row[col]
                if isinstance(skill_data, str):
                    skill_str = str(skill_data)
                    if skill_str.startswith('[') and skill_str.endswith(']'):
                        clean_skills = skill_str.strip('[]')
                        if '"' in clean_skills:
                            skill_matches = re.findall(r'"([^"]*)"', clean_skills)
                        elif "'" in clean_skills:
                            skill_matches = re.findall(r"'([^']*)'", clean_skills)
                        else:
                            skill_matches = [clean_skills.strip()] if clean_skills.strip() else []
                    else:
                        skill_matches = [skill_str.strip()] if skill_str.strip() else []
                else:
                    skill_matches = skill_data if isinstance(skill_data, list) else []
                
                for skill in skill_matches:
                    if skill and str(skill).strip():
                        skills['high_skills'].append(str(skill))
        
        # Specialties
        if pd.notna(row['SPECIALTIES']) and row['SPECIALTIES']:
            # Parse array format like other fields
            specialty_data = row['SPECIALTIES']
            if isinstance(specialty_data, str):
                specialty_str = str(specialty_data)
                if specialty_str.startswith('[') and specialty_str.endswith(']'):
                    clean_specialties = specialty_str.strip('[]')
                    if '"' in clean_specialties:
                        specialty_matches = re.findall(r'"([^"]*)"', clean_specialties)
                    elif "'" in clean_specialties:
                        specialty_matches = re.findall(r"'([^']*)'", clean_specialties)
                    else:
                        specialty_matches = [clean_specialties.strip()] if clean_specialties.strip() else []
                else:
                    specialty_matches = [specialty_str.strip()] if specialty_str.strip() else []
            else:
                specialty_matches = specialty_data if isinstance(specialty_data, list) else []
            
            for specialty in specialty_matches:
                if specialty and str(specialty).strip():
                    skills['specialties'].append(str(specialty))
        
        # Certifications
        for col in ['CERT_EXTERNAL', 'CERT_INTERNAL']:
            if pd.notna(row[col]) and row[col]:
                # Parse array format like other fields
                cert_data = row[col]
                if isinstance(cert_data, str):
                    cert_str = str(cert_data)
                    if cert_str.startswith('[') and cert_str.endswith(']'):
                        clean_certs = cert_str.strip('[]')
                        if '"' in clean_certs:
                            cert_matches = re.findall(r'"([^"]*)"', clean_certs)
                        elif "'" in clean_certs:
                            cert_matches = re.findall(r"'([^']*)'", clean_certs)
                        else:
                            cert_matches = [clean_certs.strip()] if clean_certs.strip() else []
                    else:
                        cert_matches = [cert_str.strip()] if cert_str.strip() else []
                else:
                    cert_matches = cert_data if isinstance(cert_data, list) else []
                
                for cert in cert_matches:
                    if cert and str(cert).strip():
                        skills['certifications'].append(str(cert))
        
        # Remove duplicates
        for key in skills:
            skills[key] = list(set(skills[key]))
        
        return skills
    
    @st.cache_data(ttl=600)
    def get_top_industries(_self) -> List[str]:
        """Get top 50 industries from opportunities in the past 3 years"""
        query = """
        SELECT 
            a.INDUSTRY,
            COUNT(*) as opportunity_count
        FROM FIVETRAN.SALESFORCE.OPPORTUNITY o
        LEFT JOIN FIVETRAN.SALESFORCE.ACCOUNT a ON o.ACCOUNT_ID = a.ID
        WHERE o.CLOSE_DATE >= DATEADD(year, -3, CURRENT_DATE())
            AND a.INDUSTRY IS NOT NULL
            AND a.INDUSTRY != ''
            AND (o.LEAD_SALES_ENGINEER_C IS NOT NULL OR o.OWNER_ID IS NOT NULL)
        GROUP BY a.INDUSTRY
        ORDER BY opportunity_count DESC
        LIMIT 50
        """
        
        try:
            result = _self.session.sql(query).to_pandas()
            industries = result['INDUSTRY'].tolist()
            return sorted(industries)  # Sort alphabetically
        except Exception as e:
            st.error(f"Error fetching industries: {str(e)}")
            return sorted(["Financial Services", "Healthcare", "Retail", "Manufacturing", "Technology"])  # Fallback sorted

@st.dialog("Sales Engineer Details")
def show_se_modal(expert_finder, se_row):
    """Display SE details in a modal dialog"""
    se_name = se_row['NAME'] if se_row['NAME'] else "Unknown SE"
    se_email = se_row['EMAIL'] if se_row['EMAIL'] else "No email"
    se_college = se_row['COLLEGE_CLEAN'] if se_row['COLLEGE_CLEAN'] else "Not specified"
    
    # Extract skills for this SE
    skills = expert_finder.extract_se_skills(se_row)
    
    st.subheader(f"💼 {se_name}")
    
    # Contact and background info
    st.write(f"**🎓 College:** {se_college}")
    
    # Show past employers if available
    if pd.notna(se_row['EMPLOYERS']) and se_row['EMPLOYERS']:
        employers_str = str(se_row['EMPLOYERS'])
        # Clean up array format similar to college
        if employers_str.startswith('[') and employers_str.endswith(']'):
            clean_employers = employers_str.strip('[]')
            if '"' in clean_employers:
                matches = re.findall(r'"([^"]*)"', clean_employers)
                if matches:
                    st.write(f"**🏢 Past Employers:** {', '.join(matches)}")
            elif "'" in clean_employers:
                matches = re.findall(r"'([^']*)'", clean_employers)
                if matches:
                    st.write(f"**🏢 Past Employers:** {', '.join(matches)}")
            else:
                clean_employers = clean_employers.strip()
                if clean_employers:
                    st.write(f"**🏢 Past Employers:** {clean_employers}")
        else:
            st.write(f"**🏢 Past Employers:** {employers_str}")
    
    st.markdown("---")
    
    # Skills summary
    st.write("**📊 Skills Summary:**")
    st.write(f"🎯 **{len(skills['high_skills'])} High Skills** | 🚀 **{len(skills['specialties'])} Specialties** | 🏅 **{len(skills['certifications'])} Certifications**")
    
    st.markdown("---")
    
    # High proficiency skills
    if skills['high_skills']:
        st.write("**🎯 High Proficiency Skills:**")
        skills_html = ""
        for skill in skills['high_skills'][:10]:  # Show more in modal
            skills_html += f'<span class="high-skill-tag">{skill}</span>'
        st.markdown(skills_html, unsafe_allow_html=True)
        st.write("")  # Add spacing
    
    # Specialties
    if skills['specialties']:
        st.write("**🚀 Specialties:**")
        spec_html = ""
        for spec in skills['specialties'][:8]:  # Show more in modal
            spec_html += f'<span class="skill-tag">{spec}</span>'
        st.markdown(spec_html, unsafe_allow_html=True)
        st.write("")  # Add spacing
    
    # Certifications
    if skills['certifications']:
        st.write("**🏅 Certifications:**")
        cert_html = ""
        for cert in skills['certifications'][:8]:  # Show more in modal
            cert_html += f'<span class="cert-tag">{cert}</span>'
        st.markdown(cert_html, unsafe_allow_html=True)
        st.write("")  # Add spacing
    
    # Email link if available
    if se_email and se_email != "No email":
        st.markdown(f"📧 **Email:** [{se_email}](mailto:{se_email})")

def main():
    st.markdown('<h1 class="main-header">🔍 Expert Finder</h1>', unsafe_allow_html=True)
    st.markdown("**Find internal experts for competitive opportunities using Freestyle skills and Salesforce data**")
    
    # Initialize the expert finder
    expert_finder = ExpertFinderSiS()
    
    # Tabs for different functionalities
    tab1, tab2 = st.tabs(["🔍 Expert Search", "👥 SE Directory"])
    
    with tab1:
        st.header("🔍 Search for Internal Experts")
        
        # Search and filter section
        with st.container():
            # Main search input
            search_input = st.text_input(
                "Search for expertise",
                placeholder="e.g., Databricks, AWS, Python, Machine Learning",
                help="Enter technologies, tools, or competitor names"
            )
            
            # Quick search buttons for common competitors
            st.subheader("🏆 Quick Search - Common Competitors")
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            with col1:
                if st.button("Databricks", use_container_width=True):
                    search_input = "Databricks"
            with col2:
                if st.button("Palantir", use_container_width=True):
                    search_input = "Palantir"
            with col3:
                if st.button("Tableau", use_container_width=True):
                    search_input = "Tableau"
            with col4:
                if st.button("AWS", use_container_width=True):
                    search_input = "AWS"
            with col5:
                if st.button("Microsoft", use_container_width=True):
                    search_input = "Microsoft"
            with col6:
                if st.button("Oracle", use_container_width=True):
                    search_input = "Oracle"
            
            # Advanced filters in an expandable section
            with st.expander("🔧 Advanced Filters", expanded=False):
                # First row of filters
                filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
                
                with filter_col1:
                    min_skill_level = st.selectbox(
                        "Minimum skill level",
                        ["Any", "Basic (0-100)", "Medium (200)", "High (300-400)"],
                        help="Filter by minimum self-assessed skill level"
                    )
                
                with filter_col2:
                    require_certifications = st.checkbox(
                        "Must have certifications",
                        help="Only show experts with relevant certifications"
                    )
                
                with filter_col3:
                    require_manager_endorsement = st.checkbox(
                        "Must have manager endorsement",
                        help="Only show experts with manager-scored skills"
                    )
                
                with filter_col4:
                    date_range = st.selectbox(
                        "Opportunity recency",
                        ["All time", "Last 6 months", "Last year", "Last 2 years"],
                        help="Filter by recent competitive opportunity involvement"
                    )
                
                # Second row for industry filter
                st.markdown("---")
                # Get top industries from actual data
                top_industries = expert_finder.get_top_industries()
                industry_filter = st.multiselect(
                    "🏢 Filter by Industry",
                    top_industries,
                    help="Select one or more industries to filter experts by their opportunity experience (top 50 from past 3 years)"
                )
            
            st.markdown("---")
        # Initialize variables
        sorted_experts = []
        
        # Main search logic
        if search_input:
            search_terms = [term.strip() for term in search_input.split(',')]
            
            with st.spinner("Searching for experts..."):
                # Search both data sources
                freestyle_df = expert_finder.search_freestyle_experts(search_terms)
                salesforce_df = expert_finder.search_salesforce_experts(search_terms)
                
                # Process Freestyle results
                experts_data = {}
                
                for _, row in freestyle_df.iterrows():
                    expert_id = row['EMPLOYEE_ID']
                    if pd.notna(expert_id):
                        skills = expert_finder.extract_relevant_skills(row, search_terms)
                        
                        # Apply skill level filter
                        include_expert = True
                        if min_skill_level == "High (300-400)" and not skills['high_proficiency']:
                            include_expert = False
                        elif min_skill_level == "Medium (200)" and not (skills['medium_proficiency'] or skills['high_proficiency']):
                            include_expert = False
                        
                        # Apply certification filter
                        if require_certifications and not skills['certifications']:
                            include_expert = False
                        
                        # Apply manager endorsement filter
                        if require_manager_endorsement:
                            has_mgr_endorsement = any([
                                row.get('MGR_SCORE_SKILL_100'), row.get('MGR_SCORE_SKILL_200'),
                                row.get('MGR_SCORE_SKILL_300'), row.get('MGR_SCORE_SKILL_400')
                            ])
                            if not has_mgr_endorsement:
                                include_expert = False
                        
                        # Industry filtering will be applied later using Salesforce account data
                        
                        if include_expert:
                            experts_data[expert_id] = {
                                'name': row['NAME'],
                                'email': row['EMAIL'],
                                'user_id': row['USER_ID'],
                                'skills': skills,
                                'employers': row.get('EMPLOYERS', []),
                                'industries': set(),  # Will be populated from Salesforce account data
                                'opportunities': [],
                                'opportunity_count': 0,
                                'last_activity': None
                            }
                
                # Process Salesforce results and link to experts
                sf_expert_map = {}  # Map SF user IDs to opportunity data
                
                for _, row in salesforce_df.iterrows():
                    # Collect industry information
                    account_industry = row.get('ACCOUNT_INDUSTRY')
                    
                    # Check Lead Sales Engineer
                    if pd.notna(row['LEAD_SALES_ENGINEER_C']):
                        se_id = row['LEAD_SALES_ENGINEER_C']
                        if se_id not in sf_expert_map:
                            sf_expert_map[se_id] = {'opportunities': [], 'count': 0, 'industries': set()}
                        
                        # Add industry if available
                        if pd.notna(account_industry) and account_industry:
                            sf_expert_map[se_id]['industries'].add(account_industry)
                        
                        opp_info = {
                            'name': row['OPPORTUNITY_NAME'],
                            'competitor': row['PRIMARY_COMPETITOR_C'],
                            'close_date': row['CLOSE_DATE'],
                            'stage': row['STAGE_NAME'],
                            'amount': row['AMOUNT'],
                            'industry': row.get('ACCOUNT_INDUSTRY', 'Unknown'),
                            'role': 'Lead Sales Engineer'
                        }
                        sf_expert_map[se_id]['opportunities'].append(opp_info)
                        sf_expert_map[se_id]['count'] = row['SE_OPPORTUNITY_COUNT']
                    
                    # Check Opportunity Owner
                    if pd.notna(row['OWNER_ID']):
                        owner_id = row['OWNER_ID']
                        if owner_id not in sf_expert_map:
                            sf_expert_map[owner_id] = {'opportunities': [], 'count': 0, 'industries': set()}
                        
                        # Add industry if available
                        if pd.notna(account_industry) and account_industry:
                            sf_expert_map[owner_id]['industries'].add(account_industry)
                        
                        opp_info = {
                            'name': row['OPPORTUNITY_NAME'],
                            'competitor': row['PRIMARY_COMPETITOR_C'],
                            'close_date': row['CLOSE_DATE'],
                            'stage': row['STAGE_NAME'],
                            'amount': row['AMOUNT'],
                            'industry': row.get('ACCOUNT_INDUSTRY', 'Unknown'),
                            'role': 'Opportunity Owner'
                        }
                        sf_expert_map[owner_id]['opportunities'].append(opp_info)
                        sf_expert_map[owner_id]['count'] = row['OWNER_OPPORTUNITY_COUNT']
                
                # Try to match Salesforce data to Freestyle experts by USER_ID or EMAIL
                for expert_id, data in experts_data.items():
                    user_id = data.get('user_id')
                    if user_id in sf_expert_map:
                        data['opportunities'] = sf_expert_map[user_id]['opportunities']
                        data['opportunity_count'] = sf_expert_map[user_id]['count']
                        data['industries'] = sf_expert_map[user_id]['industries']
                        if data['opportunities']:
                            data['last_activity'] = max(opp['close_date'] for opp in data['opportunities'])
                
                # Apply date range filter
                if date_range != "All time":
                    cutoff_date = datetime.now()
                    if date_range == "Last 6 months":
                        cutoff_date -= timedelta(days=180)
                    elif date_range == "Last year":
                        cutoff_date -= timedelta(days=365)
                    elif date_range == "Last 2 years":
                        cutoff_date -= timedelta(days=730)
                    
                    filtered_experts = {}
                    for expert_id, data in experts_data.items():
                        if data['last_activity'] and data['last_activity'] >= cutoff_date.date():
                            filtered_experts[expert_id] = data
                        elif not data['last_activity']:  # Include if no opportunity data but has skills
                            filtered_experts[expert_id] = data
                    experts_data = filtered_experts
                
                # Apply industry filter if specified
                if industry_filter:
                    filtered_by_industry = {}
                    for expert_id, data in experts_data.items():
                        expert_industries = data.get('industries', set())
                        if expert_industries:
                            # Check if any of the expert's industries match the filter
                            if any(industry in industry_filter for industry in expert_industries):
                                filtered_by_industry[expert_id] = data
                        # If no industry data but no industry filter, keep the expert
                    experts_data = filtered_by_industry
                
                # Calculate relevance scores and sort
                for expert_id, data in experts_data.items():
                    data['relevance_score'] = expert_finder.calculate_relevance_score(data)
                
                sorted_experts = sorted(
                    experts_data.items(),
                    key=lambda x: x[1]['relevance_score'],
                    reverse=True
                )
        
        # Display results
        if search_input and sorted_experts:
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(
                    f'<div class="metric-card"><h3>{len(sorted_experts)}</h3><p>Experts Found</p></div>',
                    unsafe_allow_html=True
                )
            
            with col2:
                total_opportunities = sum(data['opportunity_count'] for _, data in sorted_experts)
                st.markdown(
                    f'<div class="metric-card"><h3>{total_opportunities}</h3><p>Total Opportunities</p></div>',
                    unsafe_allow_html=True
                )
            
            with col3:
                avg_score = sum(data['relevance_score'] for _, data in sorted_experts) / len(sorted_experts)
                st.markdown(
                    f'<div class="metric-card"><h3>{avg_score:.1f}</h3><p>Avg Relevance Score</p></div>',
                    unsafe_allow_html=True
                )
            
            with col4:
                certified_experts = sum(1 for _, data in sorted_experts 
                                      if data['skills']['certifications'])
                st.markdown(
                    f'<div class="metric-card"><h3>{certified_experts}</h3><p>Certified Experts</p></div>',
                    unsafe_allow_html=True
                )
            
            st.markdown("---")
            
            # Expert cards
            st.subheader(f"🏆 All Experts for '{search_input}' (sorted by relevance)")
            
            for expert_id, data in sorted_experts:  # Show all experts
                # Handle None values for display
                display_name = data['name'] if data['name'] else "Unknown Expert"
                display_email = data['email'] if data['email'] else "No email available"
                display_user_id = data['user_id'] if data['user_id'] else "No user ID"
                
                with st.expander(f"⭐ {display_name} - Relevance Score: {data['relevance_score']}", expanded=False):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.write(f"**📧 Email:** {display_email}")
                        st.write(f"**🆔 User ID:** {display_user_id}")
                        
                        # Show industry if available
                        if data.get('industries'):
                            industries_list = list(data['industries'])
                            if industries_list:
                                # Show up to 3 industries
                                industries_display = ', '.join(industries_list[:3])
                                if len(industries_list) > 3:
                                    industries_display += f" (+{len(industries_list)-3} more)"
                                st.write(f"**🏭 Industry Experience:** {industries_display}")
                        
                        # Skills by proficiency level
                        skills = data['skills']
                        
                        if skills['high_proficiency']:
                            st.write("**🎯 High Proficiency Skills:**")
                            skills_html = ""
                            for skill in skills['high_proficiency'][:8]:
                                skills_html += f'<span class="high-skill-tag">{skill}</span>'
                            st.markdown(skills_html, unsafe_allow_html=True)
                        
                        if skills['medium_proficiency']:
                            st.write("**📊 Medium Proficiency Skills:**")
                            skills_html = ""
                            for skill in skills['medium_proficiency'][:8]:
                                skills_html += f'<span class="skill-tag">{skill}</span>'
                            st.markdown(skills_html, unsafe_allow_html=True)
                        
                        if skills['certifications']:
                            st.write("**🏅 Certifications:**")
                            cert_html = ""
                            for cert in skills['certifications'][:5]:
                                cert_html += f'<span class="cert-tag">{cert}</span>'
                            st.markdown(cert_html, unsafe_allow_html=True)
                        
                        if skills['specialties']:
                            st.write("**🚀 Specialties:**")
                            spec_html = ""
                            for spec in skills['specialties'][:5]:
                                spec_html += f'<span class="skill-tag">{spec}</span>'
                            st.markdown(spec_html, unsafe_allow_html=True)
                    
                    with col2:
                        st.metric("Relevant Opportunities", data['opportunity_count'])
                        if data['last_activity']:
                            days_ago = (datetime.now().date() - data['last_activity']).days
                            st.metric("Last Activity", f"{days_ago} days ago")
                        
                        # Show total skill count
                        total_skills = sum(len(skills[key]) for key in ['high_proficiency', 'medium_proficiency', 'basic_proficiency'])
                        st.metric("Total Relevant Skills", total_skills)
                    
                    # Recent opportunities
                    if data['opportunities']:
                        st.write("**📊 Recent Competitive Opportunities:**")
                        opp_data = []
                        for opp in data['opportunities'][:5]:
                            opp_data.append({
                                'Opportunity': opp['name'],
                                'Competitor': opp['competitor'],
                                'Industry': opp.get('industry', 'Unknown'),
                                'Role': opp['role'],
                                'Stage': opp['stage'],
                                'Close Date': opp['close_date'],
                                'Amount': f"${opp['amount']:,.0f}" if opp['amount'] else "N/A"
                            })
                        
                        if opp_data:
                            opp_df = pd.DataFrame(opp_data)
                            st.dataframe(opp_df, use_container_width=True)
                    
                    # Action buttons
                    # Contact information
                    if data['email']:
                        st.markdown(f"📧 **Email:** [{data['email']}](mailto:{data['email']})")
        
        elif search_input and len(sorted_experts) == 0:
            st.warning("No experts found matching your search criteria. Try adjusting your filters or search terms.")
    
    with tab2:
        st.header("👥 Sales Engineers Directory")
        st.markdown("**Browse all Sales Engineers and filter by college**")
        
        # Get all Sales Engineers data
        se_df = expert_finder.get_all_sales_engineers()
        
        if not se_df.empty:
            # Get unique colleges for filter (handle None values and array format)
            def extract_college_name(college_data):
                if pd.isna(college_data) or not college_data:
                    return None
                
                # Convert to string and clean up array/list format
                college_str = str(college_data)
                
                # Handle array format like [ "Data Engineering & Lake"]
                if college_str.startswith('[') and college_str.endswith(']'):
                    # Remove brackets and split by comma
                    clean_str = college_str.strip('[]')
                    # Handle quoted strings
                    if '"' in clean_str:
                        # Extract text between quotes
                        matches = re.findall(r'"([^"]*)"', clean_str)
                        if matches:
                            return matches[0].strip()
                    # Handle single quotes
                    elif "'" in clean_str:
                        matches = re.findall(r"'([^']*)'", clean_str)
                        if matches:
                            return matches[0].strip()
                    else:
                        # Just remove brackets if no quotes
                        return clean_str.strip()
                
                # If not array format, return as is
                return college_str.strip()
            
            # Apply college extraction and get unique values
            se_df['COLLEGE_CLEAN'] = se_df['COLLEGE'].apply(extract_college_name)
            unique_colleges = se_df['COLLEGE_CLEAN'].dropna().unique().tolist()
            unique_colleges = [col for col in unique_colleges if col and str(col).strip()]
            unique_colleges.sort()
            
            # Add "All" option
            college_options = ["All"] + unique_colleges
            
            # Filters above the table
            filter_col1, filter_col2, filter_col3 = st.columns([2, 2, 2])
            
            with filter_col1:
                selected_college = st.selectbox(
                    "Filter by College",
                    college_options,
                    help="Select a college to filter the results"
                )
            
            with filter_col2:
                name_search = st.text_input(
                    "Search by Name",
                    placeholder="Enter name to search...",
                    help="Search for specific Sales Engineers by name"
                )
                            # Filter the dataframe
            filtered_df = se_df.copy()
            
            # Apply college filter
            if selected_college != "All":
                filtered_df = filtered_df[filtered_df['COLLEGE_CLEAN'] == selected_college]
            
            # Apply name search
            if name_search:
                filtered_df = filtered_df[
                    filtered_df['NAME'].str.contains(name_search, case=False, na=False)
                ]
            
            # Display metrics
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric("Total SEs", len(se_df))
            with col_b:
                st.metric("Filtered Results", len(filtered_df))
            with col_c:
                colleges_with_data = se_df['COLLEGE_CLEAN'].dropna().nunique()
                st.metric("Colleges Represented", colleges_with_data)
            
            # Prepare display dataframe
            display_data = []
            for idx, (_, se_row) in enumerate(filtered_df.iterrows()):
                se_name = se_row['NAME'] if se_row['NAME'] else "Unknown SE"
                se_email = se_row['EMAIL'] if se_row['EMAIL'] else "No email"
                se_college = se_row['COLLEGE_CLEAN'] if se_row['COLLEGE_CLEAN'] else "Not specified"
                se_id = se_row['EMPLOYEE_ID']
                
                # Extract skills for skill count
                skills = expert_finder.extract_se_skills(se_row)
                total_skills = len(skills['high_skills']) + len(skills['specialties']) + len(skills['certifications'])
                
                display_data.append({
                    'Name': se_name,
                    'Email': se_email,
                    'College': se_college,
                    'Skills': total_skills,
                    'EMPLOYEE_ID': se_id  # Hidden for selection reference
                })
            
            # Create and display table
            if display_data:
                table_df = pd.DataFrame(display_data)
                
                st.subheader(f"👥 Sales Engineers ({len(filtered_df)} results)")
                st.markdown("*Click on any row to view detailed skills and expertise*")
                
                # Display table with selection capability
                event = st.dataframe(
                    table_df.drop('EMPLOYEE_ID', axis=1),  # Hide the EMPLOYEE_ID column
                    use_container_width=True,
                    height=500,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row"
                )
                
                # Handle row selection and show modal
                if event.selection.rows:
                    selected_row_idx = event.selection.rows[0]
                    selected_se_id = display_data[selected_row_idx]['EMPLOYEE_ID']
                    
                    # Find the selected SE's data
                    selected_se_data = filtered_df[filtered_df['EMPLOYEE_ID'] == selected_se_id]
                    if not selected_se_data.empty:
                        se_row = selected_se_data.iloc[0]
                        
                        # Show modal with SE details
                        show_se_modal(expert_finder, se_row)
        else:
            st.error("No Sales Engineer data available. Please check the database connection.")
    
    # Help section
    with st.expander("ℹ️ How to use Expert Finder"):
        st.markdown("""
        ### App Features:
        - **Expert Search**: Find experts by technology/competitor with smart relevance scoring
        - **SE Directory**: Browse all Sales Engineers with skills, college, and contact info
        
        ### Data Sources:
        - **Freestyle Skills**: Self-assessed and manager-endorsed skills from the SE_Reporting schema
        - **Salesforce Opportunities**: Historical competitive deals from Fivetran Salesforce data
        
        ### Skill Proficiency Levels:
        - **High (300-400)**: Expert-level skills, manager-endorsed capabilities
        - **Medium (100-200)**: Intermediate proficiency
        - **Basic (0-null)**: Foundational knowledge
        
        ### Search Tips:
        - **Technology**: `Databricks`, `AWS`, `Python`, `Snowflake`
        - **Competitors**: `Palantir`, `Tableau`, `Microsoft`
        - **Multiple terms**: `Databricks, Analytics, Python` (comma-separated)
        
        ### Enhanced Relevance Scoring:
        - **Skills (40%)**: Weighted by proficiency level (300-400 get highest scores)
        - **Competitive Experience (35%)**: Closed Won opportunities and ACV size matter most
        - **Certifications (15%)**: Relevant external and internal certifications
        - **Specialties (10%)**: Domain expertise and focus areas
        
        ### Opportunity Scoring Bonuses:
        - **Closed Won**: +5 points per win vs other stages
        - **Deal Size**: $1M+ deals get +8 points, $500K+ get +5 points
        - **Portfolio Value**: $5M+ total ACV gets +10 bonus points
        - **Multiple Wins**: 3+ wins get +10 bonus, 2+ wins get +5 bonus
        """)

if __name__ == "__main__":
    main() 
