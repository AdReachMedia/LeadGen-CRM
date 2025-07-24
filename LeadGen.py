# -----------------------------------------------------------------------------
# LeadGen CRM - Finale Version mit Supabase-Integration (v9.3 - Final RLS Upsert Fix)
# -----------------------------------------------------------------------------

# --- 1. IMPORTS & SETUP ---
import streamlit as st
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import base64
from datetime import datetime, date, timedelta
from supabase import create_client, Client
import numpy as np
from urllib.parse import quote
import plotly.graph_objects as go

st.set_page_config(page_title="LeadGen CRM", layout="wide")

# --- 3. SUPABASE SETUP & GLOBALE VARIABLEN ---
@st.cache_resource
def init_supabase_client():
    try: url = st.secrets["supabase"]["url"]; key = st.secrets["supabase"]["key"]; return create_client(url, key)
    except Exception as e: return None
supabase: Client = init_supabase_client()
LEADS_TABLE = "leads"
TASKS_TABLE = "tasks"
NOTES_TABLE = "notes"
PRIMARY_COLOR = "#ff7f02"

def clean_row_for_supabase(row_dict):
    cleaned_dict = {}
    for key, value in row_dict.items():
        if pd.isna(value): cleaned_dict[key] = None
        else: cleaned_dict[key] = value
    return cleaned_dict

# --- 4. DATABASE & SCRAPER FUNCTIONS (JETZT MIT user_id) ---
def get_user_id():
    if "user" in st.session_state and st.session_state.user:
        return st.session_state.user.user.id
    return None

def scrape_gelbeseiten(query, location, max_results):
    user_id = get_user_id()
    if not user_id: return []
    st.info(f"🔎 Suche auf GelbeSeiten.de: '{query}' in '{location}'...")
    chrome_options = Options(); chrome_options.add_argument("--headless"); chrome_options.add_argument("--disable-gpu"); chrome_options.add_argument("--no-sandbox"); chrome_options.add_argument("--window-size=1920x1080"); chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    try: service = Service(ChromeDriverManager().install()); driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e: st.error(f"❌ Fehler bei ChromeDriver: {e}"); return []
    url_query = query.replace(" ", "-").lower(); url_location = location.replace(" ", "-").lower(); search_url = f"https://www.gelbeseiten.de/branchen/{url_query}/{url_location}"; driver.get(search_url)
    try: cookie_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(translate(., 'A..Z', 'a..z'), 'akzeptieren')]"))); cookie_button.click(); time.sleep(2)
    except Exception: st.warning("Cookie-Banner nicht gefunden. Fahre fort...")
    results = []; processed_hashes = set(); campaign_name = f"GelbeSeiten: {query} ({location})"
    while len(results) < max_results:
        try: WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'article.mod-Treffer')))
        except Exception: break
        cards = driver.find_elements(By.CSS_SELECTOR, 'article.mod-Treffer')
        if not cards: break
        for card in cards:
            if len(results) >= max_results: break
            card_hash = hash(card.get_attribute('innerHTML'));
            if card_hash in processed_hashes: continue
            processed_hashes.add(card_hash); name, address, phone, website_url = '', '', '', ''
            try: name = card.find_element(By.CSS_SELECTOR, 'h2').text.strip()
            except: continue
            try: address = card.find_element(By.CSS_SELECTOR, '.mod-AdresseKompakt__adress-text').text.strip()
            except: pass
            try: phone = card.find_element(By.CSS_SELECTOR, '.mod-TelefonnummerKompakt__phoneNumber').text.strip()
            except: pass
            try:
                website_span = card.find_element(By.CSS_SELECTOR, '.mod-WebseiteKompakt__text'); website_encoded = website_span.get_attribute('data-webseitelink')
                if website_encoded: website_url = base64.b64decode(website_encoded).decode('utf-8')
            except: pass
            results.append({'name': name, 'branche': query, 'address': address, 'phone': phone, 'email': '', 'website': website_url, 'contact_person': '', 'status': None, 'campaign': campaign_name, 'is_archived': False, 'user_id': user_id})
    driver.quit()
    return results[:max_results]

def save_leads_to_supabase(leads_data):
    user_id = get_user_id()
    if not user_id: st.error("Nicht eingeloggt."); return
    for lead in leads_data: lead['user_id'] = user_id
    cleaned_data = [clean_row_for_supabase(row) for row in leads_data]
    try: response = supabase.table(LEADS_TABLE).insert(cleaned_data).execute(); st.success(f"{len(response.data)} Leads erfolgreich gespeichert.")
    except Exception as e: st.error(f"Fehler beim Speichern in Supabase: {e}")

@st.cache_data(ttl=30)
def load_all_leads_data():
    user_id = get_user_id()
    if not user_id: return pd.DataFrame()
    try:
        response = supabase.table(LEADS_TABLE).select("*").eq('user_id', user_id).order("id", desc=True).execute(); df = pd.DataFrame(response.data)
        expected_cols = ['id', 'name', 'branche', 'address', 'phone', 'email', 'website', 'contact_person', 'status', 'campaign', 'is_archived', 'user_id', 'created_at']
        for col in expected_cols:
            if col not in df.columns: df[col] = pd.NA if col != 'is_archived' else False
        df['is_archived'] = df['is_archived'].fillna(False); return df
    except Exception as e: st.error(f"Fehler beim Laden von Supabase: {e}"); return pd.DataFrame()

def get_unique_campaigns(archived=False):
    user_id = get_user_id()
    if not user_id: return []
    try:
        query = supabase.table(LEADS_TABLE).select("campaign").eq("is_archived", archived).eq('user_id', user_id); response = query.execute()
        if response.data: return sorted(list(set([c['campaign'] for c in response.data if c['campaign']])))
        return []
    except Exception: return []

@st.cache_data(ttl=60)
def get_all_leads_for_dropdown(archived=False):
    user_id = get_user_id()
    if not user_id: return []
    try:
        response = supabase.table(LEADS_TABLE).select("id, name, campaign").eq("is_archived", archived).eq('user_id', user_id).order("name").execute()
        return response.data
    except Exception: return []

def get_lead_details(lead_id):
    user_id = get_user_id()
    if not user_id: return None
    try:
        response = supabase.table(LEADS_TABLE).select("*").eq("id", lead_id).eq('user_id', user_id).single().execute()
        return response.data
    except Exception: return None

def add_task(lead_id, due_date, description):
    user_id = get_user_id()
    if not user_id: return
    try:
        supabase.table(TASKS_TABLE).insert({"lead_id": lead_id, "due_date": str(due_date), "description": description, 'user_id': user_id}).execute()
        st.toast("Aufgabe erfolgreich erstellt!", icon="✅"); st.cache_data.clear()
    except Exception as e: st.error(f"Fehler beim Erstellen der Aufgabe: {e}")

@st.cache_data(ttl=10)
def load_open_tasks(lead_id=None):
    user_id = get_user_id()
    if not user_id: return []
    try:
        query = supabase.table(TASKS_TABLE).select("*, leads!inner(id, name, status, is_archived)").eq("is_completed", False).eq("leads.is_archived", False).eq('user_id', user_id).order("due_date")
        if lead_id: query = query.eq("lead_id", lead_id)
        response = query.execute()
        return response.data
    except Exception as e: st.error(f"Fehler beim Laden der Aufgaben: {e}"); return []

def complete_task(task_id):
    try:
        supabase.table(TASKS_TABLE).update({"is_completed": True}).eq("id", task_id).execute()
        st.toast("Aufgabe erledigt!", icon="🎉"); st.cache_data.clear()
    except Exception as e: st.error(f"Fehler beim Abschließen der Aufgabe: {e}")

def update_task(task_id, new_due_date, new_description):
    try:
        supabase.table(TASKS_TABLE).update({"due_date": str(new_due_date), "description": new_description}).eq("id", task_id).execute()
        st.toast("Aufgabe aktualisiert!", icon="🔄"); st.cache_data.clear()
    except Exception as e: st.error(f"Fehler beim Aktualisieren der Aufgabe: {e}")

def delete_task(task_id):
    try:
        supabase.table(TASKS_TABLE).delete().eq("id", task_id).execute()
        st.toast("Aufgabe endgültig gelöscht!", icon="🗑️"); st.cache_data.clear()
    except Exception as e: st.error(f"Fehler beim Löschen der Aufgabe: {e}")

def update_lead_status(lead_id, new_status):
    try:
        supabase.table(LEADS_TABLE).update({"status": new_status}).eq("id", lead_id).execute()
        st.toast(f"Status aktualisiert auf: {new_status}", icon="📝"); st.cache_data.clear()
    except Exception as e: st.error(f"Fehler beim Ändern des Status: {e}")

def archive_campaign(campaign_name):
    user_id = get_user_id()
    if not user_id: return
    try:
        supabase.table(LEADS_TABLE).update({"is_archived": True}).eq("campaign", campaign_name).eq('user_id', user_id).execute()
        st.success(f"Kampagne '{campaign_name}' wurde archiviert."); st.cache_data.clear()
    except Exception as e: st.error(f"Fehler beim Archivieren: {e}")

def restore_campaign(campaign_name):
    user_id = get_user_id()
    if not user_id: return
    try:
        supabase.table(LEADS_TABLE).update({"is_archived": False}).eq("campaign", campaign_name).eq('user_id', user_id).execute()
        st.success(f"Kampagne '{campaign_name}' wurde wiederhergestellt."); st.cache_data.clear()
    except Exception as e: st.error(f"Fehler beim Wiederherstellen: {e}")

@st.cache_data(ttl=10)
def load_notes(lead_id):
    user_id = get_user_id()
    if not user_id: return []
    try:
        response = supabase.table(NOTES_TABLE).select("*").eq("lead_id", lead_id).eq('user_id', user_id).order("created_at", desc=True).execute()
        return response.data
    except Exception as e: st.error(f"Fehler beim Laden der Notizen: {e}"); return []

def add_note(lead_id, content):
    user_id = get_user_id()
    if not user_id: return
    try:
        supabase.table(NOTES_TABLE).insert({"lead_id": lead_id, "content": content, "user_id": user_id}).execute()
        st.toast("Notiz gespeichert!"); st.cache_data.clear()
    except Exception as e: st.error(f"Fehler beim Speichern der Notiz: {e}")

def delete_note(note_id):
    try:
        supabase.table(NOTES_TABLE).delete().eq("id", note_id).execute()
        st.toast("Notiz gelöscht!"); st.cache_data.clear()
    except Exception as e: st.error(f"Fehler beim Löschen der Notiz: {e}")

# --- 7. STREAMLIT UI ---
if not supabase:
    st.error("Supabase-Verbindung konnte nicht hergestellt werden. Bitte überprüfen Sie die `secrets.toml`.")
    st.stop()

if 'user' not in st.session_state:
    st.session_state.user = None

if not st.session_state.user:
    st.header("LeadGen CRM Login")
    with st.form("login_form"):
        email = st.text_input("E-Mail")
        password = st.text_input("Passwort", type="password")
        submitted = st.form_submit_button("Login")
        if submitted:
            try:
                user_session = supabase.auth.sign_in_with_password({"email": email, "password": password})
                supabase.auth.set_session(user_session.session.access_token, user_session.session.refresh_token)
                st.session_state.user = user_session
                st.rerun()
            except Exception:
                st.error(f"Login fehlgeschlagen. Überprüfen Sie Ihre Eingaben.")
else:
    if 'page' not in st.session_state: st.session_state.page = "🏠 Startseite"
    
    st.sidebar.title("Navigation")
    st.sidebar.write(f"Eingeloggt als:")
    st.sidebar.write(f"**{st.session_state.user.user.email}**")
    if st.sidebar.button("Logout", use_container_width=True):
        st.session_state.user = None
        supabase.auth.sign_out()
        st.rerun()
    st.sidebar.markdown("---")
    st.sidebar.write("Gehe zu:")
    
    page_options = ["🏠 Startseite", "📊 Dashboard", "☑️ Aufgaben", "🗄️ Archiv", "👤 Lead-Details", "🗓️ Termin anlegen", "🧮 Kennzahl-Hypothese", "🔎 LeadFinder", "📅 TagesGeschäft"]
    for page in page_options:
        if st.sidebar.button(page, use_container_width=True, type="primary" if st.session_state.page == page else "secondary"):
            st.session_state.page = page
            st.rerun()
    st.sidebar.markdown("---")
    st.sidebar.caption("LeadGen CRM v9.3 | Final")
    st.header(st.session_state.page)
    
    def go_to_page(page_name): st.session_state.page = page_name
    
    if st.session_state.page == "🏠 Startseite":
        st.subheader("Ihre Top-Kennzahlen (aktive Kampagnen)")
        all_leads_df = load_all_leads_data(); active_leads_df = all_leads_df[all_leads_df['is_archived'] == False]
        if active_leads_df.empty: st.info("Keine aktiven Leads vorhanden. Zeit, neue zu generieren!")
        else:
            total_leads = len(active_leads_df); leads_open = active_leads_df[(active_leads_df['status'].str.contains("Offen", na=False)) | (active_leads_df['status'].isna())].shape[0]
            leads_followup = active_leads_df[active_leads_df['status'] == "🟣 FollowUp"].shape[0]; leads_converted = active_leads_df[active_leads_df['status'] == "🟡 Termin vereinbart"].shape[0]
            col1, col2, col3, col4 = st.columns(4); col1.metric("Aktive Leads", total_leads); col2.metric("🟢 Offen", leads_open); col3.metric("🟣 FollowUp", leads_followup); col4.metric("🟡 Termin vereinbart", leads_converted)
        st.markdown("---"); st.subheader("🔥 Ihre dringendsten Aufgaben")
        open_tasks = load_open_tasks(); today = date.today()
        urgent_tasks = [t for t in open_tasks if datetime.strptime(t['due_date'], '%Y-%m-%d').date() <= today]
        if not urgent_tasks: st.success("Super! Keine dringenden Aufgaben für heute.")
        else:
            for task in urgent_tasks[:5]:
                lead_name = task['leads']['name'] if task.get('leads') else "Unbekannter Lead"
                due_date_str = datetime.strptime(task['due_date'], '%Y-%m-%d').strftime('%d.%m.%Y')
                st.warning(f"**Lead:** {lead_name} - **Fällig:** {due_date_str}\n\n*Notiz: {task['description']}*")
            if len(urgent_tasks) > 0: st.button("Alle Aufgaben anzeigen", on_click=go_to_page, args=("☑️ Aufgaben",), type="primary")
        st.markdown("---"); st.subheader("Schnellzugriff")
        col1, col2 = st.columns(2)
        col1.button("➕ Neue Aufgabe erstellen", on_click=go_to_page, args=("☑️ Aufgaben",), use_container_width=True)
        col2.button("📥 Leads importieren", on_click=go_to_page, args=("🔎 LeadFinder",), use_container_width=True)

    elif st.session_state.page == "📊 Dashboard":
        st.info("Analysieren Sie Ihre Akquise-Performance. Nutzen Sie die Filter, um die Daten nach Ihren Wünschen einzugrenzen.")
        today = date.today(); last_month = today - timedelta(days=30)
        col1, col2 = st.columns(2); start_date = col1.date_input("Startdatum", last_month); end_date = col2.date_input("Enddatum", today)
        include_archived = st.toggle("Archivierte Kampagnen einbeziehen")
        all_leads_df = load_all_leads_data()
        all_leads_df['created_at_date'] = pd.to_datetime(all_leads_df['created_at']).dt.date
        df_for_analysis = all_leads_df[(all_leads_df['created_at_date'] >= start_date) & (all_leads_df['created_at_date'] <= end_date)]
        if not include_archived: df_for_analysis = df_for_analysis[df_for_analysis['is_archived'] == False]
        if df_for_analysis.empty: st.warning("Keine Leads im ausgewählten Zeitraum gefunden.")
        else:
            EMPTY_STATUS_OPTION = "-- Leer --"; STATUS_OPTIONS = [EMPTY_STATUS_OPTION, "🟢 Offen", "🔵 Erreicht", "🔴 Nicht erreicht", "🟣 FollowUp", "🟡 Termin vereinbart", "🟤 Kein Interesse"]
            all_campaigns_in_view = sorted(list(df_for_analysis['campaign'].dropna().unique())); filter_options = ["Alle Kampagnen anzeigen"] + all_campaigns_in_view
            selected_campaign = st.selectbox("Nach Kampagne filtern:", options=filter_options)
            df_filtered = df_for_analysis if selected_campaign == "Alle Kampagnen anzeigen" else df_for_analysis[df_for_analysis['campaign'] == selected_campaign]
            st.markdown("---"); total_leads = len(df_filtered); st.metric("Gesamtzahl Leads in Auswahl", f"{total_leads}")
            st.subheader("Sales Funnel")
            funnel_order = ["🟢 Offen", "🔵 Erreicht", "🟣 FollowUp", "🟡 Termin vereinbart"]
            status_counts = df_filtered['status'].fillna("🟢 Offen").value_counts()
            funnel_values = [status_counts.get(status, 0) for status in funnel_order]
            fig = go.Figure(go.Funnel(y=funnel_order, x=funnel_values, textposition="inside", textinfo="value+percent initial", marker={"color": PRIMARY_COLOR}))
            st.plotly_chart(fig, use_container_width=True)
            if selected_campaign == "Alle Kampagnen anzeigen":
                st.markdown("---"); st.subheader("Kampagnen-Performance im Vergleich")
                campaign_performance = df_filtered.groupby('campaign').agg(Anzahl_Leads=('id', 'count'), Termine_vereinbart=('status', lambda s: s.str.contains("Termin vereinbart", na=False).sum())).reset_index()
                campaign_performance['Konversionsrate (%)'] = (campaign_performance['Termine_vereinbart'] / campaign_performance['Anzahl_Leads'] * 100).round(1)
                st.dataframe(campaign_performance.sort_values(by="Konversionsrate (%)", ascending=False), use_container_width=True)

    elif st.session_state.page == "☑️ Aufgaben":
        with st.expander("Neue Aufgabe manuell erstellen", expanded=False):
            leads_for_dropdown = get_all_leads_for_dropdown()
            if not leads_for_dropdown: st.warning("Es sind keine aktiven Leads vorhanden.")
            else:
                lead_options = {f"{lead['name']} (ID: {lead['id']})": lead['id'] for lead in leads_for_dropdown}
                with st.form("new_task_form", clear_on_submit=True):
                    selected_lead_display = st.selectbox("Lead auswählen:", options=lead_options.keys())
                    due_date_input = st.date_input("Fälligkeitsdatum:", min_value=date.today())
                    description_input = st.text_area("Notiz / Beschreibung:")
                    if st.form_submit_button("Aufgabe erstellen"):
                        if not description_input: st.error("Bitte geben Sie eine Beschreibung ein.")
                        else: add_task(lead_options[selected_lead_display], due_date_input, description_input); st.rerun()
        st.markdown("---"); st.subheader("Offene Aufgaben für aktive Leads")
        open_tasks = load_open_tasks()
        if not open_tasks: st.success("🎉 Super! Keine offenen Aufgaben vorhanden.")
        else:
            today = date.today(); urgent_tasks = [t for t in open_tasks if datetime.strptime(t['due_date'], '%Y-%m-%d').date() <= today]; future_tasks = [t for t in open_tasks if datetime.strptime(t['due_date'], '%Y-%m-%d').date() > today]
            EMPTY_STATUS_OPTION = "-- Leer --"; status_options = [EMPTY_STATUS_OPTION, "🟢 Offen", "🔵 Erreicht", "🔴 Nicht erreicht", "🟣 FollowUp", "🟡 Termin vereinbart", "🟤 Kein Interesse"]
            def display_task_list(tasks, title, expanded_default):
                if "Dringend" in title: st.warning(title)
                else: st.info(title)
                for task in tasks:
                    if not task.get('leads'): continue
                    lead_name = task['leads']['name']; lead_status = task['leads'].get('status') or EMPTY_STATUS_OPTION; due_date_str = datetime.strptime(task['due_date'], '%Y-%m-%d').strftime('%d.%m.%Y')
                    with st.expander(f"**Lead:** {lead_name} - **Fällig:** {due_date_str}", expanded=expanded_default):
                        col1, col2 = st.columns(2)
                        with col1:
                            current_status_index = status_options.index(lead_status) if lead_status in status_options else 0
                            new_status = st.selectbox("Lead-Status ändern:", options=status_options, index=current_status_index, key=f"status_{task['id']}")
                            if new_status != lead_status:
                                status_to_save = None if new_status == EMPTY_STATUS_OPTION else new_status
                                update_lead_status(task['leads']['id'], status_to_save)
                                if new_status != "🟣 FollowUp": complete_task(task['id'])
                                st.rerun()
                        with col2: new_desc = st.text_area("Beschreibung:", value=task['description'], key=f"desc_{task['id']}")
                        new_date = st.date_input("Fälligkeit:", value=datetime.strptime(task['due_date'], '%Y-%m-%d').date(), key=f"date_{task['id']}")
                        st.write("")
                        b_col1, b_col2, b_col3 = st.columns(3)
                        if b_col1.button("✎ Details speichern", key=f"save_{task['id']}"): update_task(task['id'], new_date, new_desc); st.rerun()
                        if b_col2.button("✓ Erledigt", key=f"done_{task['id']}", type="primary"): complete_task(task['id']); st.rerun()
                        if b_col3.button("🗑️ Löschen", key=f"delete_task_main_{task['id']}"): delete_task(task['id']); st.rerun()
            if urgent_tasks: display_task_list(urgent_tasks, "🔥 Dringend: Fällig & Überfällig", True)
            if future_tasks: display_task_list(future_tasks, "🗓️ Zukünftige Aufgaben", False)

    elif st.session_state.page == "🗄️ Archiv":
        st.info("Hier finden Sie alle Kampagnen, die Sie aus der Hauptansicht entfernt haben. Sie können sie hier einsehen, wiederherstellen oder endgültig löschen.")
        archived_campaigns = get_unique_campaigns(archived=True)
        if not archived_campaigns: st.success("Das Archiv ist leer.")
        else:
            for campaign in archived_campaigns:
                with st.container(border=True):
                    c1, c2, c3 = st.columns([4,1,1])
                    c1.subheader(f"{campaign}")
                    with c2:
                        if st.button(f"🔄 Wiederherstellen", key=f"restore_{campaign}", use_container_width=True): restore_campaign(campaign); st.rerun()
                    with c3:
                        if st.button(f"🔥 Endgültig löschen", key=f"delete_perm_{campaign}", type="primary", use_container_width=True):
                            st.session_state.campaign_to_delete_perm = campaign; st.rerun()
                    if st.session_state.get("campaign_to_delete_perm") == campaign:
                        st.warning(f"**Sind Sie absolut sicher?** Das Löschen der Kampagne **'{campaign}'** und aller zugehörigen Leads und Aufgaben kann nicht rückgängig gemacht werden.")
                        c1, c2 = st.columns(2)
                        if c1.button("Ja, endgültig löschen", key=f"confirm_delete_{campaign}"):
                            with st.spinner("Lösche endgültig..."): supabase.table(LEADS_TABLE).delete().eq("campaign", campaign).execute(); del st.session_state.campaign_to_delete_perm; st.cache_data.clear(); st.rerun()
                        if c2.button("Abbrechen", key=f"cancel_delete_{campaign}"): del st.session_state.campaign_to_delete_perm; st.rerun()

    elif st.session_state.page == "👤 Lead-Details":
        st.info("Wählen Sie einen Lead aus, um seine vollständige Akte mit allen Details und Aktivitäten einzusehen.")
        active_leads = get_all_leads_for_dropdown(archived=False)
        if not active_leads:
            st.warning("Keine aktiven Leads vorhanden.")
        else:
            leads_by_campaign = {}
            for lead in active_leads:
                campaign = lead['campaign'] or "Ohne Kampagne"
                if campaign not in leads_by_campaign: leads_by_campaign[campaign] = []
                leads_by_campaign[campaign].append(f"{lead['name']} (ID: {lead['id']})")
            selected_campaign = st.selectbox("1. Kampagne auswählen:", options=list(leads_by_campaign.keys()))
            if selected_campaign:
                lead_display_options = ["-- 2. Lead auswählen --"] + leads_by_campaign[selected_campaign]
                selected_lead_display = st.selectbox("2. Lead auswählen:", options=lead_display_options)
                if selected_lead_display != "-- 2. Lead auswählen --":
                    lead_id = int(selected_lead_display.split("(ID: ")[1].replace(")", ""))
                    lead_details = get_lead_details(lead_id)
                    if lead_details:
                        st.markdown("---"); status = lead_details.get('status') or "-- Leer --"
                        st.subheader(f"Lead-Akte: {lead_details['name']}")
                        st.write(f"**Status:** {status} | **Kampagne:** {lead_details['campaign']}")
                        st.write(f"📞 {lead_details.get('phone') or 'N/A'} | 📧 {lead_details.get('email') or 'N/A'} | 🌐 [{lead_details.get('website') or 'Keine Webseite'}]({lead_details.get('website')})")
                        st.markdown("---")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("#### Stammdaten")
                            st.text(f"Branche: {lead_details.get('branche') or 'N/A'}"); st.text(f"Adresse: {lead_details.get('address') or 'N/A'}"); st.text(f"Ansprechpartner: {lead_details.get('contact_person') or 'N/A'}")
                        with col2:
                            st.write("#### Aktivitäten")
                            tab1, tab2 = st.tabs(["☑️ Offene Aufgaben", "📝 Notizen"])
                            with tab1:
                                lead_tasks = load_open_tasks(lead_id=lead_id)
                                if not lead_tasks: st.info("Keine offenen Aufgaben für diesen Lead.")
                                else:
                                    for task in lead_tasks:
                                        due_date_str = datetime.strptime(task['due_date'], '%Y-%m-%d').strftime('%d.%m.%Y')
                                        c1, c2 = st.columns([4, 1])
                                        c1.markdown(f"**Fällig am {due_date_str}:** {task['description']}")
                                        if c2.button("🗑️", key=f"delete_task_details_{task['id']}", help="Aufgabe endgültig löschen"):
                                            delete_task(task['id']); st.rerun()
                                with st.form(f"task_form_{lead_id}", clear_on_submit=True):
                                    st.write("**Neue Aufgabe erstellen**"); desc = st.text_input("Beschreibung"); due = st.date_input("Fälligkeitsdatum", min_value=date.today())
                                    if st.form_submit_button("Aufgabe speichern"):
                                        add_task(lead_id, due, desc); st.rerun()
                            with tab2:
                                lead_notes = load_notes(lead_id=lead_id)
                                if not lead_notes: st.info("Keine Notizen für diesen Lead.")
                                else:
                                    for note in lead_notes:
                                        note_date = datetime.fromisoformat(note['created_at']).strftime('%d.%m.%Y, %H:%M')
                                        c1,c2 = st.columns([4,1]); c1.markdown(f"**{note_date}**"); c1.text(note['content'])
                                        if c2.button("🗑️", key=f"delete_note_{note['id']}", help="Notiz löschen"):
                                            delete_note(note['id']); st.rerun()
                                with st.form(f"note_form_{lead_id}", clear_on_submit=True):
                                    st.write("**Neue Notiz erstellen**"); content = st.text_area("Inhalt")
                                    if st.form_submit_button("Notiz speichern"):
                                        add_note(lead_id, content); st.rerun()
    
    elif st.session_state.page == "🗓️ Termin anlegen":
        st.info("Wählen Sie einen Lead aus, um die Daten vorzufüllen, oder geben Sie die Daten manuell ein, um einen Termin zu buchen.")
        leads_for_dropdown = get_all_leads_for_dropdown()
        lead_options = {f"{lead['name']} (ID: {lead['id']})": lead['id'] for lead in leads_for_dropdown}
        selected_lead_display = st.selectbox("Optional: Daten aus Lead vorfüllen:", options=["-- Manueller Eintrag --"] + list(lead_options.keys()), key="calendly_lead_selector")
        if selected_lead_display != "-- Manueller Eintrag --" and st.session_state.get("last_selected_lead") != selected_lead_display:
            selected_lead_id = lead_options[selected_lead_display]
            lead_details = get_lead_details(selected_lead_id)
            st.session_state.booking_lead_id = selected_lead_id; st.session_state.booking_name = lead_details.get("name", ""); st.session_state.booking_email = lead_details.get("email", ""); st.session_state.last_selected_lead = selected_lead_display
        elif selected_lead_display == "-- Manueller Eintrag --":
            if "last_selected_lead" not in st.session_state or st.session_state.last_selected_lead != "-- Manueller Eintrag --":
                st.session_state.booking_lead_id = None; st.session_state.booking_name = ""; st.session_state.booking_email = ""; st.session_state.last_selected_lead = "-- Manueller Eintrag --"
        st.subheader("Daten für die Terminbuchung")
        with st.form("booking_form"):
            name_input = st.text_input("Name des Ansprechpartners:", value=st.session_state.get("booking_name", "")); email_input = st.text_input("E-Mail des Ansprechpartners:", value=st.session_state.get("booking_email", ""))
            submitted = st.form_submit_button("🗓️ Kalender laden")
            if submitted: st.session_state.final_booking_name = name_input; st.session_state.final_booking_email = email_input; st.session_state.show_calendly = True
        if st.session_state.get("show_calendly"):
            st.markdown("---"); st.subheader("Termin in Calendly auswählen")
            calendly_url = f"https://calendly.com/adreachmedia/30min?name={quote(st.session_state.final_booking_name)}&email={quote(st.session_state.final_booking_email)}"
            st.components.v1.iframe(calendly_url, height=750, scrolling=True)
            st.markdown("---")
            if st.session_state.booking_lead_id:
                st.info("Nachdem Sie den Termin gebucht haben, klicken Sie hier, um den Lead-Status im CRM zu aktualisieren.")
                if st.button(f"✓ Status für '{st.session_state.booking_name}' aktualisieren"):
                    update_lead_status(st.session_state.booking_lead_id, "🟡 Termin vereinbart")
                    st.session_state.show_calendly = False; st.session_state.booking_lead_id = None; st.rerun()
    
    elif st.session_state.page == "🧮 Kennzahl-Hypothese":
        st.markdown(f"""<style>.kennzahl-box{{background-color:{PRIMARY_COLOR};color:white;padding:2rem;border-radius:1rem;}}.kennzahl-box h2{{font-size:1.5rem;margin-top:1rem;}}</style>""", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1: gewinn_pro_neukunde = st.number_input("Gewinn pro Neukunde in EUR", value=3000, min_value=0); werbebudget = st.number_input("Mtl. Werbebudget in EUR", value=3000, min_value=0)
        with c2: kosten_pro_lead = st.number_input("Voraussichtliche Kosten pro Lead in EUR", value=100, min_value=1); abschlussquote = st.slider("Abschlussquote in %", 0, 100, 60, step=5)
        anzahl_leads = werbebudget / kosten_pro_lead if kosten_pro_lead else 0; anzahl_neukunden = anzahl_leads * (abschlussquote / 100); potenzieller_verdienst = anzahl_neukunden * gewinn_pro_neukunde; roas = potenzieller_verdienst / werbebudget if werbebudget else 0; gewinn = potenzieller_verdienst - werbebudget
        st.markdown(f"""<div class="kennzahl-box"><h2>Potenzielle Leads: <strong>{anzahl_leads:.0f}</strong></h2><h2>Potenzielle Neukunden: <strong>{anzahl_neukunden:.1f}</strong></h2><h2>Potenzieller Verdienst: <strong>{potenzieller_verdienst:,.0f} EUR</strong></h2><h2>ROAS: <strong>{roas:.1f}x</strong></h2><h2>Gewinn: <strong>{gewinn:,.0f} EUR</strong></h2></div>""", unsafe_allow_html=True)
    
    elif st.session_state.page == "🔎 LeadFinder":
        st.subheader("1. Leads über GelbeSeiten.de finden");
        with st.form("search_form"):
            branche = st.text_input("Branche", "Steuerberater"); ort = st.text_input("Ort oder PLZ", "Berlin"); max_results = st.slider("Maximale Anzahl Leads", 10, 200, 20, step=10)
            submit_button = st.form_submit_button("🚀 Leads suchen")
        if submit_button:
            with st.spinner(f"Suche nach '{branche}' in '{ort}'..."):
                leads = scrape_gelbeseiten(branche, ort, max_results)
                if leads: save_leads_to_supabase(leads); st.balloons()
                else: st.warning("⚠️ Keine Leads für diese Suche gefunden.")
        st.markdown("---"); st.subheader("2. Leads aus CSV-Datei importieren")
        uploaded_file = st.file_uploader("CSV-Datei hochladen", type=["csv"])
        if uploaded_file is not None:
            try:
                df_uploaded = pd.read_csv(uploaded_file); st.dataframe(df_uploaded.head())
                EMPTY_MAPPING_OPTION = "-- Nicht zuordnen --"; uploaded_cols = [EMPTY_MAPPING_OPTION] + list(df_uploaded.columns)
                db_fields = {"name": "Name", "branche": "Branche", "address": "Adresse", "phone": "Telefon", "email": "E-Mail", "website": "Webseite", "contact_person": "Ansprechpartner"}
                APIFY_DEFAULT_MAPPING = {"name": "title", "branche": "categoryName", "address": "address", "phone": "phone", "email": "emails/0", "website": "domain", "contact_person": None}
                st.markdown("---"); campaign_name_input = st.text_input("Wie soll diese Import-Gruppe heißen? (z.B. 'Apify Steuerberater Berlin')", placeholder="Pflichtfeld")
                st.warning("Ordnen Sie die Spalten Ihrer Datei zu. Die Felder wurden basierend auf typischen Apify-Namen vorausgewählt.")
                mapping = {}; col1, col2 = st.columns(2); field_items = list(db_fields.items())
                for i, (field_key, field_label) in enumerate(field_items):
                    target_col = col1 if i < (len(field_items) + 1) / 2 else col2
                    with target_col:
                        default_col_name = APIFY_DEFAULT_MAPPING.get(field_key); default_index = 0
                        if default_col_name and default_col_name in uploaded_cols: default_index = uploaded_cols.index(default_col_name)
                        mapping[field_key] = st.selectbox(f'"{field_label}" ist Spalte:', options=uploaded_cols, key=f"map_{field_key}", index=default_index)
                if st.button("✅ Zuordnung bestätigen & Leads importieren", type="primary"):
                    if not campaign_name_input: st.error("Bitte geben Sie einen Namen für die Kampagne an!")
                    elif mapping['name'] == EMPTY_MAPPING_OPTION: st.error("Bitte ordnen Sie mindestens das Feld 'Name' zu!")
                    else:
                        try:
                            with st.spinner("Verarbeite und importiere Leads..."):
                                df_final = pd.DataFrame();
                                for db_col, csv_col in mapping.items():
                                    if csv_col != EMPTY_MAPPING_OPTION: df_final[db_col] = df_uploaded[csv_col]
                                for col in db_fields.keys():
                                    if col not in df_final.columns: df_final[col] = None
                                df_final['status'] = None; df_final['campaign'] = campaign_name_input; df_final['is_archived'] = False
                                leads_to_save = df_final.to_dict(orient='records'); save_leads_to_supabase(leads_to_save); st.balloons()
                        except Exception as e: st.error(f"Fehler beim Zuordnen der Daten: {e}")
            except Exception as e: st.error(f"Fehler beim Lesen der CSV: {e}")

    elif st.session_state.page == "📅 TagesGeschäft":
        if 'confirm_delete_campaign' not in st.session_state: st.session_state.confirm_delete_campaign = False
        if 'confirm_archive_campaign' not in st.session_state: st.session_state.confirm_archive_campaign = False
        all_campaigns = get_unique_campaigns(archived=False);
        if all_campaigns:
            selected_campaign = st.selectbox("Aktive Kampagne anzeigen:", options=["Alle Kampagnen anzeigen"] + all_campaigns, key="campaign_selector")
            all_leads_df = load_all_leads_data()
            leads_df_original = all_leads_df[all_leads_df['is_archived'] == False]
            if selected_campaign != "Alle Kampagnen anzeigen":
                leads_df_original = leads_df_original[leads_df_original['campaign'] == selected_campaign]
        else: st.info("Noch keine aktiven Kampagnen vorhanden."); leads_df_original = pd.DataFrame()
        if not leads_df_original.empty:
            if 'df_before_edit' not in st.session_state or not st.session_state.df_before_edit.equals(leads_df_original):
                 st.session_state.df_before_edit = leads_df_original.copy()
            
            df_for_display = leads_df_original.copy()
            df_for_display['Notizen_Aktion'] = False

            st.info(f"{len(leads_df_original)} Leads in der Ansicht.")
            EMPTY_STATUS_OPTION = "-- Leer --"; status_options = [EMPTY_STATUS_OPTION, "🟢 Offen", "🔵 Erreicht", "🔴 Nicht erreicht", "🟣 FollowUp", "🟡 Termin vereinbart", "🟤 Kein Interesse"]
            df_for_display['status'] = df_for_display['status'].fillna(EMPTY_STATUS_OPTION)
            
            edited_df = st.data_editor(df_for_display.drop(columns=['is_archived', 'user_id']),
                column_config={
                    "Notizen_Aktion": st.column_config.CheckboxColumn("📝 Notizen", help="Häkchen setzen, um Notizen zu verwalten."),
                    "id": st.column_config.NumberColumn("ID", disabled=True), "name": st.column_config.TextColumn("Name", required=True), 
                    "website": st.column_config.LinkColumn("Webseite", validate="^https?://"), 
                    "status": st.column_config.SelectboxColumn("Status", help="Der aktuelle Status des Leads", width="medium", options=status_options, required=True), 
                    "campaign": st.column_config.TextColumn("Kampagne"),
                }, hide_index=True, key="data_editor",
                column_order=["Notizen_Aktion", "name", "status", "campaign", "branche", "address", "phone", "email", "website", "contact_person"])
            
            selected_rows = edited_df[edited_df.Notizen_Aktion]
            if not selected_rows.empty:
                lead_data = selected_rows.iloc[0].to_dict()
                st.session_state.current_lead_for_notes = lead_data
                st.session_state.show_notes_dialog = True
                st.rerun()

            if st.session_state.get("show_notes_dialog"):
                @st.dialog(f"Notizen für: {st.session_state.current_lead_for_notes['name']}")
                def notes_dialog():
                    lead_id = st.session_state.current_lead_for_notes['id']
                    st.write("### Bisherige Notizen")
                    notes = load_notes(lead_id)
                    if not notes: st.info("Noch keine Notizen vorhanden.")
                    else:
                        for note in notes:
                            col1, col2 = st.columns([4, 1])
                            with col1: st.markdown(f"**{datetime.fromisoformat(note['created_at']).strftime('%d.%m.%Y, %H:%M')}**"); st.text(note['content'])
                            with col2:
                                if st.button("🗑️", key=f"delete_note_{note['id']}", help="Notiz löschen"):
                                    delete_note(note['id']); st.rerun()
                            st.markdown("---")
                    new_note = st.text_area("Neue Notiz hinzufügen:")
                    if st.button("Notiz speichern", type="primary"):
                        if new_note: add_note(lead_id, new_note); st.rerun()
                notes_dialog()
            
            st.markdown("---")
            save_col, _, action_col = st.columns([2, 2, 1])
            with save_col:
                if st.button("💾 Änderungen an Leads speichern", use_container_width=True):
                    df_to_save_final = edited_df.drop(columns=['Notizen_Aktion'])
                    with st.spinner("Speichere Änderungen..."):
                        df_before = st.session_state.df_before_edit; df_before_filled = df_before.fillna({'status': ''}); edited_df_filled = df_to_save_final.fillna({'status': ''})
                        merged_df = pd.merge(df_before_filled, edited_df_filled, on='id', suffixes=('_before', '_after'), how='outer')
                        followup_leads = merged_df[(merged_df['status_before'] != "🟣 FollowUp") & (merged_df['status_after'] == "🟣 FollowUp")]
                        tasks_created = 0
                        for _, lead in followup_leads.iterrows():
                            add_task(lead_id=int(lead['id']), due_date=date.today() + timedelta(days=7), description="Follow-Up"); tasks_created += 1
                        if tasks_created > 0: st.success(f"{tasks_created} neue Follow-Up Aufgabe(n) automatisch erstellt!")
                        df_to_save = df_to_save_final.copy(); df_to_save['status'] = df_to_save['status'].apply(lambda x: None if x == EMPTY_STATUS_OPTION else x);
                        original_ids = set(df_before['id'].dropna().astype(int)); updates = []; inserts = []
                        for _, row in df_to_save.iterrows():
                            cleaned_row = clean_row_for_supabase(row.to_dict()); row_id = cleaned_row.get('id')
                            if pd.notna(row_id) and int(row_id) in original_ids:
                                del cleaned_row['id']
                                updates.append((row_id, cleaned_row))
                            elif 'name' in cleaned_row and pd.notna(cleaned_row.get('name')):
                                if 'id' in cleaned_row: del cleaned_row['id']
                                inserts.append(cleaned_row)
                        for row_id, data in updates: supabase.table(LEADS_TABLE).update(data).eq('id', row_id).execute()
                        if inserts: supabase.table(LEADS_TABLE).insert(inserts).execute()
                        edited_ids = set(df_to_save['id'].dropna().astype(int)); deleted_ids = list(original_ids - edited_ids)
                        if deleted_ids: supabase.table(LEADS_TABLE).delete().in_("id", list(deleted_ids)).execute()
                        st.success("Änderungen erfolgreich gespeichert!"); del st.session_state.df_before_edit; st.rerun()
            if selected_campaign != "Alle Kampagnen anzeigen":
                with action_col:
                    action_cols = st.columns(2)
                    with action_cols[0]:
                        if st.button("🗄️ Archivieren", help="Diese Kampagne ins Archiv verschieben", use_container_width=True):
                            st.session_state.confirm_archive_campaign = True; st.rerun()
                    with action_cols[1]:
                        if st.button("🗑️ Löschen", help="Diese Kampagne endgültig löschen", use_container_width=True):
                            st.session_state.confirm_delete_campaign = True; st.rerun()
                if st.session_state.get('confirm_archive_campaign'):
                    st.warning(f"**Sicher, dass Sie die Kampagne '{selected_campaign}' archivieren möchten?**")
                    c1, c2, c3 = st.columns([1,1,2])
                    if c1.button("Ja, archivieren", type="primary"):
                        archive_campaign(selected_campaign); st.session_state.confirm_archive_campaign = False; st.rerun()
                    if c2.button("Abbrechen"):
                        st.session_state.confirm_archive_campaign = False; st.rerun()
                if st.session_state.get('confirm_delete_campaign'):
                    st.warning(f"**Sicher, dass Sie die gesamte Kampagne '{selected_campaign}' und alle zugehörigen Aufgaben löschen möchten?**")
                    c1, c2, c3 = st.columns([1,1,2])
                    if c1.button("Ja, wirklich löschen", type="primary"):
                        with st.spinner(f"Lösche Kampagne '{selected_campaign}'..."):
                            supabase.table(LEADS_TABLE).delete().eq("campaign", selected_campaign).execute()
                            st.session_state.confirm_delete_campaign = False; st.cache_data.clear(); st.rerun()
                    if c2.button("Abbrechen"):
                        st.session_state.confirm_delete_campaign = False; st.rerun()