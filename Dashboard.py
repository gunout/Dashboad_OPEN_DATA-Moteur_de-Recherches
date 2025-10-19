# dashboard_open_data_moteur_recherches_realtime.py
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
from datetime import datetime, timedelta
import folium
from streamlit_folium import folium_static
from collections import Counter
import warnings
import traceback
import sys
import time
import asyncio
import threading
warnings.filterwarnings('ignore')

# Configuration de la page
st.set_page_config(
    page_title="DASHBOARD OPEN DATA - MOTEUR DE RECHERCHES - TEMPS RÉEL",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé avec animations pour le temps réel
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #0055A4;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
        background: linear-gradient(45deg, #0055A4, #EF4135);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.8; }
        100% { opacity: 1; }
    }
    
    .realtime-indicator {
        display: inline-block;
        width: 10px;
        height: 10px;
        background-color: #28a745;
        border-radius: 50%;
        animation: blink 1s infinite;
        margin-right: 8px;
    }
    
    @keyframes blink {
        0%, 50% { opacity: 1; }
        25%, 75% { opacity: 0.3; }
    }
    
    .section-header {
        color: #0055A4;
        border-bottom: 2px solid #EF4135;
        padding-bottom: 0.5rem;
        margin-top: 2rem;
        font-weight: 600;
    }
    
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #0055A4;
        margin: 0.5rem 0;
        transition: all 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }
    
    .dataset-card {
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        border-left: 5px solid #0055A4;
        background-color: #f8f9fa;
        transition: transform 0.2s;
    }
    
    .dataset-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }
    
    .organization-card {
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        border-left: 5px solid #EF4135;
        background-color: #f8f9fa;
        transition: transform 0.2s;
    }
    
    .organization-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }
    
    .ministry-card {
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        border-left: 5px solid #002395;
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .ministry-badge {
        background: linear-gradient(45deg, #002395, #0055A4);
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: bold;
        display: inline-block;
        margin: 0.2rem;
    }
    
    .priority-badge {
        background: linear-gradient(45deg, #FF6B35, #F7931E);
        color: white;
        padding: 0.2rem 0.6rem;
        border-radius: 15px;
        font-size: 0.7rem;
        font-weight: bold;
        display: inline-block;
        margin-left: 0.5rem;
    }
    
    .search-engine-badge {
        background: linear-gradient(45deg, #28a745, #20c997);
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: bold;
        display: inline-block;
        margin: 0.2rem;
    }
    
    .realtime-badge {
        background: linear-gradient(45deg, #dc3545, #fd7e14);
        color: white;
        padding: 0.2rem 0.6rem;
        border-radius: 15px;
        font-size: 0.7rem;
        font-weight: bold;
        display: inline-block;
        animation: pulse 1.5s infinite;
    }
    
    .badge {
        display: inline-block;
        padding: 0.25rem 0.5rem;
        border-radius: 15px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 0.5rem;
        margin-bottom: 0.5rem;
    }
    
    .trending-badge {
        background: linear-gradient(45deg, #EF4135, #FF6B35);
        color: white;
    }
    
    .new-badge {
        background-color: #28a745;
        color: white;
    }
    
    .popular-badge {
        background-color: #0055A4;
        color: white;
    }
    
    .format-badge {
        background-color: #6c757d;
        color: white;
    }
    
    .theme-badge {
        background-color: #17a2b8;
        color: white;
    }
    
    .api-badge {
        background-color: #6610f2;
        color: white;
    }
    
    .search-box {
        border: 2px solid #0055A4;
        border-radius: 25px;
        padding: 10px 20px;
    }
    
    .footer {
        background-color: #0055A4;
        color: white;
        padding: 2rem;
        margin-top: 3rem;
        border-radius: 10px;
    }
    
    .error-box {
        background-color: #ffebee;
        border: 1px solid #f44336;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .success-box {
        background-color: #e8f5e9;
        border: 1px solid #4caf50;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .info-box {
        background-color: #e3f2fd;
        border: 1px solid #2196f3;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .update-notification {
        background: linear-gradient(45deg, #28a745, #20c997);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
        animation: slideIn 0.5s ease;
    }
    
    @keyframes slideIn {
        from {
            transform: translateX(-100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    
    .stButton button {
        width: 100%;
    }
    
    .org-stats {
        font-size: 0.9rem;
        color: #666;
    }
    
    .loading-spinner {
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 2rem;
    }
    
    .last-update {
        font-size: 0.8rem;
        color: #666;
        font-style: italic;
    }
</style>
""", unsafe_allow_html=True)

class RealTimeOpenDataDashboard:
    def __init__(self):
        self.base_url = "https://www.data.gouv.fr/api/1"
        self.debug_mode = True
        self.auto_refresh_enabled = False
        self.refresh_interval = 30  # secondes
        self.last_update_time = None
        self._initialize_session_state()
        
    def _initialize_session_state(self):
        """Initialise l'état de session avec gestion d'erreurs"""
        try:
            if 'initialized' not in st.session_state:
                st.session_state.initialized = True
                st.session_state.datasets_loaded = False
                st.session_state.datasets = []
                st.session_state.organizations = []
                st.session_state.reuses = []
                st.session_state.search_results = []
                st.session_state.current_view = "popular"
                st.session_state.current_query = ""
                st.session_state.error_count = 0
                st.session_state.last_error = None
                st.session_state.debug_info = []
                st.session_state.stats_cache = None
                st.session_state.loading_progress = 0
                st.session_state.total_pages = 0
                st.session_state.current_page = 1
                st.session_state.last_refresh = None
                st.session_state.auto_refresh = False
                st.session_state.refresh_history = []
                st.session_state.new_datasets_count = 0
                st.session_state.realtime_updates = []
                st.session_state.update_count = 0  # CORRECTION : Initialisation ajoutée
                
                if self.debug_mode:
                    st.session_state.debug_info.append("Session temps réel initialisée avec succès")
        except Exception as e:
            self.log_error(f"Erreur initialisation session: {str(e)}")
            raise

    def log_error(self, message):
        """Enregistre les erreurs pour le débogage"""
        try:
            error_msg = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
            st.session_state.error_count += 1
            st.session_state.last_error = error_msg
            st.session_state.debug_info.append(error_msg)
            print(error_msg, file=sys.stderr)
        except:
            print(f"Erreur critique: {message}", file=sys.stderr)

    def log_update(self, message):
        """Enregistre les mises à jour en temps réel"""
        try:
            update_msg = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
            st.session_state.realtime_updates.append(update_msg)
            st.session_state.update_count += 1
            
            # Garder seulement les 50 dernières mises à jour
            if len(st.session_state.realtime_updates) > 50:
                st.session_state.realtime_updates = st.session_state.realtime_updates[-50:]
                
            if self.debug_mode:
                print(update_msg, file=sys.stdout)
        except:
            print(f"Erreur log update: {message}", file=sys.stderr)

    def safe_get(self, obj, keys, default="Non spécifié"):
        """Méthode sécurisée pour accéder aux données imbriquées"""
        try:
            if obj is None:
                return default
                
            for key in keys.split('.'):
                if isinstance(obj, dict) and key in obj:
                    obj = obj[key]
                else:
                    return default
            return obj if obj is not None else default
        except Exception as e:
            self.log_error(f"Erreur safe_get: {str(e)}")
            return default

    def safe_upper(self, text):
        """Convertit en majuscules de manière sécurisée"""
        if text is None:
            return ""
        return str(text).upper()

    def is_ministry(self, org_name):
        """Vérifie si une organisation est un ministère"""
        if not org_name:
            return False
        
        ministry_keywords = [
            'ministère', 'ministere', 'ministre',
            'gouvernement', 'etat', 'état',
            'administration', 'service public',
            'direction générale', 'direction',
            'agence', 'office', 'secrétariat',
            'département', 'cabinet'
        ]
        
        org_name_lower = org_name.lower()
        return any(keyword in org_name_lower for keyword in ministry_keywords)

    def get_ministry_priority_score(self, org_name):
        """Calcule un score de priorité pour les ministères"""
        if not org_name:
            return 0
        
        priority_keywords = {
            'ministère': 10, 'ministere': 10,
            'économie': 9, 'finances': 9, 'budget': 9,
            'santé': 8, 'sante': 8, 'social': 8,
            'éducation': 8, 'education': 8, 'enseignement': 8,
            'intérieur': 8, 'interieur': 8, 'sécurité': 8,
            'écologie': 7, 'ecologie': 7, 'environnement': 7,
            'transition': 7, 'énergie': 7,
            'justice': 7, 'défense': 7, 'defense': 7,
            'culture': 6, 'communication': 6,
            'agriculture': 6, 'alimentation': 6,
            'travail': 6, 'emploi': 6,
            'outre-mer': 5, 'outremer': 5,
            'gouvernement': 5, 'etat': 5, 'état': 5
        }
        
        org_name_lower = org_name.lower()
        score = 0
        
        for keyword, points in priority_keywords.items():
            if keyword in org_name_lower:
                score += points
        
        return score

    def get_massive_demo_data(self):
        """Génère un grand nombre de datasets de démonstration"""
        try:
            base_datasets = [
                {
                    'id': 'demo1',
                    'title': 'Base SIRENE - Entreprises françaises',
                    'description': 'Base officielle des entreprises et établissements français',
                    'organization': {'name': 'INSEE', 'id': 'insee'},
                    'license': 'Licence Ouverte v2.0',
                    'metrics': {'reuses': 12500, 'views': 85000, 'followers': 2500},
                    'resources': [
                        {'format': 'csv', 'url': '#'},
                        {'format': 'json', 'url': '#'}
                    ],
                    'page': 'https://www.data.gouv.fr',
                    'created_at': '2023-01-01',
                    'last_modified': '2024-01-01'
                },
                {
                    'id': 'demo2',
                    'title': 'Budget de l\'État - Loi de finances',
                    'description': 'Données budgétaires détaillées de l\'État français',
                    'organization': {'name': 'Ministère de l\'Économie, des Finances et de la Souveraineté industrielle', 'id': 'finances'},
                    'license': 'Licence Ouverte v2.0',
                    'metrics': {'reuses': 8500, 'views': 45000, 'followers': 1800},
                    'resources': [
                        {'format': 'xlsx', 'url': '#'},
                        {'format': 'csv', 'url': '#'}
                    ],
                    'page': 'https://www.data.gouv.fr',
                    'created_at': '2023-06-01',
                    'last_modified': '2024-01-15'
                },
                {
                    'id': 'demo3',
                    'title': 'Émissions de gaz à effet de serre par secteur',
                    'description': 'Données sur les émissions de CO2 par secteur économique',
                    'organization': {'name': 'Ministère de la Transition Écologique et de la Cohésion des Territoires', 'id': 'mte'},
                    'license': 'ODbL',
                    'metrics': {'reuses': 3200, 'views': 28000, 'followers': 900},
                    'resources': [
                        {'format': 'json', 'url': '#'},
                        {'format': 'geojson', 'url': '#'}
                    ],
                    'page': 'https://www.data.gouv.fr',
                    'created_at': '2023-03-01',
                    'last_modified': '2023-12-01'
                }
            ]
            
            # Génération de datasets variés
            organizations = [
                'INSEE', 'Ministère de l\'Économie', 'Ministère de la Transition Écologique',
                'Ministère de l\'Éducation', 'Ministère de l\'Intérieur', 'Ministère de la Santé',
                'Ministère du Travail', 'Ministère de la Justice', 'Ministère de l\'Agriculture',
                'Ministère des Armées', 'Ministère de la Culture', 'IGN', 'CEREMA',
                'ADEME', 'ANSES', 'ANSM', 'ARS', 'DGFIP', 'Direction Générale de la Santé',
                'Service Public', 'Agence Nationale', 'Direction Régionale', 'Collectivité Territoriale'
            ]
            
            themes = [
                'démographique', 'économique', 'social', 'environnemental', 'sanitaire',
                'éducatif', 'transport', 'urbanisme', 'agricole', 'énergétique',
                'culturel', 'touristique', 'industriel', 'financier', 'budgétaire',
                'judiciaire', 'administratif', 'statistique', 'cartographique', 'temporel'
            ]
            
            formats = ['csv', 'json', 'xlsx', 'pdf', 'xml', 'geojson', 'shp', 'kml', 'ods', 'txt']
            licenses = ['Licence Ouverte v2.0', 'ODbL', 'Etalab', 'CC-BY', 'CC-BY-SA', 'Autre']
            
            massive_datasets = []
            
            # Ajouter les datasets de base
            massive_datasets.extend(base_datasets)
            
            # Générer 500+ datasets supplémentaires
            for i in range(4, 503):  # Commence à 4 pour éviter les conflits d'ID
                org = np.random.choice(organizations)
                theme = np.random.choice(themes)
                format_type = np.random.choice(formats, size=np.random.randint(1, 4))
                license_type = np.random.choice(licenses)
                
                # Générer un titre réaliste
                title_templates = [
                    f'Données {theme}s',
                    f'Statistiques {theme}s',
                    f'Base {theme}',
                    f'Répertoire {theme}',
                    f'Indicateurs {theme}s',
                    f'Cartographie {theme}',
                    f'Analyse {theme}',
                    f'Inventaire {theme}',
                    f'Recensement {theme}',
                    f'Observatoire {theme}'
                ]
                
                title = np.random.choice(title_templates)
                
                # Ajouter une spécificité
                specifics = ['national', 'régional', 'départemental', 'communal', 'annuel', 'mensuel', 'trimestriel']
                if np.random.random() > 0.5:
                    title += f" {np.random.choice(specifics)}"
                
                # Ajouter une année
                year = np.random.choice([2021, 2022, 2023, 2024])
                title += f" {year}"
                
                # Générer une description
                descriptions = [
                    f'Données {theme}s complètes et détaillées',
                    f'Statistiques officielles {theme}s',
                    f'Base de données {theme}s de référence',
                    f'Collection {theme} à grande échelle',
                    f'Jeux de données {theme}s exhaustif',
                    f'Recensement {theme} national',
                    f'Observatoire {theme} permanent',
                    f'Inventaire {theme} systématique'
                ]
                
                description = np.random.choice(descriptions)
                
                # Générer des métriques réalistes
                base_reuses = np.random.randint(50, 5000)
                base_views = base_reuses * np.random.randint(5, 25)
                base_followers = np.random.randint(10, 500)
                
                # Bonus pour les ministères
                if self.is_ministry(org):
                    base_reuses = int(base_reuses * 1.5)
                    base_views = int(base_views * 1.3)
                    base_followers = int(base_followers * 1.4)
                
                # Créer les ressources
                resources = []
                for fmt in format_type:
                    resources.append({'format': fmt, 'url': f'#resource_{fmt}_{i}'})
                
                # Générer une date aléatoire
                days_ago = np.random.randint(1, 730)
                created_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
                modified_date = (datetime.now() - timedelta(days=np.random.randint(1, 30))).strftime('%Y-%m-%d')
                
                dataset = {
                    'id': f'demo{i}',
                    'title': title,
                    'description': description,
                    'organization': {'name': org, 'id': org.lower().replace(' ', '_').replace('\'', '')},
                    'license': license_type,
                    'metrics': {
                        'reuses': base_reuses,
                        'views': base_views,
                        'followers': base_followers
                    },
                    'resources': resources,
                    'page': 'https://www.data.gouv.fr',
                    'created_at': created_date,
                    'last_modified': modified_date
                }
                
                massive_datasets.append(dataset)
            
            if self.debug_mode:
                st.session_state.debug_info.append(f"Généré {len(massive_datasets)} datasets de démonstration")
            
            return massive_datasets
            
        except Exception as e:
            self.log_error(f"Erreur création données massives: {str(e)}")
            return []

    def fetch_data_safe(self, endpoint, params=None, timeout=15):
        """Méthode sécurisée pour appeler l'API avec timeout augmenté"""
        try:
            if self.debug_mode:
                st.session_state.debug_info.append(f"Tentative de connexion à {endpoint}")
            
            url = f"{self.base_url}/{endpoint}/"
            headers = {
                'User-Agent': 'OpenData-RealTime-Engine/1.0',
                'Accept': 'application/json'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            
            if response.status_code == 200:
                data = response.json()
                if self.debug_mode:
                    st.session_state.debug_info.append(f"Succès: {len(data.get('data', []))} éléments récupérés")
                return data.get('data', [])
            else:
                self.log_error(f"Erreur API {response.status_code}: {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            self.log_error("Timeout de l'API")
            return None
        except requests.exceptions.ConnectionError:
            self.log_error("Erreur de connexion à l'API")
            return None
        except Exception as e:
            self.log_error(f"Erreur inattendue API: {str(e)}")
            return None

    def fetch_datasets_paginated(self, max_pages=10, page_size=100):
        """Récupère les datasets par pagination pour éviter les timeouts"""
        try:
            all_datasets = []
            
            for page in range(1, max_pages + 1):
                if self.debug_mode:
                    st.session_state.debug_info.append(f"Chargement page {page}/{max_pages}")
                
                # CORRECTION : Utiliser '-modified' au lieu de '-last_modified'
                data = self.fetch_data_safe('datasets', {
                    'page': page,
                    'page_size': page_size,
                    'sort': '-modified'  # CORRECTION ICI
                })
                
                if data is None:
                    if self.debug_mode:
                        st.session_state.debug_info.append(f"Arrêt à la page {page} - erreur API")
                    break
                
                if not data:
                    if self.debug_mode:
                        st.session_state.debug_info.append(f"Arrêt à la page {page} - plus de données")
                    break
                
                # Filtrer les datasets valides
                valid_datasets = []
                for dataset in data:
                    if dataset is not None and isinstance(dataset, dict) and 'id' in dataset:
                        valid_datasets.append(dataset)
                
                all_datasets.extend(valid_datasets)
                
                # Mettre à jour la progression
                progress = (page / max_pages) * 100
                st.session_state.loading_progress = progress
                
                # Petite pause pour ne pas surcharger l'API
                time.sleep(0.1)
                
                # Si on a assez de données, on peut s'arrêter
                if len(all_datasets) >= 1000:  # Limite à 1000 datasets
                    if self.debug_mode:
                        st.session_state.debug_info.append(f"Limite de 1000 datasets atteinte")
                    break
            
            return all_datasets
            
        except Exception as e:
            self.log_error(f"Erreur pagination: {str(e)}")
            return []

    @st.cache_data(ttl=300)  # Cache de 5 minutes pour le temps réel
    def fetch_datasets_cached(_self, max_pages=10, page_size=100):
        """Récupération des datasets avec cache court pour le temps réel"""
        try:
            # Tenter de récupérer depuis l'API avec pagination
            datasets = _self.fetch_datasets_paginated(max_pages, page_size)
            
            if datasets:
                # Trier avec priorité aux ministères
                sorted_datasets = _self.sort_datasets_by_priority(datasets)
                
                if _self.debug_mode:
                    st.session_state.debug_info.append(f"Récupéré {len(sorted_datasets)} datasets depuis l'API")
                
                return sorted_datasets
            else:
                if _self.debug_mode:
                    st.session_state.debug_info.append("Utilisation des données de démonstration massives")
                return _self.get_massive_demo_data()
                
        except Exception as e:
            _self.log_error(f"Erreur fetch_datasets: {str(e)}")
            return _self.get_massive_demo_data()

    def sort_datasets_by_priority(self, datasets):
        """Trie les datasets en donnant la priorité aux ministères et aux plus récents"""
        try:
            def get_priority_score(dataset):
                org_name = self.safe_get(dataset, 'organization.name', '')
                reuses = self.safe_get(dataset, 'metrics.reuses', 0)
                last_modified = self.safe_get(dataset, 'last_modified', '')
                
                # Score de base pour les réutilisations
                score = int(reuses) if reuses else 0
                
                # Bonus si c'est un ministère
                if self.is_ministry(org_name):
                    ministry_score = self.get_ministry_priority_score(org_name)
                    score += ministry_score * 100  # Multiplicateur pour donner la priorité
                
                # Bonus pour les datasets récemment modifiés
                if last_modified:
                    try:
                        modified_date = datetime.strptime(last_modified[:10], '%Y-%m-%d')
                        days_ago = (datetime.now() - modified_date).days
                        if days_ago < 30:  # Moins de 30 jours
                            score += (30 - days_ago) * 10  # Bonus décroissant
                    except:
                        pass
                
                return score
            
            return sorted(datasets, key=get_priority_score, reverse=True)
        except Exception as e:
            self.log_error(f"Erreur tri par priorité: {str(e)}")
            return datasets

    @st.cache_data(ttl=600)  # Cache de 10 minutes pour les organisations
    def fetch_organizations_cached(_self):
        """Récupération des organisations avec cache"""
        try:
            data = _self.fetch_data_safe('organizations', {'page_size': 100})
            if data is not None:
                # Filtrer les organisations None ou invalides
                valid_orgs = []
                for org in data:
                    if org is not None and isinstance(org, dict) and 'id' in org:
                        valid_orgs.append(org)
                return valid_orgs
            return []
        except Exception as e:
            _self.log_error(f"Erreur fetch_organizations: {str(e)}")
            return []

    @st.cache_data(ttl=600)  # Cache de 10 minutes pour les réutilisations
    def fetch_reuses_cached(_self):
        """Récupération des réutilisations avec cache"""
        try:
            data = _self.fetch_data_safe('reuses', {'page_size': 50})
            if data is not None:
                # Filtrer les réutilisations None ou invalides
                valid_reuses = []
                for reuse in data:
                    if reuse is not None and isinstance(reuse, dict) and 'id' in reuse:
                        valid_reuses.append(reuse)
                return valid_reuses
            return []
        except Exception as e:
            _self.log_error(f"Erreur fetch_reuses: {str(e)}")
            return []

    @st.cache_data(ttl=180)  # Cache de 3 minutes pour les recherches
    def search_datasets_cached(_self, query, page_size=50):
        """Recherche avec cache court et priorité aux ministères"""
        try:
            if not query or len(query.strip()) < 2:
                return []
            
            data = _self.fetch_data_safe('datasets', {
                'q': query.strip(),
                'page_size': page_size,
                'sort': '-reuses'
            })
            
            if data is not None:
                # Filtrer les résultats de recherche
                valid_results = []
                for dataset in data:
                    if dataset is not None and isinstance(dataset, dict) and 'id' in dataset:
                        valid_results.append(dataset)
                
                # Trier avec priorité aux ministères
                sorted_results = _self.sort_datasets_by_priority(valid_results)
                
                return sorted_results
            return []
        except Exception as e:
            _self.log_error(f"Erreur recherche: {str(e)}")
            return []

    def check_for_new_datasets(self):
        """Vérifie s'il y a de nouveaux datasets depuis la dernière mise à jour"""
        try:
            if not st.session_state.datasets_loaded:
                return 0
            
            # Récupérer les 10 datasets les plus récents
            recent_data = self.fetch_data_safe('datasets', {
                'page_size': 10,
                'sort': '-modified'  # CORRECTION ICI
            })
            
            if not recent_data:
                return 0
            
            # Comparer avec les datasets existants
            existing_ids = {d.get('id') for d in st.session_state.datasets}
            new_count = 0
            
            for dataset in recent_data:
                if dataset and dataset.get('id') and dataset['id'] not in existing_ids:
                    new_count += 1
                    self.log_update(f"Nouveau dataset détecté: {dataset.get('title', 'Sans titre')}")
            
            return new_count
            
        except Exception as e:
            self.log_error(f"Erreur vérification nouveaux datasets: {str(e)}")
            return 0

    def load_data(self, force_refresh=False):
        """Chargement des données avec barre de progression et support temps réel"""
        try:
            if force_refresh or not st.session_state.datasets_loaded:
                # Créer une barre de progression
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.text("🔄 Chargement massif des datasets...")
                
                # Charger les données
                datasets = self.fetch_datasets_cached(max_pages=10, page_size=100)
                
                # Mettre à jour la progression
                progress_bar.progress(100)
                status_text.text("✅ Chargement terminé!")
                
                # Vérifier les nouveaux datasets
                if st.session_state.datasets_loaded:
                    new_count = self.check_for_new_datasets()
                    if new_count > 0:
                        st.session_state.new_datasets_count = new_count
                        self.log_update(f"{new_count} nouveaux datasets détectés!")
                
                # Stocker les données
                st.session_state.datasets = datasets
                st.session_state.organizations = self.fetch_organizations_cached()
                st.session_state.reuses = self.fetch_reuses_cached()
                st.session_state.datasets_loaded = True
                st.session_state.last_refresh = datetime.now()
                
                # Nettoyer l'interface
                time.sleep(0.5)
                progress_bar.empty()
                status_text.empty()
                
                if self.debug_mode:
                    st.session_state.debug_info.append(
                        f"Données chargées: {len(st.session_state.datasets)} datasets, "
                        f"{len(st.session_state.organizations)} organisations, "
                        f"{len(st.session_state.reuses)} réutilisations"
                    )
                    
                    # Afficher un message de succès
                    st.success(f"🎉 {len(st.session_state.datasets)} datasets chargés avec succès!")
                    
        except Exception as e:
            self.log_error(f"Erreur load_data: {str(e)}")
            st.session_state.datasets = self.get_massive_demo_data()
            st.session_state.datasets_loaded = True

    def get_dataset_stats(self):
        """Calcule les statistiques globales avec cache"""
        if st.session_state.stats_cache:
            return st.session_state.stats_cache
            
        if not st.session_state.datasets:
            return {}
        
        try:
            # Limiter l'analyse pour la performance
            analysis_limit = min(1000, len(st.session_state.datasets))
            datasets_to_analyze = st.session_state.datasets[:analysis_limit]
            
            total_datasets = len(st.session_state.datasets)
            total_organizations = len(st.session_state.organizations)
            
            # Calcul sécurisé des métriques
            total_reuses = 0
            total_views = 0
            
            ministry_datasets = 0
            ministry_reuses = 0
            ministry_views = 0
            
            recent_datasets = 0  # Datasets modifiés dans les 30 derniers jours
            
            for dataset in datasets_to_analyze:
                try:
                    metrics = dataset.get('metrics', {})
                    if metrics and isinstance(metrics, dict):
                        reuses = int(metrics.get('reuses', 0))
                        views = int(metrics.get('views', 0))
                        
                        total_reuses += reuses
                        total_views += views
                        
                        # Statistiques spécifiques aux ministères
                        org_name = self.safe_get(dataset, 'organization.name', '')
                        if self.is_ministry(org_name):
                            ministry_datasets += 1
                            ministry_reuses += reuses
                            ministry_views += views
                    
                    # Vérifier si le dataset est récent
                    last_modified = self.safe_get(dataset, 'last_modified', '')
                    if last_modified:
                        try:
                            modified_date = datetime.strptime(last_modified[:10], '%Y-%m-%d')
                            if (datetime.now() - modified_date).days <= 30:
                                recent_datasets += 1
                        except:
                            pass
                            
                except (ValueError, TypeError):
                    continue
            
            # Formats les plus courants
            all_formats = []
            for dataset in datasets_to_analyze:
                try:
                    resources = dataset.get('resources', [])
                    if resources and isinstance(resources, list):
                        for resource in resources:
                            if resource and isinstance(resource, dict):
                                fmt = resource.get('format')
                                if fmt:
                                    all_formats.append(str(fmt).upper())
                except:
                    continue
            
            format_counts = Counter(all_formats)
            top_formats = format_counts.most_common(5)
            
            # Organisations les plus actives
            org_activity = {}
            for dataset in datasets_to_analyze:
                try:
                    org = dataset.get('organization', {})
                    if org and isinstance(org, dict):
                        org_name = org.get('name', 'Autre')
                        if org_name:
                            org_activity[org_name] = org_activity.get(org_name, 0) + 1
                except:
                    continue
            
            top_organizations = sorted(org_activity.items(), key=lambda x: x[1], reverse=True)[:5]
            
            # Datasets les plus populaires
            popular_datasets = []
            for dataset in datasets_to_analyze:
                try:
                    metrics = dataset.get('metrics', {})
                    if metrics and isinstance(metrics, dict):
                        reuses = int(metrics.get('reuses', 0))
                        dataset['_reuses_count'] = reuses
                        popular_datasets.append(dataset)
                except:
                    dataset['_reuses_count'] = 0
                    popular_datasets.append(dataset)
            
            popular_datasets.sort(key=lambda x: x.get('_reuses_count', 0), reverse=True)
            popular_datasets = popular_datasets[:10]
            
            # Répartition par licence
            license_counts = {}
            for dataset in datasets_to_analyze:
                try:
                    license = dataset.get('license', 'Non spécifiée')
                    if license:
                        license_counts[license] = license_counts.get(license, 0) + 1
                except:
                    license_counts['Non spécifiée'] = license_counts.get('Non spécifiée', 0) + 1
            
            stats = {
                'total_datasets': total_datasets,
                'total_organizations': total_organizations,
                'total_reuses': total_reuses,
                'total_views': total_views,
                'ministry_datasets': ministry_datasets,
                'ministry_reuses': ministry_reuses,
                'ministry_views': ministry_views,
                'recent_datasets': recent_datasets,
                'top_formats': top_formats,
                'top_organizations': top_organizations,
                'popular_datasets': popular_datasets,
                'license_counts': license_counts
            }
            
            st.session_state.stats_cache = stats
            return stats
            
        except Exception as e:
            self.log_error(f"Erreur calcul stats: {str(e)}")
            return {}

    def extract_organizations_from_datasets(self, datasets):
        """Extrait les organisations uniques à partir des datasets affichés"""
        try:
            if not datasets:
                return []
            
            org_dict = {}
            
            for dataset in datasets:
                if dataset is None or not isinstance(dataset, dict):
                    continue
                    
                org = dataset.get('organization', {})
                if org and isinstance(org, dict) and org.get('id'):
                    org_id = org.get('id')
                    org_name = org.get('name', 'Non spécifié')
                    
                    # Compter les datasets pour cette organisation
                    if org_id not in org_dict:
                        org_dict[org_id] = {
                            'id': org_id,
                            'name': org_name,
                            'description': org.get('description', ''),
                            'page': org.get('page', ''),
                            'datasets_count': 0,
                            'total_reuses': 0,
                            'total_views': 0,
                            'datasets': [],
                            'is_ministry': self.is_ministry(org_name)
                        }
                    
                    # Ajouter les métriques du dataset
                    metrics = dataset.get('metrics', {})
                    if metrics and isinstance(metrics, dict):
                        try:
                            org_dict[org_id]['total_reuses'] += int(metrics.get('reuses', 0))
                            org_dict[org_id]['total_views'] += int(metrics.get('views', 0))
                        except (ValueError, TypeError):
                            pass
                    
                    org_dict[org_id]['datasets_count'] += 1
                    org_dict[org_id]['datasets'].append({
                        'title': dataset.get('title', 'Sans titre'),
                        'id': dataset.get('id', ''),
                        'reuses': metrics.get('reuses', 0) if metrics else 0
                    })
            
            # Trier : d'abord les ministères, puis par nombre de datasets
            sorted_orgs = sorted(org_dict.values(), 
                                key=lambda x: (1000 if x['is_ministry'] else 0, x['datasets_count']), 
                                reverse=True)
            
            if self.debug_mode:
                st.session_state.debug_info.append(f"Extraction: {len(sorted_orgs)} organisations uniques trouvées")
            
            return sorted_orgs
            
        except Exception as e:
            self.log_error(f"Erreur extraction organisations: {str(e)}")
            return []

    def display_realtime_status(self):
        """Affiche le statut en temps réel du dashboard"""
        try:
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col1:
                st.markdown('<div class="realtime-indicator"></div>', unsafe_allow_html=True)
                st.markdown("**TEMPS RÉEL**")
            
            with col2:
                if st.session_state.last_refresh:
                    last_refresh = st.session_state.last_refresh.strftime('%H:%M:%S')
                    time_ago = (datetime.now() - st.session_state.last_refresh).seconds
                    
                    if time_ago < 60:
                        refresh_text = f"Il y a {time_ago} secondes"
                    else:
                        minutes = time_ago // 60
                        refresh_text = f"Il y a {minutes} minute(s)"
                    
                    st.markdown(f"**Dernière mise à jour:** {refresh_text}")
                else:
                    st.markdown("**En attente de mise à jour...**")
            
            with col3:
                if st.session_state.auto_refresh:
                    st.markdown(f"**Auto-rafraîchissement:** {self.refresh_interval}s")
                else:
                    st.markdown("**Auto-rafraîchissement:** Désactivé")
            
            # Afficher les notifications de mises à jour
            if st.session_state.new_datasets_count > 0:
                st.markdown(
                    f'<div class="update-notification">🆕 {st.session_state.new_datasets_count} nouveaux datasets détectés!</div>',
                    unsafe_allow_html=True
                )
            
            # Afficher le compteur de mises à jour
            if st.session_state.update_count > 0:
                st.markdown(f"**Mises à jour:** {st.session_state.update_count}")
            
        except Exception as e:
            self.log_error(f"Erreur affichage statut temps réel: {str(e)}")

    def display_debug_info(self):
        """Affiche les informations de débogage"""
        if self.debug_mode and st.session_state.debug_info:
            with st.expander("🔍 Informations de débogage"):
                for info in st.session_state.debug_info[-15:]:  # Dernières 15 entrées
                    st.text(info)
                
                if st.session_state.last_error:
                    st.markdown(f"**Dernière erreur:** {st.session_state.last_error}")
                
                if st.button("🗑️ Vider le log"):
                    st.session_state.debug_info = []
                    st.rerun()

    def display_header(self):
        """Affiche l'en-tête du dashboard"""
        try:
            st.markdown('<h1 class="main-header">🔍 DASHBOARD OPEN DATA - MOTEUR DE RECHERCHES</h1>', 
                       unsafe_allow_html=True)
            
            # Afficher le statut en temps réel
            self.display_realtime_status()
            
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.markdown("**Explorez des milliers de datasets en temps réel avec priorité aux ministères**")
                st.markdown("---")
            
            # Afficher le statut
            if st.session_state.error_count > 0:
                st.markdown(f'<div class="error-box">⚠️ {st.session_state.error_count} erreur(s) détectée(s)</div>', unsafe_allow_html=True)
            elif st.session_state.datasets:
                st.markdown('<div class="success-box">✅ Dashboard temps réel opérationnel</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="info-box">ℹ️ Chargement en cours...</div>', unsafe_allow_html=True)
                
        except Exception as e:
            self.log_error(f"Erreur affichage header: {str(e)}")
            st.error("Erreur d'affichage de l'en-tête")

    def display_key_metrics(self, stats):
        """Affiche les métriques clés avec focus sur les ministères et le temps réel"""
        try:
            st.markdown('<h3 class="section-header">📊 MÉTRIQUES OPEN DATA EN TEMPS RÉEL</h3>', 
                       unsafe_allow_html=True)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "📦 Datasets",
                    f"{stats.get('total_datasets', 0):,}",
                    f"🏛️ {stats.get('ministry_datasets', 0)} ministères"
                )
            
            with col2:
                st.metric(
                    "🏢 Organisations",
                    f"{stats.get('total_organizations', 0):,}",
                    "+ Actifs"
                )
            
            with col3:
                st.metric(
                    "🔄 Réutilisations",
                    f"{stats.get('total_reuses', 0):,}",
                    f"🏛️ {stats.get('ministry_reuses', 0):,} ministères"
                )
            
            with col4:
                st.metric(
                    "👀 Vues totales",
                    f"{stats.get('total_views', 0):,}",
                    f"🏛️ {stats.get('ministry_views', 0):,} ministères"
                )
            
            # Barre de progression pour les ministères
            ministry_ratio = (stats.get('ministry_datasets', 0) / max(stats.get('total_datasets', 1), 1)) * 100
            st.markdown(f"""
            <div style="margin-top: 1rem;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                    <span style="font-weight: bold;">🏛️ Priorité aux ministères</span>
                    <span style="font-size: 0.9rem; color: #666;">{ministry_ratio:.1f}% des datasets</span>
                </div>
                <div style="background-color: #e9ecef; border-radius: 10px; height: 8px; overflow: hidden;">
                    <div style="background: linear-gradient(90deg, #002395, #0055A4); height: 100%; width: {ministry_ratio}%; transition: width 0.3s ease;"></div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Métriques temps réel
            recent_count = stats.get('recent_datasets', 0)
            if recent_count > 0:
                st.markdown(f"""
                <div style="margin-top: 1rem; padding: 0.5rem; background-color: #e8f5e9; border-radius: 5px;">
                    <span style="font-weight: bold; color: #2e7d32;">🆕 {recent_count} datasets mis à jour dans les 30 derniers jours</span>
                </div>
                """, unsafe_allow_html=True)
            
        except Exception as e:
            self.log_error(f"Erreur affichage métriques: {str(e)}")
            st.error("Erreur d'affichage des métriques")

    def create_search_interface(self):
        """Crée l'interface de recherche avancée avec pagination"""
        try:
            st.markdown('<h3 class="section-header">🔍 MOTEUR DE RECHERCHE AVANCÉ</h3>', 
                       unsafe_allow_html=True)
            
            col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
            
            with col1:
                search_query = st.text_input(
                    "Rechercher un jeu de données...",
                    value=st.session_state.current_query,
                    placeholder="Ex: budget, éducation, santé, environnement...",
                    key="search_input_main"
                )
            
            with col2:
                sort_options = {
                    "priority": "🏛️ Priorité Ministères",
                    "reuses": "Popularité",
                    "created": "Récent", 
                    "title": "A-Z",
                    "views": "Vues"
                }
                sort_by = st.selectbox(
                    "Trier par:", 
                    options=list(sort_options.keys()), 
                    format_func=lambda x: sort_options[x],
                    key="sort_select"
                )
            
            with col3:
                format_filter = st.selectbox(
                    "Format:",
                    ["Tous", "CSV", "JSON", "XLSX", "PDF", "XML"],
                    key="format_select"
                )
            
            with col4:
                items_per_page = st.selectbox(
                    "Résultats:", 
                    [20, 50, 100, 200], 
                    index=1,
                    key="page_size_select"
                )
            
            # Options de filtrage par ministère
            st.markdown("**🏛️ Filtres prioritaires:**")
            col_filter1, col_filter2, col_filter3 = st.columns(3)
            
            with col_filter1:
                show_ministries_only = st.checkbox(
                    "Afficher uniquement les ministères",
                    key="ministries_only"
                )
            
            with col_filter2:
                ministry_types = st.multiselect(
                    "Types de ministères:",
                    ["Économie/Finances", "Éducation", "Santé", "Écologie", "Intérieur", "Justice", "Autres"],
                    default=[],
                    key="ministry_types"
                )
            
            with col_filter3:
                priority_level = st.selectbox(
                    "Niveau de priorité:",
                    ["Tous", "Élevé", "Moyen", "Standard"],
                    key="priority_level"
                )
            
            # Bouton de recherche
            search_clicked = st.button("🔎 Lancer la recherche", use_container_width=True, key="search_button")
            
            # Gestion de la recherche
            if search_clicked and search_query:
                st.session_state.current_query = search_query
                st.session_state.current_view = "search"
                
                with st.spinner(f"🔎 Recherche de '{search_query}'..."):
                    results = self.search_datasets_cached(search_query, items_per_page)
                    
                    # Appliquer les filtres
                    if show_ministries_only:
                        results = [d for d in results if self.is_ministry(self.safe_get(d, 'organization.name', ''))]
                    
                    if ministry_types:
                        filtered_results = []
                        for dataset in results:
                            org_name = self.safe_get(dataset, 'organization.name', '').lower()
                            if any(mtype.lower() in org_name for mtype in ministry_types):
                                filtered_results.append(dataset)
                        results = filtered_results
                    
                    st.session_state.search_results = results
                    
                    if results:
                        ministry_count = len([d for d in results if self.is_ministry(self.safe_get(d, 'organization.name', ''))])
                        st.success(f"✅ {len(results)} résultat(s) trouvé(s) ({ministry_count} ministères)")
                        self.log_update(f"Recherche '{search_query}': {len(results)} résultats")
                    else:
                        st.info("❌ Aucun résultat trouvé")
            
            # Déterminer quelles données afficher
            if st.session_state.current_view == "search" and st.session_state.current_query:
                return st.session_state.search_results, f"Résultats pour '{st.session_state.current_query}'"
            else:
                # Retourner les datasets populaires (déjà triés par priorité)
                popular_datasets = st.session_state.datasets[:items_per_page]
                return popular_datasets, f"Jeux de données populaires ({len(st.session_state.datasets)} disponibles)"
                
        except Exception as e:
            self.log_error(f"Erreur interface recherche: {str(e)}")
            return self.get_massive_demo_data(), "Erreur - Données de démonstration"

    def get_dataset_formats(self, dataset):
        """Récupère les formats de manière sécurisée"""
        try:
            if dataset is None or not isinstance(dataset, dict):
                return []
                
            formats = []
            resources = dataset.get('resources', [])
            
            if not isinstance(resources, list):
                return []
                
            for resource in resources:
                if resource and isinstance(resource, dict):
                    fmt = resource.get('format')
                    if fmt:
                        formats.append(str(fmt).upper())
            
            return list(set(formats))[:3]
        except Exception as e:
            self.log_error(f"Erreur get formats: {str(e)}")
            return []

    def display_datasets(self, datasets, title):
        """Affiche la liste des jeux de données avec pagination et temps réel"""
        try:
            st.markdown(f'<h3 class="section-header">📁 {title.upper()}</h3>', 
                       unsafe_allow_html=True)
            
            if not datasets:
                st.info("Aucun jeu de données trouvé.")
                return
            
            # Filtrer les datasets valides
            valid_datasets = []
            for dataset in datasets:
                if dataset is not None and isinstance(dataset, dict) and 'id' in dataset:
                    valid_datasets.append(dataset)
            
            # Pagination
            items_per_page = 50
            total_items = len(valid_datasets)
            total_pages = (total_items + items_per_page - 1) // items_per_page
            
            if total_pages > 1:
                # Sélecteur de page
                col_page1, col_page2, col_page3 = st.columns([1, 2, 1])
                with col_page2:
                    page = st.selectbox(
                        "Page:",
                        range(1, total_pages + 1),
                        index=0,
                        key="page_selector"
                    )
                
                start_idx = (page - 1) * items_per_page
                end_idx = min(start_idx + items_per_page, total_items)
                datasets_to_show = valid_datasets[start_idx:end_idx]
                
                st.info(f"Affichage {start_idx + 1}-{end_idx} sur {total_items} datasets")
            else:
                datasets_to_show = valid_datasets
            
            # Afficher les datasets
            for dataset in datasets_to_show:
                with st.container():
                    org_name = self.safe_get(dataset, 'organization.name', '')
                    is_ministry = self.is_ministry(org_name)
                    
                    # Vérifier si le dataset est récent
                    last_modified = self.safe_get(dataset, 'last_modified', '')
                    is_recent = False
                    if last_modified:
                        try:
                            modified_date = datetime.strptime(last_modified[:10], '%Y-%m-%d')
                            if (datetime.now() - modified_date).days <= 7:
                                is_recent = True
                        except:
                            pass
                    
                    # Carte spéciale pour les ministères ou datasets récents
                    if is_ministry or is_recent:
                        st.markdown('<div class="ministry-card">', unsafe_allow_html=True)
                    else:
                        st.markdown('<div class="dataset-card">', unsafe_allow_html=True)
                    
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        # Titre et badges
                        title = self.safe_get(dataset, 'title')
                        page_url = self.safe_get(dataset, 'page', '#')
                        
                        col_title1, col_title2 = st.columns([4, 1])
                        with col_title1:
                            if page_url != "#":
                                st.markdown(f"### [{title}]({page_url})")
                            else:
                                st.markdown(f"### {title}")
                        with col_title2:
                            if is_ministry:
                                st.markdown('<span class="priority-badge">🏛️ Priorité</span>', 
                                           unsafe_allow_html=True)
                            elif is_recent:
                                st.markdown('<span class="realtime-badge">🆕 Récent</span>', 
                                           unsafe_allow_html=True)
                        
                        description = self.safe_get(dataset, 'description')
                        if len(description) > 200:
                            description = description[:200] + "..."
                        st.markdown(f"*{description}*")
                        
                        # Métadonnées
                        col_meta1, col_meta2, col_meta3 = st.columns(3)
                        
                        with col_meta1:
                            if is_ministry:
                                st.markdown(f"**🏛️ Ministre:** {org_name}")
                            else:
                                st.markdown(f"**Organisation:** {org_name}")
                        
                        with col_meta2:
                            license = self.safe_get(dataset, 'license')
                            st.markdown(f"**📜 Licence:** {license}")
                        
                        with col_meta3:
                            if last_modified:
                                st.markdown(f"**Modifié:** {last_modified[:10]}")
                                if is_recent:
                                    st.markdown('<span class="last-update">🆕 Mis à jour récemment</span>', unsafe_allow_html=True)
                    
                    with col2:
                        # Métriques
                        metrics = dataset.get('metrics', {})
                        reuses = 0
                        views = 0
                        
                        if metrics and isinstance(metrics, dict):
                            try:
                                reuses = int(metrics.get('reuses', 0))
                                views = int(metrics.get('views', 0))
                            except (ValueError, TypeError):
                                pass
                        
                        st.metric("🔄 Réutilisations", reuses)
                        st.metric("👀 Vues", views)
                        
                        # Badges
                        if reuses > 100:
                            st.markdown('<span class="badge popular-badge">Populaire</span>', 
                                       unsafe_allow_html=True)
                        
                        if is_ministry:
                            st.markdown('<span class="ministry-badge">🏛️ Ministère</span>', 
                                       unsafe_allow_html=True)
                        
                        # Formats
                        formats = self.get_dataset_formats(dataset)
                        if formats:
                            for fmt in formats:
                                st.markdown(f'<span class="badge format-badge">{fmt}</span>', 
                                           unsafe_allow_html=True)
                    
                    if is_ministry or is_recent:
                        st.markdown('</div>', unsafe_allow_html=True)
                    else:
                        st.markdown('</div>', unsafe_allow_html=True)
                    st.markdown("---")
                    
        except Exception as e:
            self.log_error(f"Erreur affichage datasets: {str(e)}")
            st.error("Erreur d'affichage des datasets")

    def create_visualizations(self, stats):
        """Crée les visualisations des données avec focus sur les ministères et le temps réel"""
        try:
            st.markdown('<h3 class="section-header">📈 ANALYSE ET VISUALISATIONS EN TEMPS RÉEL</h3>', 
                       unsafe_allow_html=True)
            
            tab1, tab2, tab3, tab4 = st.tabs(["Formats", "Organisations", "Ministères", "Tendances"])
            
            with tab1:
                # Graphique des formats
                if stats.get('top_formats'):
                    formats_df = pd.DataFrame(stats['top_formats'], columns=['Format', 'Count'])
                    fig = px.bar(formats_df, x='Count', y='Format', 
                                title='Formats de fichiers les plus courants',
                                color='Count',
                                color_continuous_scale='Blues')
                    st.plotly_chart(fig, use_container_width=True)
            
            with tab2:
                # Graphique des organisations
                if stats.get('top_organizations'):
                    orgs_df = pd.DataFrame(stats['top_organizations'], columns=['Organisation', 'Count'])
                    fig = px.bar(orgs_df, x='Count', y='Organisation',
                                title='Organisations les plus actives',
                                color='Count',
                                color_continuous_scale='Reds')
                    st.plotly_chart(fig, use_container_width=True)
            
            with tab3:
                # Visualisation spécifique aux ministères
                ministry_data = []
                non_ministry_data = []
                
                # Limiter pour la performance
                analysis_limit = min(500, len(st.session_state.datasets))
                datasets_to_analyze = st.session_state.datasets[:analysis_limit]
                
                for dataset in datasets_to_analyze:
                    org_name = self.safe_get(dataset, 'organization.name', '')
                    metrics = dataset.get('metrics', {})
                    reuses = int(metrics.get('reuses', 0)) if metrics else 0
                    
                    if self.is_ministry(org_name):
                        ministry_data.append({'Organisation': org_name, 'Réutilisations': reuses})
                    else:
                        non_ministry_data.append({'Organisation': org_name, 'Réutilisations': reuses})
                
                # Graphique comparatif
                col1, col2 = st.columns(2)
                
                with col1:
                    if ministry_data:
                        ministry_df = pd.DataFrame(ministry_data)
                        ministry_agg = ministry_df.groupby('Organisation')['Réutilisations'].sum().reset_index()
                        ministry_agg = ministry_agg.sort_values('Réutilisations', ascending=True).tail(10)
                        
                        fig = px.bar(ministry_agg, x='Réutilisations', y='Organisation',
                                    title='🏛️ Top 10 des ministères par réutilisations',
                                    orientation='h',
                                    color='Réutilisations',
                                    color_continuous_scale='Blues')
                        st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    if non_ministry_data:
                        non_ministry_df = pd.DataFrame(non_ministry_data)
                        non_ministry_agg = non_ministry_df.groupby('Organisation')['Réutilisations'].sum().reset_index()
                        non_ministry_agg = non_ministry_agg.sort_values('Réutilisations', ascending=True).tail(10)
                        
                        fig = px.bar(non_ministry_agg, x='Réutilisations', y='Organisation',
                                    title='📊 Top 10 des autres organisations',
                                    orientation='h',
                                    color='Réutilisations',
                                    color_continuous_scale='Greens')
                        st.plotly_chart(fig, use_container_width=True)
                
                # Statistiques comparatives
                st.markdown("### 📊 Statistiques comparatives")
                col_stat1, col_stat2, col_stat3 = st.columns(3)
                
                with col_stat1:
                    st.metric(
                        "🏛️ Datasets ministériels",
                        f"{stats.get('ministry_datasets', 0)}",
                        f"{(stats.get('ministry_datasets', 0) / max(stats.get('total_datasets', 1), 1) * 100):.1f}%"
                    )
                
                with col_stat2:
                    st.metric(
                        "🔄 Réutilisations ministères",
                        f"{stats.get('ministry_reuses', 0):,}",
                        f"{(stats.get('ministry_reuses', 0) / max(stats.get('total_reuses', 1), 1) * 100):.1f}%"
                    )
                
                with col_stat3:
                    st.metric(
                        "👀 Vues ministères",
                        f"{stats.get('ministry_views', 0):,}",
                        f"{(stats.get('ministry_views', 0) / max(stats.get('total_views', 1), 1) * 100):.1f}%"
                    )
            
            with tab4:
                # Graphique d'évolution en temps réel (simulé mais avec données récentes)
                dates = pd.date_range('2023-01-01', datetime.now(), freq='M')
                evolution_data = []
                
                for date in dates:
                    evolution_data.append({
                        'date': date,
                        'nouveaux_datasets': np.random.randint(100, 500),
                        'nouvelles_reutilisations': np.random.randint(200, 1000),
                        'datasets_ministeriels': np.random.randint(50, 250),
                        'datasets_recents': np.random.randint(10, 100) if date.month >= 10 else np.random.randint(5, 50)
                    })
                
                evolution_df = pd.DataFrame(evolution_data)
                fig = px.line(evolution_df, x='date', y=['nouveaux_datasets', 'nouvelles_reutilisations', 'datasets_ministeriels', 'datasets_recents'],
                             title='Évolution mensuelle (estimation)',
                             labels={'value': 'Nombre', 'variable': 'Type'})
                st.plotly_chart(fig, use_container_width=True)
                
                # Afficher les datasets récents
                recent_datasets = [d for d in st.session_state.datasets[:50] 
                                   if self.is_recent_dataset(d)]
                
                if recent_datasets:
                    st.markdown("### 🆕 Datasets récemment mis à jour")
                    for dataset in recent_datasets[:5]:
                        title = self.safe_get(dataset, 'title')
                        org_name = self.safe_get(dataset, 'organization.name', '')
                        last_modified = self.safe_get(dataset, 'last_modified', '')
                        
                        st.markdown(f"**{title}** - {org_name}")
                        if last_modified:
                            st.markdown(f"*Dernière modification: {last_modified[:10]}*")
                        st.markdown("---")
        except Exception as e:
            self.log_error(f"Erreur visualisations: {str(e)}")
            st.error("Erreur lors de la création des visualisations")

    def is_recent_dataset(self, dataset):
        """Vérifie si un dataset a été récemment modifié"""
        try:
            last_modified = self.safe_get(dataset, 'last_modified', '')
            if last_modified:
                modified_date = datetime.strptime(last_modified[:10], '%Y-%m-%d')
                return (datetime.now() - modified_date).days <= 7
            return False
        except:
            return False

    def create_organizations_section(self, datasets=None):
        """Affiche la section des organisations avec focus sur les ministères"""
        try:
            st.markdown('<h3 class="section-header">🏛️ ORGANISATIONS PARTENAIRES</h3>', 
                       unsafe_allow_html=True)
            
            # Si aucun dataset n'est fourni, utiliser tous les datasets
            if datasets is None:
                datasets = st.session_state.datasets
            
            # Extraire les organisations des datasets actuels
            organizations = self.extract_organizations_from_datasets(datasets)
            
            if not organizations:
                st.info("Aucune organisation trouvée pour les datasets affichés.")
                return
            
            # Compter les ministères
            ministry_count = len([org for org in organizations if org['is_ministry']])
            
            # Afficher le nombre d'organisations trouvées
            st.markdown(f"<p class='org-stats'>📊 {len(organizations)} organisation(s) trouvée(s) pour {len(datasets)} jeu(x) de données ({ministry_count} ministères)</p>", 
                       unsafe_allow_html=True)
            
            # Options d'affichage
            col1, col2 = st.columns([2, 1])
            with col1:
                view_mode = st.selectbox(
                    "Mode d'affichage:",
                    ["Cartes détaillées", "Tableau compact", "Graphique"],
                    key="org_view_mode"
                )
            with col2:
                sort_by = st.selectbox(
                    "Trier par:",
                    ["🏛️ Priorité Ministères", "Nombre de datasets", "Réutilisations totales", "Vues totales", "Nom A-Z"],
                    key="org_sort_by"
                )
            
            # Trier les organisations
            if sort_by == "🏛️ Priorité Ministères":
                # Déjà trié par défaut (ministères en premier)
                pass
            elif sort_by == "Nombre de datasets":
                organizations.sort(key=lambda x: x['datasets_count'], reverse=True)
            elif sort_by == "Réutilisations totales":
                organizations.sort(key=lambda x: x['total_reuses'], reverse=True)
            elif sort_by == "Vues totales":
                organizations.sort(key=lambda x: x['total_views'], reverse=True)
            else:  # Nom A-Z
                organizations.sort(key=lambda x: x['name'].lower())
            
            if view_mode == "Cartes détaillées":
                # Affichage en cartes avec distinction ministères/non-ministères
                cols = st.columns(2)
                for idx, org in enumerate(organizations[:10]):  # Augmenté à 10 organisations
                    with cols[idx % 2]:
                        with st.container():
                            if org['is_ministry']:
                                st.markdown(f'<div class="ministry-card">', unsafe_allow_html=True)
                            else:
                                st.markdown(f'<div class="organization-card">', unsafe_allow_html=True)
                            
                            # En-tête avec nom et badge
                            col_header1, col_header2 = st.columns([3, 1])
                            with col_header1:
                                if org['is_ministry']:
                                    st.markdown(f"### 🏛️ {org['name']}")
                                else:
                                    st.markdown(f"### 🏢 {org['name']}")
                            with col_header2:
                                if org['is_ministry']:
                                    st.markdown('<span class="ministry-badge">Ministère</span>', 
                                               unsafe_allow_html=True)
                                elif org['datasets_count'] > 5:
                                    st.markdown('<span class="badge trending-badge">Actif</span>', 
                                               unsafe_allow_html=True)
                                elif org['datasets_count'] > 2:
                                    st.markdown('<span class="badge popular-badge">Contributeur</span>', 
                                               unsafe_allow_html=True)
                            
                            # Description
                            description = org.get('description', '')
                            if len(description) > 150:
                                description = description[:150] + "..."
                            if description:
                                st.markdown(f"*{description}*")
                            
                            # Métriques principales
                            col_metrics1, col_metrics2, col_metrics3 = st.columns(3)
                            with col_metrics1:
                                st.metric("📦 Datasets", org['datasets_count'])
                            with col_metrics2:
                                st.metric("🔄 Réutilisations", f"{org['total_reuses']:,}")
                            with col_metrics3:
                                st.metric("👀 Vues", f"{org['total_views']:,}")
                            
                            # Top datasets de cette organisation
                            if org['datasets']:
                                st.markdown("**📊 Datasets principaux:**")
                                top_datasets = sorted(org['datasets'], 
                                                   key=lambda x: x['reuses'], 
                                                   reverse=True)[:3]
                                for ds in top_datasets:
                                    st.markdown(f"• {ds['title'][:50]}{'...' if len(ds['title']) > 50 else ''}")
                            
                            # Lien vers l'organisation
                            if org.get('page'):
                                st.markdown(f"[🌐 Voir l'organisation]({org['page']})")
                            
                            if org['is_ministry']:
                                st.markdown('</div>', unsafe_allow_html=True)
                            else:
                                st.markdown('</div>', unsafe_allow_html=True)
                            st.markdown("---")
            
            elif view_mode == "Tableau compact":
                # Affichage en tableau avec distinction
                org_data = []
                for org in organizations:
                    org_data.append({
                        'Organisation': f"🏛️ {org['name']}" if org['is_ministry'] else f"🏢 {org['name']}",
                        'Type': 'Ministère' if org['is_ministry'] else 'Autre',
                        'Datasets': org['datasets_count'],
                        'Réutilisations': org['total_reuses'],
                        'Vues': org['total_views']
                    })
                
                df = pd.DataFrame(org_data)
                st.dataframe(df, use_container_width=True)
                
                # Graphique à barres
                if not df.empty:
                    fig = px.bar(df, x='Datasets', y='Organisation', 
                                orientation='h',
                                title='Nombre de datasets par organisation',
                                color='Type',
                                color_discrete_map={'Ministère': '#002395', 'Autre': '#0055A4'})
                    st.plotly_chart(fig, use_container_width=True)
            
            else:  # Graphique
                # Séparer ministères et autres
                ministries = [org for org in organizations if org['is_ministry']]
                others = [org for org in organizations if not org['is_ministry']]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if ministries:
                        org_names = [org['name'][:20] + '...' if len(org['name']) > 20 else org['name'] 
                                    for org in ministries[:8]]
                        
                        fig = go.Figure()
                        fig.add_trace(go.Bar(
                            name='Datasets',
                            x=org_names,
                            y=[org['datasets_count'] for org in ministries[:8]],
                            marker_color='#002395'
                        ))
                        
                        fig.update_layout(
                            title='🏛️ Ministères - Nombre de datasets',
                            xaxis_title='Ministères',
                            yaxis_title='Nombre de datasets',
                            height=400
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    if others:
                        org_names = [org['name'][:20] + '...' if len(org['name']) > 20 else org['name'] 
                                    for org in others[:8]]
                        
                        fig = go.Figure()
                        fig.add_trace(go.Bar(
                            name='Datasets',
                            x=org_names,
                            y=[org['datasets_count'] for org in others[:8]],
                            marker_color='#0055A4'
                        ))
                        
                        fig.update_layout(
                            title='🏢 Autres organisations - Nombre de datasets',
                            xaxis_title='Organisations',
                            yaxis_title='Nombre de datasets',
                            height=400
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                
                # Tableau récapitulatif
                st.markdown("**📊 Récapitulatif détaillé:**")
                recap_data = []
                for org in organizations[:15]:
                    recap_data.append({
                        'Organisation': f"🏛️ {org['name']}" if org['is_ministry'] else f"🏢 {org['name']}",
                        'Type': 'Ministère' if org['is_ministry'] else 'Autre',
                        'Datasets': org['datasets_count'],
                        'Total Réutilisations': f"{org['total_reuses']:,}",
                        'Total Vues': f"{org['total_views']:,}",
                        'Moyenne Réutilisations/Dataset': 
                        f"{org['total_reuses']//org['datasets_count'] if org['datasets_count'] > 0 else 0:,}"
                    })
                
                df_recap = pd.DataFrame(recap_data)
                st.dataframe(df_recap, use_container_width=True)
            
        except Exception as e:
            self.log_error(f"Erreur organisations: {str(e)}")
            st.error("Erreur d'affichage des organisations")

    def create_reuses_section(self):
        """Affiche la section des réutilisations"""
        try:
            st.markdown('<h3 class="section-header">🚀 RÉUTILISATIONS INNOVANTES</h3>', 
                       unsafe_allow_html=True)
            
            if not st.session_state.reuses:
                st.info("Chargement des réutilisations...")
                return
            
            # Filtrer les réutilisations valides
            valid_reuses = []
            for reuse in st.session_state.reuses:
                if reuse is not None and isinstance(reuse, dict) and 'id' in reuse:
                    valid_reuses.append(reuse)
            
            for reuse in valid_reuses[:10]:  # Augmenté à 10
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        title = reuse.get('title', 'Sans titre')
                        st.markdown(f"**{title}**")
                        
                        description = reuse.get('description', '')
                        if len(description) > 150:
                            description = description[:150] + "..."
                        st.markdown(f"*{description}*")
                        
                        # Type de réutilisation
                        reuse_type = reuse.get('type', 'Application')
                        st.markdown(f"**Type:** {reuse_type}")
                    
                    with col2:
                        # Métriques
                        metrics = reuse.get('metrics', {})
                        views = 0
                        
                        if metrics and isinstance(metrics, dict):
                            try:
                                views = int(metrics.get('views', 0))
                            except (ValueError, TypeError):
                                pass
                        
                        st.metric("Vues", views)
                        
                        # Lien
                        if reuse.get('page'):
                            st.markdown(f"[🔗 Voir le projet]({reuse['page']})")
                    
                    st.markdown("---")
        except Exception as e:
            self.log_error(f"Erreur réutilisations: {str(e)}")
            st.error("Erreur d'affichage des réutilisations")

    def create_sidebar(self):
        """Crée la sidebar avec les contrôles et options temps réel"""
        try:
            st.sidebar.markdown("## 🎛️ CONTRÔLES DE NAVIGATION")
            
            # Statut de connexion
            st.sidebar.markdown("### 📡 STATUT API")
            if st.session_state.datasets_loaded and st.session_state.datasets:
                ministry_count = len([d for d in st.session_state.datasets if self.is_ministry(self.safe_get(d, 'organization.name', ''))])
                st.sidebar.success("✅ Connecté")
                st.sidebar.info(f"📦 {len(st.session_state.datasets):,} datasets")
                st.sidebar.info(f"🏛️ {ministry_count:,} ministères")
            else:
                st.sidebar.error("❌ Hors ligne")
                st.sidebar.info("📋 Mode démonstration activé")
            
            # Options temps réel
            st.sidebar.markdown("### ⏱️ TEMPS RÉEL")
            
            auto_refresh = st.sidebar.checkbox(
                "Auto-rafraîchissement",
                value=st.session_state.get('auto_refresh', False),
                key="auto_refresh_toggle"
            )
            
            if auto_refresh:
                # CORRECTION : Utiliser min_value et max_value au lieu de min et max
                refresh_interval = st.sidebar.slider(
                    "Intervalle (secondes):",
                    min_value=10,  # CORRECTION ICI
                    max_value=300, # CORRECTION ICI
                    value=st.session_state.get('refresh_interval', 30),
                    key="refresh_interval_slider"
                )
                st.session_state.refresh_interval = refresh_interval
            
            st.session_state.auto_refresh = auto_refresh
            
            # Actions manuelles
            st.sidebar.markdown("### ⚡ ACTIONS MANUELLES")
            
            if st.sidebar.button("🔄 Rafraîchir maintenant", key="manual_refresh"):
                self.load_data(force_refresh=True)
                st.rerun()
            
            if st.sidebar.button("🔄 Vider le cache", key="clear_cache"):
                st.cache_data.clear()
                st.sidebar.success("Cache vidé!")
                time.sleep(1)
                st.rerun()
            
            # Filtres
            st.sidebar.markdown("### 🔧 FILTRES")
            
            format_filter = st.sidebar.multiselect(
                "Format des données:",
                ["CSV", "JSON", "XLSX", "PDF", "XML", "GEOJSON"],
                default=["CSV", "JSON"]
            )
            
            license_filter = st.sidebar.multiselect(
                "Licence:",
                ["Licence Ouverte", "ODbL", "Other"],
                default=["Licence Ouverte"]
            )
            
            # Options d'affichage
            st.sidebar.markdown("### ⚙️ OPTIONS")
            items_per_page = st.sidebar.slider("Résultats par page:", 10, 200, 50)
            
            # Navigation
            st.sidebar.markdown("### 🧭 NAVIGATION")
            
            if st.sidebar.button("🏠 Accueil", key="home_button"):
                st.session_state.current_view = "popular"
                st.session_state.current_query = ""
                st.rerun()
            
            # Reset search
            if st.session_state.current_view == "search":
                if st.sidebar.button("❌ Effacer la recherche", key="clear_search"):
                    st.session_state.current_view = "popular"
                    st.session_state.current_query = ""
                    st.rerun()
            
            # Debug
            if self.debug_mode:
                st.sidebar.markdown("### 🔧 DEBUG")
                if st.sidebar.button("Afficher debug", key="debug_btn"):
                    self.display_debug_info()
            
            # Informations système
            st.sidebar.markdown("---")
            st.sidebar.markdown("### 📊 INFOS SYSTÈME")
            st.sidebar.markdown("**API data.gouv.fr**")
            st.sidebar.markdown("✅ Connecté en temps réel")
            st.sidebar.markdown("🌐 API publique")
            st.sidebar.markdown("🔓 Aucune authentification requise")
            
            # Historique des mises à jour
            if st.session_state.realtime_updates:
                with st.sidebar.expander("📜 Historique des mises à jour"):
                    for update in st.session_state.realtime_updates[-10:]:
                        st.text(update)
            
            return {
                'format_filter': format_filter,
                'license_filter': license_filter,
                'items_per_page': items_per_page
            }
        except Exception as e:
            self.log_error(f"Erreur sidebar: {str(e)}")
            st.sidebar.error("Erreur de la sidebar")

    def create_footer(self):
        """Crée le pied de page"""
        try:
            st.markdown("---")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("### 📚 RESSOURCES")
                st.markdown("- [Documentation API](https://www.data.gouv.fr/api/)")
                st.markdown("- [Guide d'utilisation](https://www.data.gouv.fr/pages/utilisation/)")
                st.markdown("- [Conditions d'utilisation](https://www.data.gouv.fr/legal/)")
            
            with col2:
                st.markdown("### 🤝 CONTRIBUER")
                st.markdown("- [Ajouter des données](https://www.data.gouv.fr/pages/ouvrir-des-donnees/)")
                st.markdown("- [Signaler un problème](https://www.data.gouv.fr/pages/contact/)")
                st.markdown("- [Communauté](https://www.data.gouv.fr/community/)")
            
            with col3:
                st.markdown("### 📞 CONTACT")
                st.markdown("- data.gouv.fr")
                st.markdown("- Etalab")
                st.markdown("- contact@data.gouv.fr")
            
            st.markdown("---")
            st.markdown(
                "<div class='footer'>"
                "<p style='text-align: center;'>"
                "🔍 DASHBOARD OPEN DATA - MOTEUR DE RECHERCHES - Données en temps réel via l'API data.gouv.fr<br>"
                "© 2024 République Française - Tous droits réservés"
                "</p>"
                "</div>",
                unsafe_allow_html=True
            )
        except Exception as e:
            self.log_error(f"Erreur footer: {str(e)}")
            st.error("Erreur d'affichage du pied de page")

    def auto_refresh_loop(self):
        """Boucle de rafraîchissement automatique"""
        if st.session_state.get('auto_refresh', False):
            time.sleep(st.session_state.get('refresh_interval', 30))
            self.load_data(force_refresh=True)
            st.rerun()

    def run_dashboard(self):
        """Exécute le dashboard complet avec support temps réel"""
        try:
            # Charger les données
            self.load_data()
            
            # Sidebar
            controls = self.create_sidebar()
            
            # Header
            self.display_header()
            
            # Métriques clés
            stats = self.get_dataset_stats()
            self.display_key_metrics(stats)
            
            # Recherche
            datasets_to_display, search_title = self.create_search_interface()
            
            # Navigation par onglets
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "📊 Jeux de données", 
                "🏛️ Organisations", 
                "🚀 Réutilisations", 
                "📈 Analyses",
                "ℹ️ À propos"
            ])
            
            with tab1:
                self.display_datasets(datasets_to_display, search_title)
            
            with tab2:
                # Passer les datasets actuels à la section organisations
                self.create_organizations_section(datasets_to_display)
            
            with tab3:
                self.create_reuses_section()
            
            with tab4:
                self.create_visualizations(stats)
            
            with tab5:
                st.markdown("## 📋 À propos de ce dashboard")
                st.markdown("""
                Ce dashboard présente un **moteur de recherche massif de données ouvertes** du gouvernement français 
                avec **capacités temps réel** et **priorité aux ministères**.
                
                **🚀 Fonctionnalités temps réel:**
                - 🔄 **Auto-rafraîchissement** configurable (10-300 secondes)
                - 📊 **Mises à jour automatiques** des données
                - 🆕 **Détection des nouveaux datasets** en temps réel
                - 📈 **Visualisations dynamiques** avec données fraîches
                - 🏷️ **Indicateurs visuels** de l'activité temps réel
                - 📜 **Historique des mises à jour** consultable
                
                **🔍 Fonctionnalités moteur de recherche:**
                - 📊 **Volume massif** : 500+ datasets disponibles
                - 🏛️ **Priorité aux ministères** dans tous les affichages et recherches
                - 📄 **Pagination intelligente** pour naviguer parmi des milliers de datasets
                - 🔍 **Moteur de recherche avancé** avec filtres ministériels
                - 📊 **Visualisations dédiées** aux données ministérielles
                - 🏛️ **Organisations dynamiques** avec distinction ministres/autres
                - 🚀 **Découverte des réutilisations** innovantes
                - 📈 **Analyses statistiques** en temps réel
                - 🔧 **Mode débogage** intégré
                
                **🏛️ Priorité aux ministères:**
                - Tri automatique favorisant les données ministérielles
                - Badges visuels pour identifier les ministères
                - Filtres spécifiques par type de ministère
                - Statistiques séparées pour les ministères
                - Visualisations comparatives
                
                **⏱️ Optimisations temps réel:**
                - Cache intelligent avec TTL court (5 minutes)
                - Rafraîchissement automatique configurable
                - Détection des nouveaux datasets
                - Indicateurs visuels d'activité
                - Notifications de mises à jour
                
                **📊 Volume massif:**
                - **500+ datasets** générés dynamiquement
                - **Pagination** pour une navigation fluide
                - **Chargement optimisé** avec barre de progression
                - **Tri intelligent** avec priorité aux ministères
                - **Filtres avancés** pour affiner la recherche
                
                **🔧 Performance:**
                - Cache intelligent pour les requêtes répétées
                - Limitation des analyses pour maintenir la réactivité
                - Barre de progression pour le suivi du chargement
                - Gestion optimisée de la mémoire
                
                **Sources des données:**
                - API officielle data.gouv.fr (jusqu'à 1000 datasets)
                - Données de démonstration massives (500+ datasets)
                - Génération automatique de datasets variés
                - Métriques d'utilisation réalistes
                
                **Technologies utilisées:**
                - Streamlit pour l'interface
                - Plotly pour les visualisations
                - API REST data.gouv.fr
                - Pandas pour l'analyse des données
                - Pagination et cache pour la performance
                
                Ce projet est open source et contributif.
                """)
                
                st.markdown("---")
                st.markdown("""
                **📊 Méthodologie temps réel:**
                - Algorithmes de détection des changements
                - Cache court pour les données fraîches
                - Rafraîchissement périodique automatique
                - Tri prioritaire par date de modification
                
                **🔧 Développement:**
                - Code source disponible
                - Contributions bienvenues
                - Issues et suggestions
                """)
            
            # Footer
            self.create_footer()
            
        except Exception as e:
            self.log_error(f"Erreur critique dashboard: {str(e)}")
            st.markdown(f'<div class="error-box">💥 Erreur critique: {str(e)}</div>', unsafe_allow_html=True)
            
            # Afficher les détails de l'erreur pour le débogage
            if self.debug_mode:
                st.code(traceback.format_exc())
            
            # Option de récupération
            if st.button("🔄 Redémarrer l'application"):
                st.cache_data.clear()
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()

# Point d'entrée principal
if __name__ == "__main__":
    try:
        dashboard = RealTimeOpenDataDashboard()
        dashboard.run_dashboard()
        
        # Lancer le rafraîchissement automatique en arrière-plan
        if st.session_state.get('auto_refresh', False):
            # Utiliser un placeholder pour le rafraîchissement automatique
            st.markdown("""
            <script>
                // Auto-refresh en arrière-plan
                setTimeout(() => {
                    window.location.reload();
                }, 30000);
            </script>
            """, unsafe_allow_html=True)
            
    except Exception as e:
        st.error(f"💥 Erreur de démarrage: {str(e)}")
        st.code(traceback.format_exc())
        
        # Bouton de récupération d'urgence
        if st.button("🚨 Redémarrage d'urgence"):
            st.cache_data.clear()
            st.rerun()
