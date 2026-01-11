"""
🚀 API 1xBet Complète - FastAPI + Redis + SQLite + Authentification
📦 Version Finale 100% Complète avec Toute la Documentation et Authentification
📊 Basé sur la documentation complète du format JSON
🆕 AJOUTS: 6 nouveaux endpoints API incluant Get1x2_VZip
"""

import json
import requests
import asyncio
import httpx
import redis.asyncio as redis
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from fastapi import FastAPI, WebSocket, HTTPException, BackgroundTasks, Depends, Query, Request, WebSocketDisconnect, status, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, Field, validator
from enum import IntEnum
from contextlib import asynccontextmanager
import uuid
import logging
from dataclasses import dataclass
import time
import subprocess
import sys
import os
import signal
import atexit
from decimal import Decimal
import math
import jwt as pyjwt
from jose import JWTError, jwt
from passlib.context import CryptContext
import secrets


# ============================================================================
# CONFIGURATION RAILWAY
# ============================================================================

def get_railway_config():
    """Configuration spécifique pour Railway"""
    # Utiliser les variables d'environnement Railway
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_password = os.getenv("REDIS_PASSWORD", None)
    redis_url = os.getenv("REDIS_URL", None)
    
    # Configuration SQLite adaptée pour Railway
    sqlite_path = os.getenv("SQLITE_PATH", "/data/1xbet.db")
    
    # Port pour Railway
    port = int(os.getenv("PORT", 8000))
    
    # Debug mode
    debug = os.getenv("DEBUG", "false").lower() == "true"
    
    return {
        "REDIS_HOST": redis_host,
        "REDIS_PORT": redis_port,
        "REDIS_PASSWORD": redis_password,
        "REDIS_URL": redis_url,
        "SQLITE_DB_PATH": sqlite_path,
        "PORT": port,
        "DEBUG": debug
    }

railway_config = get_railway_config()

class Config:
    """Configuration de l'application adaptée pour Railway"""
    # Redis config
    REDIS_HOST = railway_config["REDIS_HOST"]
    REDIS_PORT = railway_config["REDIS_PORT"]
    REDIS_PASSWORD = railway_config["REDIS_PASSWORD"]
    REDIS_URL = railway_config["REDIS_URL"]
    
    # Database
    SQLITE_DB_PATH = railway_config["SQLITE_DB_PATH"]
    
    # API
    API_BASE_URL = "https://1xbet.ci/service-api"
    CACHE_TTL = {
        'live_odds': 10,
        'prematch_odds': 60,
        'sports': 300,
        'events': 30,
        'event_detail': 60,
        'express_day': 600,
        'championships': 3600,
        'top_games': 300,
        'game_results': 3600,
        'get1x2_vzip': 5
    }
    REQUEST_TIMEOUT = 30
    COLLECTION_INTERVAL = 10
    
    # Authentication
    SECRET_KEY = os.getenv("SECRET_KEY", "WjXp4s7v9y$B&E)H+MbQeThWmZq4t6w9z$C&F)J@NcRfUjXn2r5u8x/A?D(G+KbP")
    ALGORITHM = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 300000
    REFRESH_TOKEN_EXPIRE_DAYS = 7
    API_KEY_LENGTH = 32
    API_KEY_PREFIX = "1xbet_"
    
    # Railway specific
    PORT = railway_config["PORT"]
    DEBUG = railway_config["DEBUG"]


# ============================================================================
# CONFIGURATION
# ============================================================================

def setup_signal_handlers():
    """Configure les gestionnaires de signaux"""
    def signal_handler(sig, frame):
        print("\n\n👋 Arrêt demandé...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def setup_logging():
    """Configure le logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('1xbet_api_complete.log')
        ]
    )

class ServiceManager:
    """Gestionnaire de services simplifié pour Railway"""
    
    def __init__(self):
        self.sqlite_db_path = Config.SQLITE_DB_PATH
        
    def setup_sqlite(self):
        """Configure SQLite pour Railway"""
        try:
            # Créer le répertoire si nécessaire
            os.makedirs(os.path.dirname(self.sqlite_db_path), exist_ok=True)
            logging.info(f"✅ SQLite configuré: {self.sqlite_db_path}")
            return True
        except Exception as e:
            logging.error(f"❌ Erreur configuration SQLite: {e}")
            return False
    
    def check_services(self):
        """Vérifie l'état des services pour Railway"""
        services_status = {
            "sqlite": False
        }
        
        # Vérifier SQLite
        try:
            services_status["sqlite"] = os.path.exists(self.sqlite_db_path) or self.setup_sqlite()
        except:
            services_status["sqlite"] = False
        
        # Redis est géré séparément
        services_status["redis"] = True  # Assume Redis via Railway
        
        return services_status
    
    def stop_services(self):
        """Arrête les services"""
        print("\n🛑 Arrêt des services...")
        
        # Arrêter Redis
        if self.redis_process:
            try:
                subprocess.run(["redis-cli", "shutdown"], capture_output=True)
                print("✅ Redis arrêté")
            except:
                print("⚠️ Impossible d'arrêter Redis proprement")
                subprocess.run(["pkill", "-f", "redis-server"])
        
        # Supprimer les fichiers temporaires
        for f in [self.redis_pid_file]:
            if os.path.exists(f):
                os.remove(f)


# ============================================================================
# MODÈLES D'AUTHENTIFICATION
# ============================================================================

class User(BaseModel):
    """Modèle utilisateur"""
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = False
    roles: List[str] = ["user"]
    api_key: Optional[str] = None
    api_key_expires: Optional[datetime] = None
    created_at: datetime = datetime.now()
    last_login: Optional[datetime] = None

class UserInDB(User):
    """Modèle utilisateur en base avec mot de passe"""
    hashed_password: str

class Token(BaseModel):
    """Modèle token JWT"""
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "bearer"
    expires_in: int

class APIKeyCreate(BaseModel):
    """Modèle pour créer une clé API"""
    name: str
    expires_days: Optional[int] = 30

class APIKeyInfo(BaseModel):
    """Information sur une clé API"""
    name: str
    key: str
    created_at: datetime
    expires_at: Optional[datetime]
    last_used: Optional[datetime]

# ============================================================================
# TABLES DE RÉFÉRENCE COMPLÈTES (Documentation 100%)
# ============================================================================

# 🏷️ Catégories de sports (CID)
class SportCategory(IntEnum):
    POPULAR = 1           # Sports principaux avec images
    OTHER_SPORTS = 2      # Sports secondaires
    SPECIAL_GAMES = 3     # Jeux virtuels (Marble Games)
    ESPORTS = 4          # Compétitions de jeux vidéo
    CARD_GAMES = 6       # Jeux de cartes virtuels

# 🏆 Statuts des matchs (SS)
class MatchStatus(IntEnum):
    UNDEFINED = 0        # Statut inconnu/non défini
    FINISHED = 1         # Match terminé
    UPCOMING = 2         # À venir
    LIVE = 3            # En cours
    POSTPONED = 4       # Reporté
    CANCELLED = 5       # Annulé
    SUSPENDED = 6       # Temporairement arrêté
    HALFTIME = 7        # Période de pause
    ABANDONED = 8       # Match abandonné
    DELAYED = 9         # Début retardé

# 💰 Types de cotes (KI)
class OddsType(IntEnum):
    LIVE = 1            # Cotes en direct
    PREMATCH = 3        # Cotes en prématch
    FRACTIONAL = 5      # Cotes fractionnaires
    AMERICAN = 7        # Cotes américaines

# 🎯 Groupes de marchés (G)
class MarketGroup(IntEnum):
    RESULT = 1          # Résultat final (1, X, 2)
    ASIAN_HANDICAP = 2  # Handicap asiatique
    DOUBLE_CHANCE = 8   # Double chance (1X, X2, 12)
    TOTAL = 15          # Total de buts/points
    TEAM_TOTAL = 17     # Total équipe individuelle
    EXACT_SCORE = 19    # Résultat exact
    EURO_HANDICAP = 62  # Handicap 3 voies
    ASIAN_TOTAL = 99    # Total asiatique
    HALFTIME_RESULT = 101  # Résultat mi-temps
    HALFTIME_ASIAN_HANDICAP = 2854  # Handicap asiatique mi-temps
    HALFTIME_EXACT_SCORE = 2766  # Score exact mi-temps

# 📊 Types d'événement (T)
class EventType(IntEnum):
    FOOTBALL_FULL = 1000      # Match complet de football (90-120 min)
    FOOTBALL_HALF = 500       # Mi-temps de football (45 min)
    BASKETBALL_QUARTER = 300  # Quart-temps de basketball (10 min)
    BASKETBALL_QUARTER_VAR = 250  # Quart-temps variante
    TENNIS_SET = 200          # Set de tennis/volleyball
    TENNIS_GAME = 50          # Jeu de tennis

# 🏆 Sous-statuts (SST)
class SubStatus(IntEnum):
    NORMAL = 1         # Événement régulier
    WITH_ODDS = 2      # Marchés disponibles
    WITHOUT_ODDS = 3   # Aucun marché
    LIMITED_ODDS = 4   # Marchés réduits

# 🌍 Codes pays complets (COI)
class CountryCode(IntEnum):
    INTERNATIONAL = 1
    GREECE = 60
    SPAIN = 78
    ITALY = 79
    QATAR = 86
    CYPRUS = 88
    UAE = 139
    USA = 153
    TURKEY = 190
    FINLAND = 197
    FRANCE = 198
    CZECH_REPUBLIC = 204
    EUROPE = 223
    GENERIC = 225
    ASIA = 229
    ENGLAND = 231
    LITHUANIA = 107
    GERMANY = 245
    PORTUGAL = 247
    NETHERLANDS = 250

# ⚽ IDs Sport (SI) courants
class SportID(IntEnum):
    FOOTBALL = 1
    ICE_HOCKEY = 2
    BASKETBALL = 3
    TENNIS = 4
    VOLLEYBALL = 6
    RUGBY = 7
    HANDBALL = 8
    TABLE_TENNIS = 10
    AMERICAN_FOOTBALL = 13
    ESPORTS = 40
    CRICKET = 66
    FIFA_ESPORT = 85
    WEATHER = 176
    POLITICS = 202
    MARBLE_FOOTBALL = 211

# 📊 IDs Statistiques (SC.ST)
class StatisticID(IntEnum):
    POSSESSION = 29        # Possession %
    ATTACKS = 45           # Attaques
    THREE_POINTERS = 48    # Paniers à 3 points
    TWO_POINTERS = 49      # Paniers à 2 points
    FREE_THROWS = 50       # Lancers francs
    TIMEOUTS_REMAINING = 52 # Temps morts restants
    FOULS = 53             # Fautes
    DANGEROUS_ATTACKS = 58 # Attaques dangereuses
    SHOTS_ON_TARGET = 59   # Tirs cadrés
    SHOTS_OFF_TARGET = 60  # Tirs non cadrés
    CORNERS = 70           # Corners
    SUBSTITUTIONS = 92     # Substitutions
    EXPECTED_GOALS = 93    # xG

# 🎪 Indicateurs de marché spécial (MS)
class SpecialMarket(IntEnum):
    STANDARD = 0          # Marché standard
    LIVE_EVENTS = 3       # Matchs en direct
    PROMOTIONS = 7        # Événements promotionnels
    MARBLE_GAMES = 8      # Jeux Marble
    UPCOMING_EVENTS = 9   # Matchs à venir
    ESPORTS = 28          # Compétitions eSports

# 🔑 Clés MIS (Métadonnées Match)
class MetadataKey(IntEnum):
    TOUR = 1              # Tour/étape
    VENUE = 2             # Lieu/stade
    FORMAT = 3            # Format du match
    CITY = 7              # Ville
    TEMPERATURE = 9       # Température
    COUNTRY = 11          # Pays
    LOCAL_TIME = 20       # Heure locale
    WEATHER_CONDITION = 21 # Conditions météo
    FEELS_LIKE = 22       # Température ressentie
    WIND_SPEED = 23       # Vitesse du vent
    WIND_DIRECTION = 24   # Direction du vent
    PRESSURE = 25         # Pression atmosphérique
    PRESSURE_UNIT = 26    # Unité de pression
    HUMIDITY = 27         # Humidité
    HUMIDITY_UNIT = 28    # Unité humidité
    PRECIPITATION = 35    # Précipitation
    PRECIPITATION_UNIT = 36 # Unité précipitation
    REFEREE = 40          # Arbitre
    ATTENDANCE = 41       # Assistance
    SURFACE = 50          # Type de surface

# 💰 Types de paris par groupe (T dans E)
class BetType(IntEnum):
    # Groupe 1 (Résultat)
    HOME_WIN = 1          # Équipe 1 gagne
    DRAW = 2              # Match nul
    AWAY_WIN = 3          # Équipe 2 gagne
    
    # Groupe 2 (Handicap asiatique)
    HOME_HANDICAP = 7     # Handicap équipe 1
    AWAY_HANDICAP = 8     # Handicap équipe 2
    
    # Groupe 8 (Double chance)
    HOME_OR_DRAW = 4      # 1X
    DRAW_OR_AWAY = 5      # X2
    HOME_OR_AWAY = 6      # 12
    
    # Groupe 15 (Total)
    OVER = 11             # Over
    UNDER = 12            # Under
    
    # Groupe 62 (Handicap européen)
    EURO_HOME_HANDICAP = 13
    EURO_AWAY_HANDICAP = 14

# ============================================================================
# SCHEMAS PYDANTIC COMPLETS (Documentation 100%)
# ============================================================================

class TeamSchema(BaseModel):
    """Schéma complet pour une équipe"""
    name_fr: Optional[Union[str, Dict]] = Field(None, alias="O1")
    name_en: Optional[str] = Field(None, alias="O1E")
    name_ru: Optional[str] = Field(None, alias="O1R")
    team_id: Optional[int] = Field(None, alias="O1I")
    logos: Optional[List[str]] = Field(None, alias="O1IMG")
    country_id: Optional[int] = Field(None, alias="O1C")
    city: Optional[str] = Field(None, alias="O1CT")
    additional_ids: Optional[List[int]] = Field(None, alias="O1IS")
    
    class Config:
        populate_by_name = True
        extra = "ignore"
    
    @validator('name_fr', pre=True)
    def handle_name_fr(cls, v):
        """Gère le nom qui peut être dict ou string"""
        if isinstance(v, dict):
            return v.get('name', str(v))
        return v

class PeriodScoreSchema(BaseModel):
    """Schéma pour un score de période"""
    period_number: Optional[int] = Field(None, alias="Key")
    period_data: Optional[Dict] = Field(None, alias="Value")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class StatisticItemSchema(BaseModel):
    """Schéma pour un item de statistique"""
    stat_id: Optional[int] = Field(None, alias="ID")
    name_fr: Optional[str] = Field(None, alias="N")
    home_value: Optional[Union[str, int, float]] = Field(None, alias="S1")
    away_value: Optional[Union[str, int, float]] = Field(None, alias="S2")
    name_en: Optional[str] = Field(None, alias="E")
    name_ru: Optional[str] = Field(None, alias="R")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class StatisticsGroupSchema(BaseModel):
    """Schéma pour un groupe de statistiques (par période)"""
    period: Optional[int] = Field(None, alias="Key")  # 0=global, 1=1ère période
    statistics: Optional[List[StatisticItemSchema]] = Field(None, alias="Value")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class LiveScoreSchema(BaseModel):
    """Schéma complet pour les scores en direct (SC)"""
    current_period: Optional[int] = Field(None, alias="CP")
    period_name: Optional[str] = Field(None, alias="CPS")
    final_score: Optional[Dict[str, int]] = Field(None, alias="FS")
    serving_team: Optional[int] = Field(None, alias="HC")
    additional_info: Optional[str] = Field(None, alias="I")
    points_in_period: Optional[int] = Field(None, alias="P")
    period_scores: Optional[List[PeriodScoreSchema]] = Field(None, alias="PS")
    live_status: Optional[str] = Field(None, alias="SLS")
    statistics: Optional[List[StatisticsGroupSchema]] = Field(None, alias="ST")
    elapsed_time: Optional[int] = Field(None, alias="TS")
    remaining_time: Optional[int] = Field(None, alias="TR")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class MarketSchema(BaseModel):
    """Schéma complet pour un marché de pari"""
    odds: Optional[float] = Field(None, alias="C")
    odds_str: Optional[str] = Field(None, alias="CV")
    group: Optional[int] = Field(None, alias="G")
    type: Optional[int] = Field(None, alias="T")
    handicap: Optional[float] = Field(None, alias="P")
    calculated: Optional[int] = Field(None, alias="CE")
    market_id: Optional[int] = Field(None, alias="ID")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class AdditionalMarketGroupSchema(BaseModel):
    """Schéma pour un groupe de marchés additionnels (AE)"""
    group: Optional[int] = Field(None, alias="G")
    markets: Optional[List[MarketSchema]] = Field(None, alias="ME")
    name_fr: Optional[str] = Field(None, alias="N")
    name_en: Optional[str] = Field(None, alias="E")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class MatchInfoSchema(BaseModel):
    """Schéma pour les métadonnées du match (MIO)"""
    location: Optional[str] = Field(None, alias="Loc")
    round: Optional[str] = Field(None, alias="TSt")
    match_format: Optional[str] = Field(None, alias="MaF")
    referee: Optional[str] = Field(None, alias="Ref")
    attendance: Optional[str] = Field(None, alias="Att")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class MetadataItemSchema(BaseModel):
    """Schéma pour un item de métadonnées (MIS)"""
    key: Optional[int] = Field(None, alias="K")
    value: Optional[str] = Field(None, alias="V")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class WinProbabilitySchema(BaseModel):
    """Schéma pour les probabilités de victoire (WP)"""
    home_win: Optional[float] = Field(None, alias="P1")
    away_win: Optional[float] = Field(None, alias="P2")
    draw: Optional[float] = Field(None, alias="PX")
    winner: Optional[float] = Field(None, alias="PW")
    loser: Optional[float] = Field(None, alias="PL")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class EventSchema(BaseModel):
    """Schéma principal COMPLET pour un événement"""
    # Identifiants
    event_id: Optional[int] = Field(None, alias="I")
    secondary_id: Optional[int] = Field(None, alias="N")
    start_time: Optional[datetime] = Field(None, alias="S")
    parent_event_id: Optional[int] = Field(None, alias="ZP")
    
    # Sport
    sport_id: Optional[int] = Field(None, alias="SI")
    sport_name_fr: Optional[str] = Field(None, alias="SN")
    sport_name_en: Optional[str] = Field(None, alias="SE")
    sport_name_ru: Optional[str] = Field(None, alias="SR")
    sport_category: Optional[int] = Field(None, alias="CID")
    sport_icon: Optional[str] = Field(None, alias="SIMG")
    
    # Compétition
    competition_id: Optional[int] = Field(None, alias="CI")
    competition_name_fr: Optional[Union[str, Dict]] = Field(None, alias="L")
    competition_name_en: Optional[str] = Field(None, alias="LE")
    competition_name_ru: Optional[str] = Field(None, alias="LR")
    league_id: Optional[int] = Field(None, alias="LI")
    country_id: Optional[int] = Field(None, alias="COI")
    country_name_fr: Optional[str] = Field(None, alias="CN")
    country_name_en: Optional[str] = Field(None, alias="CE")
    counter: Optional[int] = Field(None, alias="CO")
    
    # Équipes
    home_team: Optional[Union[Dict, str]] = Field(None, alias="O1")
    away_team: Optional[Union[Dict, str]] = Field(None, alias="O2")
    
    # Statut
    status: Optional[MatchStatus] = Field(None, alias="SS")
    substatus: Optional[int] = Field(None, alias="SST")
    status_indicator: Optional[int] = Field(None, alias="SSI")
    highlight_status: Optional[int] = Field(None, alias="HS")
    highlight_indicator: Optional[bool] = Field(None, alias="HSI")
    
    # Cotes et marchés
    main_markets: Optional[List[MarketSchema]] = Field(None, alias="E")
    additional_markets: Optional[List[AdditionalMarketGroupSchema]] = Field(None, alias="AE")
    total_markets: Optional[int] = Field(None, alias="EC")
    odds_type: Optional[OddsType] = Field(None, alias="KI")
    
    # Temps et périodes
    event_type: Optional[int] = Field(None, alias="T")
    period_name: Optional[str] = Field(None, alias="TN")
    period_name_singular: Optional[str] = Field(None, alias="TNS")
    note_fr: Optional[str] = Field(None, alias="V")
    note_en: Optional[str] = Field(None, alias="VE")
    
    # Indicateurs
    is_available: Optional[bool] = Field(None, alias="SUBA")
    is_highlighted: Optional[bool] = Field(None, alias="HL")
    is_recommended: Optional[bool] = Field(None, alias="GSE")
    is_live_updated: Optional[bool] = Field(None, alias="HLU")
    has_half_match: Optional[bool] = Field(None, alias="HHTHS")
    has_graphics: Optional[bool] = Field(None, alias="IG")
    is_virtual: Optional[bool] = Field(None, alias="ICY")
    in_session: Optional[bool] = Field(None, alias="INS")
    
    # Groupes
    bookmaker_id: Optional[int] = Field(None, alias="B")
    competition_group: Optional[int] = Field(None, alias="SGC")
    sport_group_id: Optional[str] = Field(None, alias="SGI")
    sport_template_id: Optional[str] = Field(None, alias="STI")
    league_group: Optional[int] = Field(None, alias="GLI")
    group_version: Optional[int] = Field(None, alias="GVE")
    highlight_group: Optional[bool] = Field(None, alias="HLGI")
    
    # Probabilités
    win_probabilities: Optional[WinProbabilitySchema] = Field(None, alias="WP")
    
    # Métadonnées
    match_info: Optional[MatchInfoSchema] = Field(None, alias="MIO")
    metadata: Optional[List[MetadataItemSchema]] = Field(None, alias="MIS")
    
    # Streaming
    stream_id: Optional[int] = Field(None, alias="SmI")
    
    # Marchés spéciaux
    special_markets: Optional[List[int]] = Field(None, alias="MS")
    
    # Champs LIVE exclusifs
    live_score: Optional[LiveScoreSchema] = Field(None, alias="SC")
    current_period_seconds: Optional[int] = Field(None, alias="R")
    home_match_home: Optional[int] = Field(None, alias="HMH")
    last_update: Optional[datetime] = Field(None, alias="U")
    additional_info: Optional[str] = Field(None, alias="DI")
    info_version: Optional[int] = Field(None, alias="IV")
    competition_image: Optional[str] = Field(None, alias="CHIMG")
    bet_note: Optional[str] = Field(None, alias="PN")
    
    class Config:
        use_enum_values = True
        populate_by_name = True
        arbitrary_types_allowed = True
        extra = "ignore"
    
    @validator('home_team', 'away_team', 'competition_name_fr', pre=True)
    def handle_string_or_dict(cls, v):
        """Gère les champs qui peuvent être soit des strings soit des dicts"""
        if isinstance(v, dict):
            # Si c'est un dict avec un nom, extraire le nom
            return v.get('name', v.get('value', str(v)))
        return v

class SportSchema(BaseModel):
    """Schéma complet pour un sport"""
    competition_count: Optional[int] = Field(None, alias="C")
    event_count: Optional[int] = Field(None, alias="CC")
    category_id: Optional[int] = Field(None, alias="CID")
    name_en: Optional[str] = Field(None, alias="E")
    sport_id: Optional[int] = Field(None, alias="I")
    image_type: Optional[int] = Field(None, alias="IT")
    special_market: Optional[int] = Field(None, alias="MS")
    name_fr: Optional[str] = Field(None, alias="N")
    name_ru: Optional[str] = Field(None, alias="R")
    live_event_count: Optional[int] = Field(None, alias="V")
    live_competition_count: Optional[int] = Field(None, alias="Z")
    is_virtual: Optional[bool] = Field(None, alias="ICY")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

# ============================================================================
# NOUVEAUX MODÈLES POUR LES NOUVEAUX ENDPOINTS
# ============================================================================

class ExpressDayItem(BaseModel):
    """Schéma pour un pari express du jour"""
    expressId: int = Field(..., alias="expressId")
    cf: float = Field(..., alias="cf")
    cfView: str = Field(..., alias="cfView")
    events: List[Dict[str, Any]] = Field(..., alias="events")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class ChampionshipItem(BaseModel):
    """Schéma pour un championnat"""
    id: int = Field(..., alias="id")
    name: str = Field(..., alias="name")
    image: Optional[str] = Field(None, alias="image")
    sportId: int = Field(..., alias="sportId")
    gamesCount: int = Field(..., alias="gamesCount")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class GameResultSchema(BaseModel):
    """Schéma pour les résultats de match"""
    id: int = Field(..., alias="id")
    sportId: int = Field(..., alias="sportId")
    champId: int = Field(..., alias="champId")
    champName: str = Field(..., alias="champName")
    opp1: str = Field(..., alias="opp1")
    opp2: str = Field(..., alias="opp2")
    opp1Images: List[str] = Field(..., alias="opp1Images")
    opp2Images: List[str] = Field(..., alias="opp2Images")
    score: str = Field(..., alias="score")
    dopInfo: str = Field("", alias="dopInfo")
    hasSubGame: bool = Field(False, alias="hasSubGame")
    dateStart: int = Field(..., alias="dateStart")
    countSubGame: int = Field(0, alias="countSubGame")
    subGame: List[Dict] = Field([], alias="subGame")
    matchInfosFull: str = Field("", alias="matchInfosFull")
    matchInfos: Dict = Field({}, alias="matchInfos")
    gameTypeName: str = Field("", alias="gameTypeName")
    gameVidName: str = Field("", alias="gameVidName")
    stadiumId: int = Field(0, alias="stadiumId")
    champCountry: int = Field(0, alias="champCountry")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class SportsListItem(BaseModel):
    """Schéma pour un sport dans la liste simple"""
    id: int = Field(..., alias="id")
    name: str = Field(..., alias="name")
    isTop: bool = Field(False, alias="isTop")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class TopGameSchema(BaseModel):
    """Schéma pour les matchs du top"""
    CI: int = Field(..., alias="CI")
    COI: int = Field(..., alias="COI")
    E: List[Dict] = Field(..., alias="E")
    EC: int = Field(..., alias="EC")
    I: int = Field(..., alias="I")
    KI: int = Field(..., alias="KI")
    L: str = Field(..., alias="L")
    LE: str = Field(..., alias="LE")
    LI: int = Field(..., alias="LI")
    LR: str = Field(..., alias="LR")
    MIS: List[Dict] = Field(..., alias="MIS")
    N: int = Field(..., alias="N")
    O1: str = Field(..., alias="O1")
    O1C: int = Field(..., alias="O1C")
    O1CT: str = Field(..., alias="O1CT")
    O1E: str = Field(..., alias="O1E")
    O1I: int = Field(..., alias="O1I")
    O1IMG: List[str] = Field(..., alias="O1IMG")
    O1IS: List[int] = Field(..., alias="O1IS")
    O1R: str = Field(..., alias="O1R")
    O2: str = Field(..., alias="O2")
    O2C: int = Field(..., alias="O2C")
    O2CT: str = Field(..., alias="O2CT")
    O2E: str = Field(..., alias="O2E")
    O2I: int = Field(..., alias="O2I")
    O2IMG: List[str] = Field(..., alias="O2IMG")
    O2IS: List[int] = Field(..., alias="O2IS")
    O2R: str = Field(..., alias="O2R")
    PN: str = Field("", alias="PN")
    S: int = Field(..., alias="S")
    SE: str = Field(..., alias="SE")
    SGI: str = Field(..., alias="SGI")
    SI: int = Field(..., alias="SI")
    SN: str = Field(..., alias="SN")
    SR: str = Field(..., alias="SR")
    SS: int = Field(..., alias="SS")
    SST: int = Field(..., alias="SST")
    STI: str = Field(..., alias="STI")
    TG: str = Field("", alias="TG")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

class APIResponse(BaseModel):
    """Schéma pour les réponses API standard"""
    error: Optional[str] = Field(None, alias="Error")
    error_code: Optional[int] = Field(None, alias="ErrorCode")
    guid: Optional[str] = Field(None, alias="Guid")
    request_id: Optional[int] = Field(None, alias="Id")
    success: Optional[bool] = Field(None, alias="Success")
    value: Optional[Union[List, Dict]] = Field(None, alias="Value")
    
    class Config:
        populate_by_name = True
        extra = "ignore"

# ============================================================================
# ANALYSE ET MONITORING
# ============================================================================

class AnalysisData(BaseModel):
    """Données d'analyse de la collecte"""
    endpoint: str
    status: str
    count: int
    success: bool
    error_code: int
    error_msg: str
    categories: Optional[Dict[str, int]] = None
    timestamp: datetime

class MonitoringMetrics(BaseModel):
    """Métriques de monitoring"""
    data_freshness: Optional[float] = None
    odds_completeness: Optional[float] = None
    update_frequency: Optional[float] = None
    event_validity: bool = True

class AlertRule(BaseModel):
    """Règle d'alerte"""
    condition: str
    severity: str  # 'info', 'warning', 'error'
    message: str
    threshold: Optional[float] = None

# ============================================================================
# UTILS AUTHENTIFICATION
# ============================================================================

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Vérifie un mot de passe"""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Hash un mot de passe"""
    return pwd_context.hash(password)

def generate_api_key() -> str:
    """Génère une clé API"""
    random_bytes = secrets.token_bytes(Config.API_KEY_LENGTH)
    api_key = Config.API_KEY_PREFIX + random_bytes.hex()
    return api_key

def hash_api_key(api_key: str) -> str:
    """Hash une clé API"""
    return pwd_context.hash(api_key)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Crée un token JWT"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=Config.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = pyjwt.encode(to_encode, Config.SECRET_KEY, algorithm=Config.ALGORITHM)
    return encoded_jwt

def create_refresh_token(data: dict) -> str:
    """Crée un refresh token JWT"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=Config.REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    encoded_jwt = pyjwt.encode(to_encode, Config.SECRET_KEY, algorithm=Config.ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> Optional[dict]:
    """Vérifie et décode un token JWT (SYNCHRONE)"""
    try:
        # Essayer avec jose d'abord
        try:
            from jose import jwt
            payload = jwt.decode(
                token, 
                Config.SECRET_KEY, 
                algorithms=[Config.ALGORITHM]
            )
            return payload
        except ImportError:
            # Fallback vers pyjwt
            import jwt as pyjwt
            payload = jwt.decode(
                token, 
                Config.SECRET_KEY, 
                algorithms=[Config.ALGORITHM]
            )
            return payload
        except Exception:
            return None
    except Exception:
        return None

# ============================================================================
# USER DATABASE (SQLite)
# ============================================================================

class UserDatabase:
    """Gestionnaire de base de données utilisateurs"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        
    def init_tables(self):
        """Initialise les tables utilisateurs"""
        try:
            self.db.cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    email TEXT UNIQUE,
                    full_name TEXT,
                    hashed_password TEXT NOT NULL,
                    disabled BOOLEAN DEFAULT FALSE,
                    roles TEXT DEFAULT '["user"]',
                    api_key TEXT UNIQUE,
                    api_key_expires TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP,
                    settings TEXT DEFAULT '{}'
                )
            ''')
            
            self.db.cursor.execute('''
                CREATE TABLE IF NOT EXISTS api_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    key_hash TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    last_used TIMESTAMP,
                    revoked BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (user_id) REFERENCES users(username)
                )
            ''')
            
            self.db.cursor.execute('''
                CREATE TABLE IF NOT EXISTS refresh_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    token_hash TEXT UNIQUE NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    revoked BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(username)
                )
            ''')
            
            self.db.cursor.execute('''
                CREATE TABLE IF NOT EXISTS login_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ip_address TEXT,
                    user_agent TEXT,
                    success BOOLEAN,
                    method TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(username)
                )
            ''')
            
            # Indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_users_api_key ON users(api_key)",
                "CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_id ON refresh_tokens(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_login_history_user_id ON login_history(user_id)",
                "CREATE INDEX IF NOT EXISTS idx_login_history_login_time ON login_history(login_time)"
            ]
            
            for idx in indexes:
                self.db.cursor.execute(idx)
            
            self.db.conn.commit()
            
            # Créer l'utilisateur admin par défaut si nécessaire
            self.create_default_admin()
            
            logging.info("✅ Tables d'authentification créées")
            
        except Exception as e:
            logging.error(f"Erreur création tables auth: {e}")
    
    def create_default_admin(self):
        """Crée un utilisateur admin par défaut"""
        try:
            admin_user = self.get_user("admin")
            if not admin_user:
                hashed_password = get_password_hash("admin123")
                self.db.cursor.execute('''
                    INSERT INTO users (username, hashed_password, roles, full_name, email)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    "admin",
                    hashed_password,
                    json.dumps(["admin", "user"]),
                    "Administrateur",
                    "admin@1xbet-api.com"
                ))
                self.db.conn.commit()
                logging.info("✅ Utilisateur admin créé (mot de passe: admin123)")
        except Exception as e:
            logging.error(f"Erreur création admin: {e}")
    
    def get_user(self, username: str) -> Optional[UserInDB]:
        """Récupère un utilisateur par son username"""
        try:
            self.db.cursor.execute(
                "SELECT * FROM users WHERE username = ?", 
                (username,)
            )
            row = self.db.cursor.fetchone()
            if row:
                user_dict = dict(row)
                user_dict["roles"] = json.loads(user_dict.get("roles", "[]"))
                user_dict["settings"] = json.loads(user_dict.get("settings", "{}"))
                return UserInDB(**user_dict)
        except Exception as e:
            logging.error(f"Erreur get_user: {e}")
        return None
    
    def get_user_by_api_key(self, api_key: str) -> Optional[UserInDB]:
        """Récupère un utilisateur par sa clé API"""
        try:
            self.db.cursor.execute(
                "SELECT * FROM users WHERE api_key = ? AND (api_key_expires IS NULL OR api_key_expires > ?)",
                (api_key, datetime.now())
            )
            row = self.db.cursor.fetchone()
            if row:
                user_dict = dict(row)
                user_dict["roles"] = json.loads(user_dict.get("roles", "[]"))
                user_dict["settings"] = json.loads(user_dict.get("settings", "{}"))
                return UserInDB(**user_dict)
        except Exception as e:
            logging.error(f"Erreur get_user_by_api_key: {e}")
        return None
    
    def get_user_by_api_key_hash(self, api_key_hash: str) -> Optional[UserInDB]:
        """Récupère un utilisateur par hash de clé API"""
        try:
            self.db.cursor.execute('''
                SELECT u.* FROM users u
                JOIN api_keys ak ON u.username = ak.user_id
                WHERE ak.key_hash = ? 
                AND ak.revoked = FALSE 
                AND (ak.expires_at IS NULL OR ak.expires_at > ?)
            ''', (api_key_hash, datetime.now()))
            
            row = self.db.cursor.fetchone()
            if row:
                user_dict = dict(row)
                user_dict["roles"] = json.loads(user_dict.get("roles", "[]"))
                user_dict["settings"] = json.loads(user_dict.get("settings", "{}"))
                return UserInDB(**user_dict)
        except Exception as e:
            logging.error(f"Erreur get_user_by_api_key_hash: {e}")
        return None
    
    def create_user(self, user: UserInDB) -> bool:
        """Crée un nouvel utilisateur"""
        try:
            self.db.cursor.execute('''
                INSERT INTO users (username, email, full_name, hashed_password, roles)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                user.username,
                user.email,
                user.full_name,
                user.hashed_password,
                json.dumps(user.roles)
            ))
            self.db.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Erreur create_user: {e}")
            return False
    
    def update_user_last_login(self, username: str):
        """Met à jour la dernière connexion"""
        try:
            self.db.cursor.execute(
                "UPDATE users SET last_login = ? WHERE username = ?",
                (datetime.now(), username)
            )
            self.db.conn.commit()
        except Exception as e:
            logging.error(f"Erreur update_user_last_login: {e}")
    
    def add_login_history(self, user_id: str, ip_address: str = None, 
                         user_agent: str = None, success: bool = True, 
                         method: str = "password"):
        """Ajoute une entrée dans l'historique de connexion"""
        try:
            self.db.cursor.execute('''
                INSERT INTO login_history (user_id, ip_address, user_agent, success, method)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, ip_address, user_agent, success, method))
            self.db.conn.commit()
        except Exception as e:
            logging.error(f"Erreur add_login_history: {e}")
    
    def create_api_key(self, username: str, name: str, 
                      expires_days: Optional[int] = None) -> Optional[str]:
        """Crée une nouvelle clé API"""
        try:
            # Générer la clé
            api_key = generate_api_key()
            api_key_hash = hash_api_key(api_key)
            
            # Calculer la date d'expiration
            expires_at = None
            if expires_days:
                expires_at = datetime.now() + timedelta(days=expires_days)
            
            # Stocker dans la base
            self.db.cursor.execute('''
                INSERT INTO api_keys (user_id, name, key_hash, expires_at)
                VALUES (?, ?, ?, ?)
            ''', (username, name, api_key_hash, expires_at))
            
            self.db.conn.commit()
            return api_key
            
        except Exception as e:
            logging.error(f"Erreur create_api_key: {e}")
            return None
    
    def get_user_api_keys(self, username: str) -> List[dict]:
        """Récupère les clés API d'un utilisateur"""
        try:
            self.db.cursor.execute('''
                SELECT name, created_at, expires_at, last_used, revoked
                FROM api_keys 
                WHERE user_id = ?
                ORDER BY created_at DESC
            ''', (username,))
            
            rows = self.db.cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logging.error(f"Erreur get_user_api_keys: {e}")
            return []
    
    def revoke_api_key(self, username: str, key_hash: str) -> bool:
        """Révoque une clé API"""
        try:
            self.db.cursor.execute('''
                UPDATE api_keys 
                SET revoked = TRUE 
                WHERE user_id = ? AND key_hash = ?
            ''', (username, key_hash))
            
            affected = self.db.cursor.rowcount
            self.db.conn.commit()
            return affected > 0
            
        except Exception as e:
            logging.error(f"Erreur revoke_api_key: {e}")
            return False
    
    def update_api_key_last_used(self, key_hash: str):
        """Met à jour la dernière utilisation d'une clé API"""
        try:
            self.db.cursor.execute(
                "UPDATE api_keys SET last_used = ? WHERE key_hash = ?",
                (datetime.now(), key_hash)
            )
            self.db.conn.commit()
        except Exception as e:
            logging.error(f"Erreur update_api_key_last_used: {e}")
    
    def store_refresh_token(self, username: str, refresh_token: str, 
                           expires_at: datetime) -> bool:
        """Stocke un refresh token"""
        try:
            token_hash = pwd_context.hash(refresh_token)
            self.db.cursor.execute('''
                INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
                VALUES (?, ?, ?)
            ''', (username, token_hash, expires_at))
            
            self.db.conn.commit()
            return True
            
        except Exception as e:
            logging.error(f"Erreur store_refresh_token: {e}")
            return False
    
    def validate_refresh_token(self, username: str, refresh_token: str) -> bool:
        """Valide un refresh token"""
        try:
            self.db.cursor.execute('''
                SELECT token_hash FROM refresh_tokens 
                WHERE user_id = ? AND revoked = FALSE AND expires_at > ?
                ORDER BY created_at DESC LIMIT 1
            ''', (username, datetime.now()))
            
            row = self.db.cursor.fetchone()
            if row and pwd_context.verify(refresh_token, row["token_hash"]):
                return True
        except Exception as e:
            logging.error(f"Erreur validate_refresh_token: {e}")
        return False
    
    def revoke_refresh_tokens(self, username: str):
        """Révoque tous les refresh tokens d'un utilisateur"""
        try:
            self.db.cursor.execute(
                "UPDATE refresh_tokens SET revoked = TRUE WHERE user_id = ?",
                (username,)
            )
            self.db.conn.commit()
        except Exception as e:
            logging.error(f"Erreur revoke_refresh_tokens: {e}")

# ============================================================================
# DEPENDENCIES FASTAPI POUR AUTHENTIFICATION
# ============================================================================

def get_current_user(
    token: str = Depends(oauth2_scheme),
    request: Request = None
) -> UserInDB:
    """Dépendance pour récupérer l'utilisateur courant - VERSION SIMPLIFIÉE"""
    payload = verify_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalide ou expiré",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    username = payload.get("sub")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalide",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user = app.state.user_db.get_user(username)
    if user is None or user.disabled:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Utilisateur non trouvé ou désactivé",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return user


def get_current_active_user(current_user: UserInDB = Depends(get_current_user)) -> UserInDB:
    """Vérifie que l'utilisateur est actif"""
    if current_user.disabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Utilisateur désactivé"
        )
    return current_user

def require_role(role: str):
    """Décorateur pour vérifier les rôles"""
    def role_checker(current_user: UserInDB = Depends(get_current_active_user)):
        if role not in current_user.roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission refusée: rôle {role} requis"
            )
        return current_user
    return role_checker

def require_any_role(roles: List[str]):
    """Décorateur pour vérifier plusieurs rôles"""
    def role_checker(current_user: UserInDB = Depends(get_current_active_user)):
        if not any(role in current_user.roles for role in roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission refusée: un des rôles {roles} requis"
            )
        return current_user
    return role_checker

# ============================================================================
# WEBSOCKET AUTHENTICATION
# ============================================================================

class WebSocketAuthenticator:
    """Gestionnaire d'authentification WebSocket"""
    
    @staticmethod
    async def authenticate_websocket(
        websocket: WebSocket,
        token: Optional[str] = None,
        api_key: Optional[str] = None
    ) -> Optional[UserInDB]:
        """Authentifie une connexion WebSocket"""
        try:
            user = None
            
            # Priorité au token JWT
            if token:
                payload = verify_token(token)
                if payload and payload.get("type") == "access":
                    username = payload.get("sub")
                    user = app.state.user_db.get_user(username)
            
            # Sinon essayer avec clé API
            elif api_key:
                user = app.state.user_db.get_user_by_api_key(api_key)
                if not user:
                    api_key_hash = hash_api_key(api_key)
                    user = app.state.user_db.get_user_by_api_key_hash(api_key_hash)
                    if user:
                        app.state.user_db.update_api_key_last_used(api_key_hash)
            
            if not user or user.disabled:
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
                return None
            
            # Mettre à jour la dernière connexion
            app.state.user_db.update_user_last_login(user.username)
            
            return user
            
        except Exception as e:
            logging.error(f"Erreur authentification WebSocket: {e}")
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
            return None

# ============================================================================
# COLLECTOR AMÉLIORÉ AVEC NOUVEAUX ENDPOINTS
# ============================================================================

class OneXBetAPICollector:
    """Collecteur de données 1xBet avec support complet"""
    
    def __init__(self):
        self.base_url = Config.API_BASE_URL
        self.endpoints = self._initialize_endpoints()
        self.session = requests.Session()
        self.async_client = None
        
    def _initialize_endpoints(self) -> Dict:
        """Initialise les endpoints basés sur la documentation"""
        common_headers = {
            'content-type': 'application/json',
            'accept': 'application/json, text/plain, */*',
            'x-requested-with': 'XMLHttpRequest',
            'is-srv': 'false',
            'x-svc-source': '__BETTING_APP__',
            'x-app-n': '__BETTING_APP__',
        }
        
        # Tokens d'authentification
        x_h_token = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZXYiOjEsImd1aWQiOiJWNEFlNUpDSFI0Kzg1eStwM2p2NjZoZ3hUWjVIY1Rld0QzbXZZY3R2ekIycUlQM0tPVTV1NjRCNDVrMzg2MkUzMndIRDBudFFvSVN2bWNIZTVxWmk3T3RQUGhyTmFnRkhmRm9hRTlhRERROUNrbVYrbnFubTdXeFhyWVFaVE11dTlnNGU1a20xWVpUNDJaUTBOZlU1SFBtTmZNbUw3YlN4WlNLSnNaS3RIUXRLaHUrKzBLcWZRcE80RDU3QitxcGxkR3NkdTlwdHpmZFJFNVJSVXBPb0JkS3BtaG1zNm42SzZUUXova0tOMFEraUlCZlllUHNIdmNjK3h1VDhDSHRPWTBVRXE1VmpDMHRIZ045QkFNVG45ZWVXemV4dW5XMU9Ya2RVV2E1a0xlL1JqcnhXenAzQUYxQ1dHeFZsWGdVK1VVK01vcmNwNDNNNS9HbGd5TmRwQ1MzWkVsSmNzTXcyNTBNTHlodHhURVRkSnRaS3hPc3dkNzBpejJtYS95bDBoSWtKSlNyM2dnajRoUWtjeXdSOERMLzQ0R2JkQnhycTdacGNEaTUrc2dQTmVoMW5ROWYyR3pBTFBYNzZzakJuOWUwS1k4UkxIQnhKcUFnODMyditrUHo3WFlKMFdjUVJpcFRNdXg0QmdzdVRLQmMyZmVuY1NNV2ZJQkZzSThMS2hwUDYyNDVqREYrMG5ZWHc8bFZaNEtRbUUzOTZaN0VqdVoydXU2Mktld0kwOVFQc203THN1NVZLc2Nmd1pWTCtQVE9OcWRnOVdEWHNoUUFkQit6OGJMN2ljZzRhNDN0eFQ2RXZacVJCRVh6c2p0bnBvenJSSFVXZGF2TUJQUHczZ3c9PSIsImV4cCI6MTc2NTQyMTg3NCwiaWF0IjoxNzY1NDA3NDc0fQ._Uf1Au9mh3OD_JZI6MznN-BXcPGQg83-RtuHL6N9CQly5YQHF7FkZVGIU7VX8KBP3R8Pwd5rfKWSPTM58tI8XQ"
        x_hd_token = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0ZXYiOjEsImd1aWQiOiJWNEFlNUpDSFI0Kzg1eStwM2p2NjZoZ3hUWjVIY1Rld0QzbXZZY3R2ekIycUlQM0tPVTV1NjRCNDVrMzg2MkUzMndIRDBudFFvSVN2bWNIZTVxWmk3T3RQUGhyTmFnRkhmRm9hRTlhRERROUNrbVYrbnFubTdXeFhyWVFaVE11dTlnNGU1a20xWVpUNDJaUTBOZlU1SFBtTmZNbUw3YlN4WlNLSnNaS3RIUXRLaHUrKzBLcWZRcE80RDU3QitxcGxkR3NkdTlwdHpmZFJFNVJSVXBPb0JkS3BtaG1zNm42SzZUUXova0tOMFEraUlCZlllUHNIdmNjK3h1VDhDSHRPWTBVRXE1VmpDMHRIZ045QkFNVG45ZWVXemV4dW5XMU9Ya2RVV2E1a0xlL1JqcnhXenAzQUYxQ1dHeFZsWGdVK1VVK01vcmNwNDNNNS9HbGd5TmRwQ1MzWkVsSmNzTXcyNTBNTHlodHhURVRkSnRaS3hPc3dkNzBpejJtYS95bDBoSWtKSlNyM2dnajRoUWtjeXdSOERMLzQ0R2JkQnhycTdacGNEaTUrc2dQTmVoMW5ROWYyR3pBTFBYNzZzakJuOWUwS1k4UkxIQnhKcUFnODMyditrUHo3WFlKMFdjUVJpcFRNdXg0QmdzdVRLQmMyZmVuY1NNV2ZJQkZzSThMS2hwUDYyNDVqREYrMG5ZWHc8bFZaNEtRbUUzOTZaN0VqdVoydXU2Mktld0kwOVFQc203THN1NVZLc2Nmd1pWTCtQVE9OcWRnOVdEWHNoUUFkQit6OGJMN2ljZzRhNDN0eFQ2RXZacVJCRVh6c2p0bnBvenJSSFVXZGF2TUJQUHczZ3c9PSIsImV4cCI6MTc2NTQyMTg3NCwiaWF0IjoxNzY1NDA3NDc0fQ._Uf1Au9mh3OD_JZI6MznN-BXcPGQg83-RtuHL6N9CQly5YQHF7FkZVGIU7VX8KBP3R8Pwd5rfKWSPTM58tI8XQ"
        
        return {
            # Endpoints existants
            'prematch_sports': {
                'path': '/LineFeed/GetSportsShortZip',
                'params': {
                    'lng': 'fr', 'country': '96', 'partner': '286',
                    'virtualSports': 'true', 'gr': '674', 'groupChamps': 'true'
                },
                'headers': {**common_headers, 'x-h': x_h_token},
                'method': 'get_with_params'
            },
            'prematch_odds': {
                'path': '/LineFeed/Get1x2_VZip',
                'params': {
                    'count': '50', 'lng': 'fr', 'mode': '4', 'country': '96',
                    'partner': '286', 'virtualSports': 'true'
                },
                'headers': {
                    **common_headers,
                    'x-hd': x_hd_token,
                    'cache-control': 'public, max-age=10'
                },
                'method': 'get_with_full_url'
            },
            'live_sports': {
                'path': '/LiveFeed/GetSportsShortZip',
                'params': {
                    'lng': 'fr', 'gr': '674', 'country': '96',
                    'partner': '286', 'virtualSports': 'true', 'groupChamps': 'true'
                },
                'headers': {**common_headers, 'x-h': x_h_token},
                'method': 'get_with_params'
            },
            'live_odds': {
                'path': '/LiveFeed/Get1x2_VZip',
                'params': {
                    'count': '50', 'lng': 'fr', 'gr': '674', 'mode': '4',
                    'country': '96', 'partner': '286', 'virtualSports': 'true',
                    'noFilterBlockEvent': 'true'
                },
                'headers': {
                    **common_headers,
                    'x-hd': x_hd_token,
                    'cache-control': 'public, max-age=5'
                },
                'method': 'get_with_full_url'
            },
            
            # NOUVEAU ENDPOINT: Get1x2_VZip avec paramètres spécifiques
            'get1x2_vzip_complete': {
                'path': '/LiveFeed/Get1x2_VZip',
                'params': {
                    'count': '500',
                    'lng': 'fr',
                    'cyberFlag': '4',
                    'partner': '286',
                    'getEmpty': 'true',
                    'altFlag': 'true',
                    'virtualSports': 'true',
                    'noFilterBlockEvent': 'true'
                },
                'headers': {
                    **common_headers,
                    'cache-control': 'public, max-age=5'
                },
                'method': 'get_with_full_url'
            },
            
            # NOUVEAUX ENDPOINTS
            'express_day': {
                'path': '/main-line-feed/v1/expressDay',
                'params': {
                    'cfView': '3',
                    'country': '96',
                    'gr': '674',
                    'lng': 'fr',
                    'ref': '286'
                },
                'headers': common_headers,
                'method': 'get_with_params'
            },
            
            'championships': {
                'path': '/result/web/api/v2/champs',
                'params': {
                    'dateFrom': str(int(time.time()) - 86400),  # 24h avant
                    'dateTo': str(int(time.time()) + 86400),    # 24h après
                    'lng': 'fr',
                    'ref': '286',
                    'sportIds': '1'  # Football par défaut
                },
                'headers': common_headers,
                'method': 'get_with_params'
            },
            
            'top_games': {
                'path': '/LineFeed/GetTopGamesStatZip',
                'params': {
                    'lng': 'fr',
                    'antisports': '66',  # Cricket exclu
                    'partner': '286'
                },
                'headers': common_headers,
                'method': 'get_with_params'
            },
            
            'game_results': {
                'path': '/result/web/api/v3/games',
                'params': {
                    'champId': '',  # À remplir dynamiquement
                    'dateFrom': str(int(time.time()) - 86400),
                    'dateTo': str(int(time.time()) + 86400),
                    'lng': 'fr',
                    'ref': '286'
                },
                'headers': common_headers,
                'method': 'get_with_params'
            },
            
            'sports_list': {
                'path': '/result/web/api/v2/sports',
                'params': {
                    'cyberFlag': '4',  # Inclure esports
                    'dateFrom': str(int(time.time()) - 86400),
                    'dateTo': str(int(time.time()) + 86400),
                    'lng': 'fr',
                    'ref': '286'
                },
                'headers': common_headers,
                'method': 'get_with_params'
            },
            
            'sports_simple': {
                'path': '/LineFeed/GetSportsShortZip',
                'params': {
                    'lng': 'fr', 'country': '96', 'partner': '286',
                    'virtualSports': 'true', 'gr': '674', 'groupChamps': 'true'
                },
                'headers': {**common_headers, 'x-h': x_h_token},
                'method': 'get_with_params'
            },
            
            # AJOUTER CES ENDPOINTS MANQUANTS
            'championships_v2': {
                'path': '/result/web/api/v2/champs',
                'params': {
                    'lng': 'fr',
                    'ref': '286',
                    'sportIds': '1'
                },
                'headers': common_headers,
                'method': 'get_with_params'
            },
            
            'game_results_v3': {
                'path': '/result/web/api/v3/games',
                'params': {
                    'lng': 'fr',
                    'ref': '286'
                },
                'headers': common_headers,
                'method': 'get_with_params'
            }
        }
    
    def _build_full_url(self, endpoint_name: str) -> str:
        """Construit l'URL complète avec paramètres"""
        endpoint = self.endpoints[endpoint_name]
        url = f"{self.base_url}{endpoint['path']}"
        
        if endpoint.get('method') == 'get_with_full_url':
            params_str = '&'.join([f"{k}={v}" for k, v in endpoint['params'].items()])
            return f"{url}?{params_str}"
        
        return url
    
    async def fetch_endpoint_async(self, endpoint_name: str) -> Optional[Dict]:
        """Récupère les données d'un endpoint (asynchrone)"""
        if self.async_client is None:
            self.async_client = httpx.AsyncClient(timeout=Config.REQUEST_TIMEOUT)
        
        if endpoint_name not in self.endpoints:
            return None
        
        endpoint = self.endpoints[endpoint_name]
        
        try:
            if endpoint['method'] == 'get_with_full_url':
                url = self._build_full_url(endpoint_name)
                response = await self.async_client.get(url, headers=endpoint['headers'])
            else:
                url = f"{self.base_url}{endpoint['path']}"
                response = await self.async_client.get(
                    url,
                    headers=endpoint['headers'],
                    params=endpoint['params']
                )
            
            if response.status_code == 200:
                return response.json()
                
        except Exception as e:
            logging.error(f"Async error {endpoint_name}: {e}")
        
        return None

# ============================================================================
# CACHE MANAGER
# ============================================================================

class CacheManager:
    """Gestionnaire de cache Redis adapté pour Railway"""
    
    def __init__(self):
        self.redis = None
        
    async def connect(self):
        """Connexion à Redis avec support Railway"""
        if not Config.REDIS_URL and not Config.REDIS_HOST:
            logging.info("🔶 Redis désactivé - mode sans cache")
            self.redis = None
            return
        
        try:
            connection_params = {}
            
            # Utiliser REDIS_URL si disponible (standard Railway)
            if Config.REDIS_URL:
                self.redis = await redis.from_url(
                    Config.REDIS_URL,
                    decode_responses=True,
                    socket_connect_timeout=5
                )
            else:
                # Fallback aux paramètres individuels
                connection_params = {
                    "host": Config.REDIS_HOST,
                    "port": Config.REDIS_PORT,
                    "decode_responses": True,
                    "socket_connect_timeout": 5
                }
                
                if Config.REDIS_PASSWORD:
                    connection_params["password"] = Config.REDIS_PASSWORD
                
                self.redis = await redis.Redis(**connection_params)
            
            await self.redis.ping()
            logging.info("✅ Connecté à Redis (Railway)")
            
        except Exception as e:
            logging.error(f"❌ Erreur connexion Redis: {e}")
            logging.info("🔶 Continuation en mode sans cache")
            self.redis = None
    
    async def set(self, key: str, data: Any, ttl: int = None) -> bool:
        """Stocke des données dans le cache"""
        if not self.redis:
            return False
        
        try:
            def datetime_serializer(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")
            
            json_data = json.dumps(data, default=datetime_serializer)
            if ttl:
                await self.redis.setex(key, ttl, json_data)
            else:
                await self.redis.set(key, json_data)
            return True
        except Exception as e:
            logging.error(f"Erreur cache set {key}: {e}")
            return False
    
    async def get(self, key: str) -> Optional[Any]:
        """Récupère des données du cache"""
        if not self.redis:
            return None
        
        try:
            data = await self.redis.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logging.error(f"Erreur cache get {key}: {e}")
        
        return None
    
    async def publish(self, channel: str, message: Dict):
        """Publie un message sur un canal Redis"""
        if not self.redis:
            return
        
        try:
            def datetime_serializer(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")
            
            await self.redis.publish(channel, json.dumps(message, default=datetime_serializer))
        except Exception as e:
            logging.error(f"Erreur publish {channel}: {e}")
    
    async def subscribe(self, channel: str):
        """S'abonne à un canal Redis"""
        if not self.redis:
            return None
        
        try:
            pubsub = self.redis.pubsub()
            await pubsub.subscribe(channel)
            return pubsub
        except Exception as e:
            logging.error(f"Erreur subscribe {channel}: {e}")
            return None

# ============================================================================
# DATABASE MANAGER AMÉLIORÉ
# ============================================================================

class DatabaseManager:
    """Gestionnaire de base de données SQLite amélioré"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connexion à SQLite"""
        try:
            os.makedirs(os.path.dirname(Config.SQLITE_DB_PATH), exist_ok=True)
            self.conn = sqlite3.connect(
                Config.SQLITE_DB_PATH,
                check_same_thread=False,
                timeout=30.0
            )
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            
            self._create_tables()
            logging.info(f"✅ Connecté à SQLite: {Config.SQLITE_DB_PATH}")
        except Exception as e:
            logging.error(f"❌ Erreur connexion SQLite: {e}")
            self.conn = None
            self.cursor = None
    
    def _create_tables(self):
        """Crée toutes les tables nécessaires"""
        try:
            # Table des sports
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sports (
                    sport_id INTEGER PRIMARY KEY,
                    name_fr TEXT,
                    name_en TEXT,
                    name_ru TEXT,
                    category_id INTEGER,
                    competition_count INTEGER DEFAULT 0,
                    event_count INTEGER DEFAULT 0,
                    live_event_count INTEGER DEFAULT 0,
                    is_virtual BOOLEAN DEFAULT FALSE,
                    image_type INTEGER,
                    special_market INTEGER,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Table des événements
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY,
                    secondary_id INTEGER,
                    sport_id INTEGER,
                    sport_name_fr TEXT,
                    sport_name_en TEXT,
                    sport_name_ru TEXT,
                    sport_category INTEGER,
                    sport_icon TEXT,
                    competition_id INTEGER,
                    competition_name_fr TEXT,
                    competition_name_en TEXT,
                    competition_name_ru TEXT,
                    league_id INTEGER,
                    country_id INTEGER,
                    country_name_fr TEXT,
                    country_name_en TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    start_time TIMESTAMP,
                    status INTEGER,
                    substatus INTEGER,
                    odds_type INTEGER,
                    main_markets TEXT,
                    additional_markets TEXT,
                    total_markets INTEGER DEFAULT 0,
                    live_score TEXT,
                    win_probabilities TEXT,
                    match_info TEXT,
                    metadata TEXT,
                    special_markets TEXT,
                    is_virtual BOOLEAN DEFAULT FALSE,
                    is_available BOOLEAN DEFAULT FALSE,
                    is_highlighted BOOLEAN DEFAULT FALSE,
                    stream_id INTEGER,
                    last_update TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Table de l'historique des cotes
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS odds_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    main_odds TEXT,
                    additional_odds TEXT,
                    live_score TEXT,
                    status INTEGER,
                    total_markets INTEGER,
                    odds_type INTEGER
                )
            ''')
            
            # Table de l'historique des scores
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS score_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    score_data TEXT,
                    period INTEGER,
                    elapsed_time INTEGER,
                    remaining_time INTEGER
                )
            ''')
            
            # Table d'analyse
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    endpoint TEXT,
                    status TEXT,
                    count INTEGER,
                    success BOOLEAN,
                    error_code INTEGER,
                    error_msg TEXT,
                    categories TEXT
                )
            ''')
            
            # Index
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_events_sport_id ON events(sport_id)",
                "CREATE INDEX IF NOT EXISTS idx_events_status ON events(status)",
                "CREATE INDEX IF NOT EXISTS idx_events_start_time ON events(start_time)",
                "CREATE INDEX IF NOT EXISTS idx_events_competition_id ON events(competition_id)",
                "CREATE INDEX IF NOT EXISTS idx_odds_history_event_id ON odds_history(event_id)",
                "CREATE INDEX IF NOT EXISTS idx_odds_history_timestamp ON odds_history(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_score_history_event_id ON score_history(event_id)",
                "CREATE INDEX IF NOT EXISTS idx_score_history_timestamp ON score_history(timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_analysis_timestamp ON analysis(timestamp)"
            ]
            
            for idx in indexes:
                self.cursor.execute(idx)
            
            self.conn.commit()
            logging.info("✅ Tables SQLite créées")
            
        except Exception as e:
            logging.error(f"Erreur création tables SQLite: {e}")
            if self.conn:
                self.conn.rollback()
    
    def _dict_to_json(self, data):
        """Convertit un dict en JSON string"""
        if data is None:
            return "{}"
        if isinstance(data, (list, dict)):
            try:
                return json.dumps(data, default=str, ensure_ascii=False)
            except:
                return "{}"
        if isinstance(data, str):
            try:
                json.loads(data)
                return data
            except:
                return json.dumps({"value": data}, ensure_ascii=False)
        return json.dumps({"value": str(data)}, ensure_ascii=False)
    
    def _extract_string(self, value):
        """Extrait une string d'une valeur qui peut être dict ou autre"""
        if isinstance(value, dict):
            return value.get("name", value.get("value", str(value)))
        return str(value)
    
    def _extract_timestamp(self, value):
        """Extrait un timestamp ISO"""
        if isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(value).isoformat()
            except:
                return datetime.now().isoformat()
        elif isinstance(value, str):
            return value
        else:
            return datetime.now().isoformat()
    
    async def store_event(self, event_data: Dict) -> bool:
        """Stocke un événement dans SQLite - VERSION CORRIGÉE"""
        if not self.conn:
            return False
        
        try:
            event_id = event_data.get("I") or event_data.get("event_id") or int(time.time() * 1000)
            
            # Préparer les données
            fields = {
                "event_id": event_id,
                "secondary_id": str(event_data.get("N", event_data.get("secondary_id", ""))),
                "sport_id": str(event_data.get("SI", event_data.get("sport_id", ""))),
                "sport_name_fr": str(event_data.get("SN", event_data.get("sport_name_fr", ""))).strip(),
                "sport_name_en": str(event_data.get("SE", event_data.get("sport_name_en", ""))),
                "sport_name_ru": str(event_data.get("SR", event_data.get("sport_name_ru", ""))),
                "sport_category": str(event_data.get("CID", event_data.get("sport_category", ""))),
                "sport_icon": str(event_data.get("SIMG", event_data.get("sport_icon", ""))),
                "competition_id": str(event_data.get("CI", event_data.get("competition_id", ""))),
                "competition_name_fr": self._extract_string(event_data.get("L", event_data.get("competition_name_fr", ""))),
                "competition_name_en": str(event_data.get("LE", event_data.get("competition_name_en", ""))),
                "competition_name_ru": str(event_data.get("LR", event_data.get("competition_name_ru", ""))),
                "league_id": str(event_data.get("LI", event_data.get("league_id", ""))),
                "country_id": str(event_data.get("COI", event_data.get("country_id", ""))),
                "country_name_fr": str(event_data.get("CN", event_data.get("country_name_fr", ""))),
                "country_name_en": str(event_data.get("CE", event_data.get("country_name_en", ""))),
                "home_team": self._dict_to_json(event_data.get("O1", event_data.get("home_team", {}))),
                "away_team": self._dict_to_json(event_data.get("O2", event_data.get("away_team", {}))),
                "start_time": self._extract_timestamp(event_data.get("S", event_data.get("start_time"))),
                "status": str(event_data.get("SS", event_data.get("status", 0))),
                "substatus": str(event_data.get("SST", event_data.get("substatus", 1))),
                "odds_type": str(event_data.get("KI", event_data.get("odds_type", 3))),
                "main_markets": self._dict_to_json(event_data.get("E", event_data.get("main_markets", []))),
                "additional_markets": self._dict_to_json(event_data.get("AE", event_data.get("additional_markets", []))),
                "total_markets": str(event_data.get("EC", event_data.get("total_markets", 0))),
                "live_score": self._dict_to_json(event_data.get("SC", event_data.get("live_score", {}))),
                "win_probabilities": self._dict_to_json(event_data.get("WP", event_data.get("win_probabilities", {}))),
                "match_info": self._dict_to_json(event_data.get("MIO", event_data.get("match_info", {}))),
                "metadata": self._dict_to_json(event_data.get("MIS", event_data.get("metadata", []))),
                "special_markets": self._dict_to_json(event_data.get("MS", event_data.get("special_markets", []))),
                "is_virtual": "1" if event_data.get("ICY", event_data.get("is_virtual", False)) else "0",
                "is_available": "1" if event_data.get("SUBA", event_data.get("is_available", False)) else "0",
                "is_highlighted": "1" if event_data.get("HL", event_data.get("is_highlighted", False)) else "0",
                "stream_id": str(event_data.get("SmI", event_data.get("stream_id", ""))),
                "last_update": self._extract_timestamp(event_data.get("U", event_data.get("last_update")))
            }
            
            # Construire la requête
            columns = ", ".join(fields.keys())
            placeholders = ", ".join(["?"] * len(fields))
            values = list(fields.values())
            
            # Utiliser UN SEUL curseur local
            cursor = self.conn.cursor()
            cursor.execute(f'''
                INSERT OR REPLACE INTO events ({columns})
                VALUES ({placeholders})
            ''', values)
            
            self.conn.commit()
            cursor.close()  # Fermer le curseur
            return True
            
        except Exception as e:
            logging.error(f"Erreur store_event SQLite: {e}")
            # Assurez-vous que le curseur est fermé en cas d'erreur
            if 'cursor' in locals():
                cursor.close()
            return False
    
    async def store_analysis(self, analysis_data: Dict) -> bool:
        """Stocke les données d'analyse"""
        if not self.conn:
            return False
        
        try:
            self.cursor.execute('''
                INSERT INTO analysis (
                    endpoint, status, count, success, error_code, error_msg, categories
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                analysis_data.get("endpoint"),
                analysis_data.get("status"),
                analysis_data.get("count", 0),
                analysis_data.get("success", False),
                analysis_data.get("error_code", 0),
                analysis_data.get("error_msg", ""),
                json.dumps(analysis_data.get("categories", {}), ensure_ascii=False)
            ))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logging.error(f"Erreur store_analysis SQLite: {e}")
            if 'cursor' in locals():
                cursor.close()
            return False

# ============================================================================
# DATA PROCESSOR COMPLET AVEC NOUVELLES MÉTHODES
# ============================================================================

class DataProcessor:
    """Processeur de données COMPLET avec toutes les fonctionnalités"""
    
    @staticmethod
    def normalize_sport(sport_data: Dict) -> Dict:
        """Normalise les données d'un sport"""
        try:
            cleaned_data = {k: v for k, v in sport_data.items() if v is not None}
            return SportSchema(**cleaned_data).dict(exclude_none=True, by_alias=True)
        except Exception as e:
            logging.error(f"Erreur normalisation sport: {e}")
            return {k: v for k, v in sport_data.items() if v is not None}
    
    @staticmethod
    def normalize_event(event_data: Dict) -> Optional[Dict]:
        """Normalise COMPLÈTEMENT les données d'un événement"""
        try:
            # Nettoyer les données
            cleaned_data = {}
            for k, v in event_data.items():
                if v is not None:
                    cleaned_data[k] = v
            
            # Convertir les timestamps
            if "S" in cleaned_data and isinstance(cleaned_data["S"], (int, float)):
                try:
                    cleaned_data["S"] = datetime.fromtimestamp(cleaned_data["S"])
                except:
                    cleaned_data["S"] = datetime.now()
            
            if "U" in cleaned_data and isinstance(cleaned_data["U"], (int, float)):
                try:
                    cleaned_data["U"] = datetime.fromtimestamp(cleaned_data["U"])
                except:
                    cleaned_data["U"] = datetime.now()
            
            # Gérer les équipes
            for field in ["O1", "O2"]:
                if field in cleaned_data:
                    if isinstance(cleaned_data[field], str):
                        cleaned_data[field] = {"name": cleaned_data[field]}
                    elif not isinstance(cleaned_data[field], dict):
                        cleaned_data[field] = {}
            
            # Gérer la compétition
            if "L" in cleaned_data and isinstance(cleaned_data["L"], dict):
                cleaned_data["L"] = cleaned_data["L"].get("name", str(cleaned_data["L"]))
            
            # Valider avec le schéma
            validated = EventSchema(**cleaned_data)
            result = validated.dict(exclude_none=True, by_alias=True)
            
            if "I" not in result:
                return None
                
            return result
            
        except Exception as e:
            logging.error(f"Erreur normalisation événement: {e}")
            # Version simplifiée
            simplified = {k: v for k, v in event_data.items() if v is not None}
            if "I" not in simplified:
                simplified["I"] = int(time.time() * 1000)
            return simplified
    
    @staticmethod
    def extract_main_odds(event_data: Dict) -> Dict:
        """Extrait les cotes principales d'un événement"""
        odds = {
            "result": None,
            "handicaps": [],
            "totals": [],
            "double_chance": None
        }
        
        main_markets = event_data.get("E", [])
        if not main_markets:
            main_markets = event_data.get("main_markets", [])
        
        for market in main_markets:
            if not isinstance(market, dict):
                continue
            
            group = market.get("G")
            market_type = market.get("T")
            odds_value = market.get("C")
            handicap = market.get("P")
            
            # Résultat final (G=1)
            if group == 1:
                if odds.get("result") is None:
                    odds["result"] = {}
                
                if market_type == 1:
                    odds["result"]["home"] = odds_value
                elif market_type == 2:
                    odds["result"]["draw"] = odds_value
                elif market_type == 3:
                    odds["result"]["away"] = odds_value
            
            # Handicap asiatique (G=2)
            elif group == 2 and handicap is not None:
                handicap_data = {
                    "value": handicap,
                    "home": odds_value if market_type == 7 else None,
                    "away": odds_value if market_type == 8 else None
                }
                odds["handicaps"].append(handicap_data)
            
            # Total (G=15)
            elif group == 15 and handicap is not None:
                total_data = {
                    "value": handicap,
                    "over": odds_value if market_type == 11 else None,
                    "under": odds_value if market_type == 12 else None
                }
                odds["totals"].append(total_data)
        
        return odds
    
    @staticmethod
    def parse_period_scores(ps_data: List[Dict]) -> List[Dict]:
        """Parse la structure PS complète"""
        period_scores = []
        
        for period in ps_data:
            if isinstance(period, dict):
                period_num = period.get("Key")
                period_value = period.get("Value")
                
                if isinstance(period_value, dict):
                    period_scores.append({
                        "period": period_num,
                        "name_fr": period_value.get("NF", ""),
                        "name_en": period_value.get("NE", ""),
                        "name_ru": period_value.get("NR", ""),
                        "home_score": period_value.get("S1", 0),
                        "away_score": period_value.get("S2", 0)
                    })
        
        return period_scores
    
    @staticmethod
    def parse_statistics(st_data: List[Dict]) -> Dict[str, List]:
        """Structure les statistiques par période"""
        stats_by_period = {}
        
        for stat_group in st_data:
            if isinstance(stat_group, dict):
                period = stat_group.get("Key", 0)  # 0=global
                stats_list = stat_group.get("Value", [])
                
                if isinstance(stats_list, list):
                    parsed_stats = []
                    for stat in stats_list:
                        if isinstance(stat, dict):
                            parsed_stats.append({
                                "id": stat.get("ID"),
                                "name_fr": stat.get("N", ""),
                                "name_en": stat.get("E", ""),
                                "name_ru": stat.get("R", ""),
                                "home_value": stat.get("S1", "0"),
                                "away_value": stat.get("S2", "0")
                            })
                    
                    stats_by_period[str(period)] = parsed_stats
        
        return stats_by_period
    
    @staticmethod
    def parse_metadata(mis_data: List[Dict]) -> Dict[str, str]:
        """Parse les métadonnées MIS en dict organisé"""
        metadata = {}
        
        if not mis_data:
            return metadata
        
        for item in mis_data:
            if isinstance(item, dict):
                key = item.get("K")
                value = item.get("V")
                
                if key is not None and value is not None:
                    try:
                        key_name = MetadataKey(key).name if key in [e.value for e in MetadataKey] else f"key_{key}"
                        metadata[key_name] = value
                    except:
                        metadata[str(key)] = value
        
        return metadata
    
    @staticmethod
    def calculate_implied_probabilities(wp_data: Dict) -> Dict:
        """Calcule les cotes implicites depuis WP"""
        if not wp_data:
            return {}
        
        implied_odds = {}
        for key, prob in wp_data.items():
            if isinstance(prob, (int, float)) and prob > 0:
                implied_odds[key] = 1 / prob
            else:
                implied_odds[key] = None
        
        return implied_odds
    
    @staticmethod
    def extract_additional_markets(ae_data: List[Dict]) -> List[Dict]:
        """Extrait les marchés additionnels structurés"""
        markets = []
        
        if not ae_data:
            return markets
        
        for group in ae_data:
            if isinstance(group, dict):
                market_group = {
                    "group": group.get("G"),
                    "name_fr": group.get("N", ""),
                    "name_en": group.get("E", ""),
                    "markets": []
                }
                
                group_markets = group.get("ME", [])
                if isinstance(group_markets, list):
                    for market in group_markets:
                        if isinstance(market, dict):
                            market_group["markets"].append({
                                "odds": market.get("C"),
                                "group": market.get("G"),
                                "type": market.get("T"),
                                "handicap": market.get("P")
                            })
                
                markets.append(market_group)
        
        return markets
    
    @staticmethod
    def calculate_odds_movement(old_odds: List[Dict], new_odds: List[Dict]) -> Dict:
        """Calcule le mouvement des cotes"""
        movement = {"changes": [], "volatility": 0}
        
        old_dict = {}
        for odd in old_odds:
            key = f"{odd.get('G')}_{odd.get('T')}_{odd.get('P', '')}"
            old_dict[key] = odd.get("C")
        
        changes = []
        for new_odd in new_odds:
            key = f"{new_odd.get('G')}_{new_odd.get('T')}_{new_odd.get('P', '')}"
            new_value = new_odd.get("C")
            old_value = old_dict.get(key)
            
            if old_value and new_value:
                change = new_value - old_value
                percent_change = (change / old_value) * 100
                
                if abs(change) > 0.01:
                    changes.append({
                        "key": key,
                        "old": old_value,
                        "new": new_value,
                        "change": change,
                        "percent_change": percent_change,
                        "direction": "up" if change > 0 else "down"
                    })
        
        movement["changes"] = changes
        movement["count"] = len(changes)
        
        if changes:
            avg_change = sum(abs(c["percent_change"]) for c in changes) / len(changes)
            movement["volatility"] = avg_change
        
        return movement
    
    @staticmethod
    def format_odds_display(odds: float, odds_type: int = 3, locale: str = 'fr-FR') -> str:
        """Formate les cotes pour l'affichage"""
        if odds_type == 5:  # Fractionnaire
            return DataProcessor.decimal_to_fraction(odds)
        elif odds_type == 7:  # Américain
            if odds >= 2.0:
                return f"+{int((odds - 1) * 100)}"
            else:
                return f"-{int(100 / (odds - 1))}"
        else:  # Décimal (par défaut)
            formatter = f"{{:.{3 if odds < 10 else 2}f}}"
            return formatter.format(odds)
    
    @staticmethod
    def decimal_to_fraction(decimal_odds: float) -> str:
        """Convertit des cotes décimales en fractionnaires"""
        for denom in range(1, 101):
            numer = int(decimal_odds * denom)
            if abs(numer/denom - decimal_odds) < 0.01:
                return f"{numer}/{denom}"
        return f"{decimal_odds:.2f}"
    
    @staticmethod
    def validate_event_completeness(event_data: Dict) -> Tuple[bool, List[str]]:
        """Valide si l'événement a tous les champs nécessaires"""
        required_fields = ['I', 'S', 'SI', 'O1', 'O2', 'SS']
        missing_fields = []
        
        for field in required_fields:
            if field not in event_data or event_data[field] is None:
                missing_fields.append(field)
        
        is_complete = len(missing_fields) == 0
        
        # Validation supplémentaire
        warnings = []
        if event_data.get("SS") == 3 and not event_data.get("SC"):
            warnings.append("Événement LIVE sans données SC")
        
        if event_data.get("E") and len(event_data.get("E", [])) == 0:
            warnings.append("Aucun marché principal disponible")
        
        return is_complete, missing_fields + warnings
    
    @staticmethod
    def get_status_text(status: int, live_score: Optional[Dict] = None) -> str:
        """Retourne le texte du statut"""
        status_map = {
            1: "Terminé",
            2: "À venir",
            3: live_score.get("SLS", "En cours") if live_score else "En cours",
            4: "Reporté",
            5: "Annulé",
            6: "Suspendu",
            7: "Mi-temps",
            8: "Abandonné",
            9: "Retardé"
        }
        return status_map.get(status, "Statut inconnu")
    
    # NOUVELLES MÉTHODES POUR LES NOUVEAUX ENDPOINTS
    @staticmethod
    def process_express_day_data(data: List[Dict]) -> Dict:
        """Traite les données des paris express du jour"""
        if not data:
            return {"count": 0, "bets": []}
        
        processed_bets = []
        total_odds = 0
        
        for bet in data:
            if isinstance(bet, dict):
                # Analyser les événements du pari
                events = bet.get("events", [])
                sport_distribution = {}
                
                for event in events:
                    if isinstance(event, dict):
                        sport_name = event.get("sport", {}).get("name", "Inconnu")
                        sport_distribution[sport_name] = sport_distribution.get(sport_name, 0) + 1
                
                processed_bet = {
                    "express_id": bet.get("expressId"),
                    "total_odds": bet.get("cf", 1.0),
                    "total_events": len(events),
                    "sport_distribution": sport_distribution,
                    "has_special_event": any(
                        e.get("event", {}).get("type") == 707 
                        for e in events if isinstance(e, dict)
                    )
                }
                
                processed_bets.append(processed_bet)
                total_odds += bet.get("cf", 1.0)
        
        return {
            "count": len(processed_bets),
            "average_odds": total_odds / len(processed_bets) if processed_bets else 0,
            "total_events": sum(bet["total_events"] for bet in processed_bets),
            "bets": processed_bets
        }
    
    @staticmethod
    def process_championship_data(data: Dict) -> Dict:
        """Traite les données des championnats"""
        if not data:
            return {"count": 0, "championships": []}
        
        items = data.get("items", [])
        processed_champs = []
        
        for champ in items:
            if isinstance(champ, dict):
                processed_champs.append({
                    "id": champ.get("id"),
                    "name": champ.get("name", ""),
                    "sport_id": champ.get("sportId"),
                    "game_count": champ.get("gamesCount", 0),
                    "has_image": bool(champ.get("image"))
                })
        
        # Trier par nombre de matchs
        processed_champs.sort(key=lambda x: x["game_count"], reverse=True)
        
        return {
            "count": len(processed_champs),
            "total_games": sum(c["game_count"] for c in processed_champs),
            "championships": processed_champs
        }
    
    @staticmethod
    def process_top_games_data(data: List[Dict]) -> Dict:
        """Traite les données des matchs du top"""
        if not data:
            return {"count": 0, "games": []}
        
        processed_games = []
        
        for game in data:
            if isinstance(game, dict):
                # Analyser les marchés disponibles
                markets = game.get("E", [])
                market_types = set()
                
                for market in markets:
                    if isinstance(market, dict):
                        market_types.add(market.get("G"))
                
                processed_games.append({
                    "game_id": game.get("I"),
                    "home_team": game.get("O1"),
                    "away_team": game.get("O2"),
                    "sport_id": game.get("SI"),
                    "competition_id": game.get("CI"),
                    "start_time": game.get("S"),
                    "market_count": len(markets),
                    "market_types": list(market_types),
                    "odds_type": game.get("KI")
                })
        
        return {
            "count": len(processed_games),
            "games": processed_games,
            "market_statistics": {
                "average_markets": len(processed_games) > 0 and sum(g["market_count"] for g in processed_games) / len(processed_games) or 0,
                "unique_market_types": len(set(mt for g in processed_games for mt in g["market_types"]))
            }
        }

# ============================================================================
# MONITORING ET ALERTES
# ============================================================================

class MonitoringSystem:
    """Système de monitoring et alertes"""
    
    def __init__(self):
        self.metrics_history = []
        self.alert_rules = self._initialize_alert_rules()
    
    def _initialize_alert_rules(self) -> List[AlertRule]:
        """Initialise les règles d'alerte"""
        return [
            AlertRule(
                condition="data_freshness > 300",
                severity="warning",
                message="Données potentiellement obsolètes",
                threshold=300
            ),
            AlertRule(
                condition="odds_completeness < 80",
                severity="info",
                message="Cotes incomplètes pour cet événement",
                threshold=80
            ),
            AlertRule(
                condition="status == 3 and not live_score",
                severity="error",
                message="Incohérence de statut détectée"
            ),
            AlertRule(
                condition="update_frequency > 60",
                severity="warning",
                message="Fréquence de mise à jour faible",
                threshold=60
            )
        ]
    
    def check_metrics(self, metrics: MonitoringMetrics) -> List[Dict]:
        """Vérifie les métriques contre les règles d'alerte"""
        alerts = []
        
        for rule in self.alert_rules:
            alert_triggered = False
            
            if rule.condition == "data_freshness > 300" and metrics.data_freshness:
                if metrics.data_freshness > rule.threshold:
                    alert_triggered = True
            
            elif rule.condition == "odds_completeness < 80" and metrics.odds_completeness:
                if metrics.odds_completeness < rule.threshold:
                    alert_triggered = True
            
            elif rule.condition == "update_frequency > 60" and metrics.update_frequency:
                if metrics.update_frequency > rule.threshold:
                    alert_triggered = True
            
            if alert_triggered:
                alerts.append({
                    "severity": rule.severity,
                    "message": rule.message,
                    "timestamp": datetime.now().isoformat(),
                    "rule": rule.condition
                })
        
        return alerts

# ============================================================================
# APP STATE
# ============================================================================

@dataclass
class AppState:
    """État de l'application"""
    collector: OneXBetAPICollector = None
    cache: CacheManager = None
    db: DatabaseManager = None
    processor: DataProcessor = None
    monitor: MonitoringSystem = None
    user_db: UserDatabase = None
    collection_task: asyncio.Task = None
    websocket_connections: Dict[str, List[WebSocket]] = None
    service_manager: ServiceManager = None
    
    def __post_init__(self):
        self.websocket_connections = {}

# ============================================================================
# BACKGROUND TASKS
# ============================================================================

async def periodic_collection_task(state: AppState):
    """Tâche périodique de collecte des données"""
    logging.info("🔄 Démarrage de la collecte périodique")
    
    while True:
        try:
            await collect_and_process_data(state)
            await asyncio.sleep(Config.COLLECTION_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Erreur dans la collecte périodique: {e}")
            await asyncio.sleep(30)

async def collect_and_process_data(state: AppState):
    """Collecte et traite les données"""
    analysis_data = {
        "timestamp": datetime.now(),
        "endpoints": {},
        "totals": {
            "total_endpoints": 0,
            "successful_endpoints": 0,
            "total_events": 0,
            "total_sports": 0,
            "failed_endpoints": 0
        }
    }
    
    try:
        # Collecte des données live
        live_data = await state.collector.fetch_endpoint_async("live_odds")
        analysis_data["endpoints"]["live_odds"] = {
            "status": "success" if live_data and live_data.get("Success") else "error",
            "count": len(live_data.get("Value", [])) if live_data else 0,
            "success": live_data.get("Success", False) if live_data else False,
            "error_code": live_data.get("ErrorCode", 0) if live_data else 500,
            "error_msg": live_data.get("Error", "") if live_data else "No data"
        }
        
        if live_data and live_data.get("Value"):
            events = live_data["Value"]
            analysis_data["totals"]["total_events"] += len(events)
            
            for event_data in events:
                await process_single_event(state, event_data)
        
        # Collecte des données prematch
        prematch_data = await state.collector.fetch_endpoint_async("prematch_odds")
        analysis_data["endpoints"]["prematch_odds"] = {
            "status": "success" if prematch_data and prematch_data.get("Success") else "error",
            "count": len(prematch_data.get("Value", [])) if prematch_data else 0,
            "success": prematch_data.get("Success", False) if prematch_data else False,
            "error_code": prematch_data.get("ErrorCode", 0) if prematch_data else 500,
            "error_msg": prematch_data.get("Error", "") if prematch_data else "No data"
        }
        
        if prematch_data and prematch_data.get("Value"):
            events = prematch_data["Value"]
            analysis_data["totals"]["total_events"] += len(events)
            
            for event_data in events:
                await process_single_event(state, event_data)
        
        # NOUVELLE COLLECTE: Endpoint Get1x2_VZip
        get1x2_data = await state.collector.fetch_endpoint_async("get1x2_vzip_complete")
        analysis_data["endpoints"]["get1x2_vzip"] = {
            "status": "success" if get1x2_data and get1x2_data.get("Success") else "error",
            "count": len(get1x2_data.get("Value", [])) if get1x2_data else 0,
            "success": get1x2_data.get("Success", False) if get1x2_data else False,
            "error_code": get1x2_data.get("ErrorCode", 0) if get1x2_data else 500,
            "error_msg": get1x2_data.get("Error", "") if get1x2_data else "No data"
        }
        
        if get1x2_data and get1x2_data.get("Value"):
            events = get1x2_data["Value"]
            analysis_data["totals"]["total_events"] += len(events)
            
            for event_data in events:
                await process_single_event(state, event_data)
        
        # Collecte des sports
        for sport_type in ["live_sports", "prematch_sports"]:
            sports_data = await state.collector.fetch_endpoint_async(sport_type)
            analysis_data["endpoints"][sport_type] = {
                "status": "success" if sports_data and sports_data.get("Success") else "error",
                "count": len(sports_data.get("Value", [])) if sports_data else 0,
                "success": sports_data.get("Success", False) if sports_data else False,
                "error_code": sports_data.get("ErrorCode", 0) if sports_data else 500,
                "error_msg": sports_data.get("Error", "") if sports_data else "No data"
            }
            
            if sports_data and sports_data.get("Value"):
                sports = sports_data["Value"]
                analysis_data["totals"]["total_sports"] += len(sports)
                
                normalized_sports = [state.processor.normalize_sport(s) for s in sports]
                await state.cache.set(
                    f"sports:{sport_type}",
                    normalized_sports,
                    ttl=Config.CACHE_TTL['sports']
                )
        
        new_endpoints = ["express_day", "top_games", "sports_simple"]
        
        for endpoint_name in new_endpoints:
            try:
                endpoint_data = await state.collector.fetch_endpoint_async(endpoint_name)
                
                analysis_data["endpoints"][endpoint_name] = {
                    "status": "success" if endpoint_data else "error",
                    "count": len(endpoint_data.get("Value", [])) if endpoint_data and isinstance(endpoint_data, dict) else 0,
                    "success": endpoint_data is not None,
                    "error_code": 0 if endpoint_data else 500,
                    "error_msg": "" if endpoint_data else "No data"
                }
                
                # Stocker en cache
                if endpoint_data:
                    cache_key = f"{endpoint_name}:latest"
                    await state.cache.set(cache_key, endpoint_data, ttl=Config.CACHE_TTL.get(endpoint_name, 300))
                    
            except Exception as e:
                analysis_data["endpoints"][endpoint_name] = {
                    "status": "error",
                    "count": 0,
                    "success": False,
                    "error_code": 500,
                    "error_msg": str(e)
                }
        
        
        # NOUVELLE COLLECTE: Endpoints supplémentaires
        additional_endpoints = [
            "express_day",
            "top_games",
            # "championships",  # Besoin de paramètres spécifiques
            # "sports_list",    # Besoin de paramètres spécifiques
        ]
        
        for endpoint_name in additional_endpoints:
            try:
                endpoint_data = await state.collector.fetch_endpoint_async(endpoint_name)
                
                analysis_data["endpoints"][endpoint_name] = {
                    "status": "success" if endpoint_data else "error",
                    "count": len(endpoint_data) if isinstance(endpoint_data, list) else 1 if endpoint_data else 0,
                    "success": endpoint_data is not None,
                    "error_code": 0 if endpoint_data else 500,
                    "error_msg": "" if endpoint_data else "No data"
                }
                
                # Stocker en cache
                if endpoint_data:
                    cache_key = f"{endpoint_name}:latest"
                    await state.cache.set(cache_key, endpoint_data, ttl=Config.CACHE_TTL.get(endpoint_name, 300))
                    
            except Exception as e:
                analysis_data["endpoints"][endpoint_name] = {
                    "status": "error",
                    "count": 0,
                    "success": False,
                    "error_code": 500,
                    "error_msg": str(e)
                }
                logging.error(f"Erreur collecte {endpoint_name}: {e}")
        
        # Calcul des totaux
        analysis_data["totals"]["total_endpoints"] = len(analysis_data["endpoints"])
        analysis_data["totals"]["successful_endpoints"] = sum(
            1 for ep in analysis_data["endpoints"].values() if ep["status"] == "success"
        )
        analysis_data["totals"]["failed_endpoints"] = (
            analysis_data["totals"]["total_endpoints"] - 
            analysis_data["totals"]["successful_endpoints"]
        )
        
        # Stocker l'analyse
        if state.db.conn:
            for endpoint_name, endpoint_data in analysis_data["endpoints"].items():
                await state.db.store_analysis({
                    "endpoint": endpoint_name,
                    "status": endpoint_data["status"],
                    "count": endpoint_data["count"],
                    "success": endpoint_data["success"],
                    "error_code": endpoint_data["error_code"],
                    "error_msg": endpoint_data["error_msg"]
                })
        
        logging.info(f"✅ Collecte terminée: {analysis_data['totals']['total_events']} événements + {len(additional_endpoints)} endpoints additionnels + Get1x2_VZip")
        
    except Exception as e:
        logging.error(f"Erreur dans collect_and_process_data: {e}")

async def process_single_event(state: AppState, event_data: Dict):
    """Traite un seul événement"""
    try:
        event_id = event_data.get("I")
        if not event_id:
            return
        
        # Normaliser l'événement
        normalized_event = state.processor.normalize_event(event_data)
        if not normalized_event:
            return
        
        # Stocker dans le cache
        cache_key = f"event:{event_id}"
        await state.cache.set(cache_key, normalized_event, ttl=Config.CACHE_TTL['live_odds'])
        
        # Stocker dans SQLite
        if state.db.conn:
            await state.db.store_event(normalized_event)
        
        # Publier les mises à jour
        try:
            await state.cache.publish(f"event:{event_id}:updates", {
                "event_id": event_id,
                "type": "update",
                "data": normalized_event,
                "timestamp": datetime.now().isoformat()
            })
            
            await state.cache.publish("events:live", {
                "event_id": event_id,
                "status": event_data.get("SS"),
                "sport_id": event_data.get("SI"),
                "timestamp": datetime.now().isoformat()
            })
        except Exception as e:
            logging.error(f"Erreur publish: {e}")
            
    except Exception as e:
        logging.error(f"Erreur process_single_event: {e}")

# ============================================================================
# LIFESPAN MANAGER
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestion du cycle de vie de l'application pour Railway"""
    # Initialisation
    logging.info("🚀 Démarrage de l'application sur Railway...")
    
    # Démarrer les services
    service_manager = ServiceManager()
    app.state.service_manager = service_manager
    
    logging.info("🔍 Vérification des services...")
    services_status = service_manager.check_services()
    
    if not services_status["sqlite"]:
        logging.warning("⚠️ Mode sans SQLite - pas de persistance des données")
        Config.SQLITE_DB_PATH = None
    
    logging.info("✅ Services prêts\n")
    
    # Initialiser les composants
    app.state.collector = OneXBetAPICollector()
    app.state.cache = CacheManager()
    app.state.db = DatabaseManager()
    app.state.processor = DataProcessor()
    app.state.monitor = MonitoringSystem()
    
    # Initialiser la base de données utilisateurs
    app.state.user_db = UserDatabase(app.state.db)
    
    # Connexion aux services
    await app.state.cache.connect()
    
    # Initialiser SQLite et tables auth
    if Config.SQLITE_DB_PATH:
        app.state.db.connect()
        app.state.user_db.init_tables()
    else:
        logging.warning("⚠️ SQLite désactivé - l'authentification ne fonctionnera pas")
        app.state.db.conn = None
    
    # Démarrer la collecte périodique
    app.state.collection_task = asyncio.create_task(periodic_collection_task(app.state))
    
    yield
    
    # Nettoyage
    logging.info("🛑 Arrêt de l'application...")
    
    if app.state.collection_task:
        app.state.collection_task.cancel()
        try:
            await app.state.collection_task
        except asyncio.CancelledError:
            pass
    
    if app.state.collector.async_client:
        await app.state.collector.async_client.aclose()
    
    if app.state.db.conn:
        app.state.db.conn.close()
    
    service_manager.stop_services()

# ============================================================================
# CREATE APP FUNCTION
# ============================================================================

def create_app() -> FastAPI:
    """Crée et configure l'application FastAPI pour Railway"""
    app = FastAPI(
        title="1xBet Data Collector API - Version Railway",
        description="API complète de collecte et diffusion de données sportives 1xBet avec authentification - Déployé sur Railway",
        version="5.2.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc"
    )
    
    # CORS middleware pour Railway
    origins = [
        "https://*.railway.app",
        "http://localhost:8000",
        "http://localhost:3000",
        "*"  # En développement seulement
    ]
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    return app

# ============================================================================
# CREATE APP INSTANCE
# ============================================================================

app = create_app()

# ============================================================================
# API ENDPOINTS COMPLETS AVEC NOUVEAUX ENDPOINTS
# ============================================================================

@app.get("/")
async def railway_root():
    """Page d'accueil spécifique pour Railway"""
    railway_env = os.getenv("RAILWAY_ENVIRONMENT", "production")
    railway_project = os.getenv("RAILWAY_PROJECT_NAME", "1xbet-api")
    
    return {
        "message": "1xBet Data Collector API déployé sur Railway 🚄",
        "version": "5.2.0",
        "environment": railway_env,
        "project": railway_project,
        "documentation": "/docs",
        "endpoints": {
            "health": "/health",
            "api": "/api/v1",
            "authentication": "/api/v1/auth/docs"
        },
        "features": [
            "📊 Collecte complète des données 1xBet",
            "🔐 Authentification JWT et clés API",
            "⚡ Cache Redis (Railway managed)",
            "💾 Persistance SQLite",
            "🔗 WebSocket temps réel",
            "📈 6 nouveaux endpoints",
            "🚄 Optimisé pour Railway"
        ]
    }

@app.get("/health")
async def railway_health():
    """Health check spécifique pour Railway"""
    try:
        # Vérifier Redis
        redis_status = "unknown"
        if app.state.cache and app.state.cache.redis:
            try:
                await app.state.cache.redis.ping()
                redis_status = "connected"
            except:
                redis_status = "disconnected"
        
        # Vérifier SQLite
        sqlite_status = "connected" if app.state.db and app.state.db.conn else "disconnected"
        
        # Vérifier la collecte
        collection_status = "running" if hasattr(app.state, 'collection_task') and app.state.collection_task and not app.state.collection_task.done() else "stopped"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "redis": redis_status,
                "sqlite": sqlite_status,
                "collection": collection_status
            },
            "environment": os.getenv("RAILWAY_ENVIRONMENT", "unknown"),
            "version": "5.2.0"
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )


# ============================================================================
# NOUVEAU ENDPOINT: Get1x2_VZip
# ============================================================================

@app.get("/api/v1/get1x2-vzip", response_model=Dict, tags=["Nouveaux Endpoints"])
async def get_1x2_vzip(
    count: int = Query(500, description="Nombre d'événements à récupérer (max 500)"),
    lng: str = Query("fr", description="Langue (fr, en, etc.)"),
    cyber_flag: int = Query(4, description="Flag cyber sports"),
    get_empty: bool = Query(True, description="Inclure événements vides"),
    alt_flag: bool = Query(True, description="Inclure événements alternatifs"),
    virtual_sports: bool = Query(True, description="Inclure sports virtuels"),
    no_filter_block_event: bool = Query(True, description="Pas de filtrage par bloc"),
    skip_cache: bool = Query(False, description="Ignorer le cache")
):
    """Récupère les données complètes de l'endpoint Get1x2_VZip"""
    try:
        # Construire la clé de cache
        cache_key = f"get1x2_vzip:count_{count}:lng_{lng}"
        
        if not skip_cache:
            cached = await app.state.cache.get(cache_key)
            if cached:
                return {
                    "success": True,
                    "from_cache": True,
                    **cached,
                    "timestamp": datetime.now().isoformat()
                }
        
        # Mettre à jour les paramètres dynamiquement
        collector = app.state.collector
        endpoint_config = collector.endpoints['get1x2_vzip_complete']
        
        # Mettre à jour les paramètres
        params = endpoint_config['params'].copy()
        params.update({
            'count': str(count),
            'lng': lng,
            'cyberFlag': str(cyber_flag),
            'getEmpty': str(get_empty).lower(),
            'altFlag': str(alt_flag).lower(),
            'virtualSports': str(virtual_sports).lower(),
            'noFilterBlockEvent': str(no_filter_block_event).lower()
        })
        
        # Récupérer les données
        data = await collector.fetch_endpoint_async("get1x2_vzip_complete")
        
        if not data:
            raise HTTPException(status_code=404, detail="Aucune donnée récupérée")
        
        # Analyser les données
        analysis = {
            "total_events": len(data.get("Value", [])),
            "success": data.get("Success", False),
            "error_code": data.get("ErrorCode", 0),
            "error_message": data.get("Error", ""),
            "sports_distribution": {},
            "status_distribution": {},
            "live_events": 0,
            "upcoming_events": 0
        }
        
        events = data.get("Value", [])
        for event in events:
            sport_id = event.get("SI")
            if sport_id:
                sport_name = SportID(sport_id).name if sport_id in [e.value for e in SportID] else f"Sport_{sport_id}"
                analysis["sports_distribution"][sport_name] = analysis["sports_distribution"].get(sport_name, 0) + 1
            
            status = event.get("SS")
            if status:
                status_name = MatchStatus(status).name if status in [e.value for e in MatchStatus] else f"Status_{status}"
                analysis["status_distribution"][status_name] = analysis["status_distribution"].get(status_name, 0) + 1
                
                if status == MatchStatus.LIVE.value:
                    analysis["live_events"] += 1
                elif status == MatchStatus.UPCOMING.value:
                    analysis["upcoming_events"] += 1
        
        # Traiter les événements
        processed_events = []
        for event in events:
            normalized_event = app.state.processor.normalize_event(event)
            if normalized_event:
                processed_events.append(normalized_event)
                
                # Stocker individuellement en cache
                event_id = normalized_event.get("event_id")
                if event_id:
                    event_cache_key = f"event:{event_id}"
                    await app.state.cache.set(
                        event_cache_key, 
                        normalized_event, 
                        ttl=Config.CACHE_TTL['get1x2_vzip']
                    )
        
        response_data = {
            "original_data": data,
            "processed_events": processed_events,
            "analysis": analysis,
            "parameters": {
                "count": count,
                "lng": lng,
                "cyber_flag": cyber_flag,
                "get_empty": get_empty,
                "alt_flag": alt_flag,
                "virtual_sports": virtual_sports,
                "no_filter_block_event": no_filter_block_event
            },
            "count": len(processed_events),
            "success": data.get("Success", False)
        }
        
        # Stocker en cache
        await app.state.cache.set(
            cache_key, 
            response_data, 
            ttl=Config.CACHE_TTL['get1x2_vzip']
        )
        
        return {
            "success": True,
            "from_cache": False,
            **response_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_1x2_vzip: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# AUTRES NOUVEAUX ENDPOINTS API (inchangés)
# ============================================================================

@app.get("/api/v1/express-day", response_model=Dict, tags=["Nouveaux Endpoints"])
async def get_express_day():
    """Récupère les paris express du jour"""
    try:
        cache_key = "express_day:latest"
        cached = await app.state.cache.get(cache_key)
        
        if cached:
            return {
                "success": True,
                "from_cache": True,
                "count": len(cached),
                "data": cached,
                "timestamp": datetime.now().isoformat()
            }
        
        data = await app.state.collector.fetch_endpoint_async("express_day")
        
        if not data:
            raise HTTPException(status_code=404, detail="Aucun pari express trouvé")
        
        # Traiter les données
        express_bets = []
        for item in data:
            if isinstance(item, dict):
                express_bets.append(item)
        
        # Stocker en cache
        await app.state.cache.set(cache_key, express_bets, ttl=Config.CACHE_TTL['express_day'])
        
        # Analyse des données
        analysis = app.state.processor.process_express_day_data(express_bets)
        
        return {
            "success": True,
            "from_cache": False,
            "count": len(express_bets),
            "data": express_bets,
            "analysis": analysis,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_express_day: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/championships", response_model=Dict, tags=["Nouveaux Endpoints"])
async def get_championships(
    sport_id: int = Query(1, description="ID du sport (1=Football par défaut)"),
    date_range_days: int = Query(1, description="Nombre de jours avant/après"),
    limit: int = Query(100, description="Nombre maximum de championnats")
):
    """Récupère la liste des championnats"""
    try:
        cache_key = f"championships:sport_{sport_id}:days_{date_range_days}:limit_{limit}"
        cached = await app.state.cache.get(cache_key)
        
        if cached:
            return {
                "success": True,
                "from_cache": True,
                **cached,
                "timestamp": datetime.now().isoformat()
            }
        
        # Mettre à jour les paramètres
        current_time = int(time.time())
        date_from = current_time - (date_range_days * 86400)
        date_to = current_time + (date_range_days * 86400)
        
        # AJOUTER CETTE LIGNE POUR S'ASSURER QUE L'URL EST CONSTRUITE
        endpoint_config = app.state.collector.endpoints['championships']
        endpoint_config['params']['dateFrom'] = str(date_from)
        endpoint_config['params']['dateTo'] = str(date_to)
        endpoint_config['params']['sportIds'] = str(sport_id)
        
        data = await app.state.collector.fetch_endpoint_async("championships")
        
        # GÉRER LE CAS OÙ LES DONNÉES SONT VIDES
        if not data or len(items) == 0:
            # Retourner une liste vide au lieu d'une erreur 404
            response_data = {
                "total_count": 0,
                "count": 0,
                "sport_id": sport_id,
                "date_range_days": date_range_days,
                "data": [],
                "analysis": {"count": 0, "total_games": 0, "championships": []}
            }
            
            # Stocker en cache quand même (même vide)
            await app.state.cache.set(cache_key, response_data, ttl=Config.CACHE_TTL['championships'])
            
            return {
                "success": True,
                "total_count": 0,
                "count": 0,
                "sport_id": sport_id,
                "date_range_days": date_range_days,
                "data": [],
                "analysis": {
                    "count": 0,
                    "total_games": 0,
                    "championships": []
                },
                "timestamp": datetime.now().isoformat()
            }
        
        # Gérer différents formats de réponse
        if isinstance(data, list):
            items = data
            count = len(data)
        elif isinstance(data, dict):
            items = data.get("items", data.get("data", data.get("Value", [])))
            if isinstance(items, dict):
                items = items.get("items", [])
            count = data.get("count", data.get("total", len(items)))
        else:
            items = []
            count = 0
        
        # Limiter les résultats
        limited_items = items[:limit] if isinstance(items, list) else []
        
        # Analyse des données
        analysis = app.state.processor.process_championship_data({
            "items": limited_items if isinstance(limited_items, list) else [],
            "count": count
        })
        
        response_data = {
            "total_count": count,
            "count": len(limited_items),
            "sport_id": sport_id,
            "date_range_days": date_range_days,
            "data": limited_items,
            "analysis": analysis
        }
        
        # Stocker en cache
        await app.state.cache.set(cache_key, response_data, ttl=Config.CACHE_TTL['championships'])
        
        return {
            "success": True,
            "from_cache": False,
            **response_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_championships: {e}")
        # Retourner une réponse vide plutôt qu'une erreur 500
        return {
            "success": True,
            "total_count": 0,
            "count": 0,
            "sport_id": sport_id,
            "date_range_days": date_range_days,
            "data": [],
            "analysis": {"count": 0, "total_games": 0, "championships": []},
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/v1/game-results", response_model=Dict, tags=["Nouveaux Endpoints"])
async def get_game_results(
    championship_id: int = Query(..., description="ID du championnat"),
    date_range_days: int = Query(1, description="Nombre de jours avant/après")
):
    """Récupère les résultats des matchs d'un championnat"""
    try:
        cache_key = f"game_results:champ_{championship_id}:days_{date_range_days}"
        cached = await app.state.cache.get(cache_key)
        
        if cached:
            return {
                "success": True,
                "from_cache": True,
                **cached,
                "timestamp": datetime.now().isoformat()
            }
        
        # Mettre à jour les paramètres
        current_time = int(time.time())
        date_from = current_time - (date_range_days * 86400)
        date_to = current_time + (date_range_days * 86400)
        
        endpoint_config = app.state.collector.endpoints['game_results']
        endpoint_config['params']['champId'] = str(championship_id)
        endpoint_config['params']['dateFrom'] = str(date_from)
        endpoint_config['params']['dateTo'] = str(date_to)
        
        data = await app.state.collector.fetch_endpoint_async("game_results")
        
        # GÉRER LE CAS OÙ LES DONNÉES SONT VIDES
        if not data:
            response_data = {
                "championship_id": championship_id,
                "total_count": 0,
                "count": 0,
                "date_range_days": date_range_days,
                "data": [],
                "analysis": {
                    "total_matches": 0,
                    "matches_with_stats": 0,
                    "matches_with_details": 0,
                    "average_subgames": 0
                }
            }
            
            await app.state.cache.set(cache_key, response_data, ttl=Config.CACHE_TTL['game_results'])
            
            return {
                "success": True,
                "from_cache": False,
                **response_data,
                "timestamp": datetime.now().isoformat()
            }
        
        # Gérer différents formats de réponse
        if isinstance(data, list):
            items = data
            count = len(data)
        elif isinstance(data, dict):
            items = data.get("items", data.get("data", data.get("Value", [])))
            if isinstance(items, dict):
                items = items.get("items", [])
            count = data.get("count", data.get("total", len(items)))
        else:
            items = []
            count = 0
        
        # Analyser les résultats
        match_analysis = {
            "total_matches": count,
            "matches_with_stats": sum(1 for item in items if isinstance(item, dict) and item.get("hasSubGame", False)),
            "matches_with_details": sum(1 for item in items if isinstance(item, dict) and item.get("matchInfosFull", "")),
            "average_subgames": sum(item.get("countSubGame", 0) for item in items if isinstance(item, dict)) / count if count > 0 else 0
        }
        
        response_data = {
            "championship_id": championship_id,
            "total_count": count,
            "count": len(items),
            "date_range_days": date_range_days,
            "data": items,
            "analysis": match_analysis
        }
        
        # Stocker en cache
        await app.state.cache.set(cache_key, response_data, ttl=Config.CACHE_TTL['game_results'])
        
        return {
            "success": True,
            "from_cache": False,
            **response_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_game_results: {e}")
        return {
            "success": True,
            "championship_id": championship_id,
            "total_count": 0,
            "count": 0,
            "date_range_days": date_range_days,
            "data": [],
            "analysis": {
                "total_matches": 0,
                "matches_with_stats": 0,
                "matches_with_details": 0,
                "average_subgames": 0
            },
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/v1/top-games", response_model=Dict, tags=["Nouveaux Endpoints"])
async def get_top_games(
    limit: int = Query(20, description="Nombre maximum de matchs")
):
    """Récupère les matchs les plus populaires/mis en avant"""
    try:
        cache_key = f"top_games:limit_{limit}"
        cached = await app.state.cache.get(cache_key)
        
        if cached:
            return {
                "success": True,
                "from_cache": True,
                **cached,
                "timestamp": datetime.now().isoformat()
            }
        
        data = await app.state.collector.fetch_endpoint_async("top_games")
        
        if not data or not data.get("Value"):
            raise HTTPException(status_code=404, detail="Aucun match top trouvé")
        
        games = data["Value"]
        
        # Limiter les résultats
        limited_games = games[:limit]
        
        # Analyse des données
        analysis = app.state.processor.process_top_games_data(limited_games)
        
        # Distribution par sport
        sports_distribution = {}
        for game in limited_games:
            sport_id = game.get("SI")
            if sport_id:
                sport_name = SportID(sport_id).name if sport_id in [e.value for e in SportID] else f"Sport_{sport_id}"
                sports_distribution[sport_name] = sports_distribution.get(sport_name, 0) + 1
        
        response_data = {
            "count": len(limited_games),
            "total_available": len(games),
            "data": limited_games,
            "analysis": analysis,
            "sports_distribution": sports_distribution,
            "game_stats": {
                "average_markets": analysis["market_statistics"]["average_markets"],
                "unique_market_types": analysis["market_statistics"]["unique_market_types"]
            }
        }
        
        # Stocker en cache
        await app.state.cache.set(cache_key, response_data, ttl=Config.CACHE_TTL['top_games'])
        
        return {
            "success": True,
            "from_cache": False,
            **response_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_top_games: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/sports-simple", response_model=Dict, tags=["Nouveaux Endpoints"])
async def get_sports_simple(
    include_esports: bool = Query(True, description="Inclure les esports"),
    only_top: bool = Query(False, description="Seulement les sports marqués 'isTop'")
):
    """Récupère la liste simplifiée des sports"""
    try:
        cache_key = f"sports_simple:esports_{include_esports}:only_top_{only_top}"
        cached = await app.state.cache.get(cache_key)
        
        if cached:
            return {
                "success": True,
                "from_cache": True,
                **cached,
                "timestamp": datetime.now().isoformat()
            }
        
        current_time = int(time.time())
        date_range = 86400  # 24 heures
        
        # UTILISER UN ENDPOINT DIFFÉRENT CAR L'ENDPOINT 'sports_list' N'EXISTE PAS
        # Utiliser l'endpoint 'prematch_sports' à la place
        sports_data = await app.state.collector.fetch_endpoint_async("prematch_sports")
        
        if not sports_data or not sports_data.get("Value"):
            # Retourner une liste basée sur SportID si pas de données
            default_items = []
            for sport_enum in SportID:
                if sport_enum.value == 40 and not include_esports:
                    continue
                default_items.append({
                    "id": sport_enum.value,
                    "name": sport_enum.name,
                    "isTop": sport_enum.value in [1, 2, 3, 4]  # Football, Ice Hockey, Basketball, Tennis
                })
            
            if only_top:
                default_items = [item for item in default_items if item["isTop"]]
            
            response_data = {
                "stats": {
                    "total_sports": len(default_items),
                    "filtered_sports": len(default_items),
                    "top_sports": sum(1 for item in default_items if item.get("isTop", False)),
                    "esports_included": include_esports,
                    "esports_count": 1 if include_esports else 0
                },
                "count": len(default_items),
                "data": default_items
            }
            
            await app.state.cache.set(cache_key, response_data, ttl=Config.CACHE_TTL['sports'])
            
            return {
                "success": True,
                "from_cache": False,
                **response_data,
                "timestamp": datetime.now().isoformat()
            }
        
        sports = sports_data["Value"]
        items = []
        
        for sport in sports:
            if isinstance(sport, dict):
                # Créer un item simplifié
                items.append({
                    "id": sport.get("I", 0),
                    "name": sport.get("N", sport.get("E", "Unknown")),
                    "isTop": sport.get("CID", 0) == 1  # POPULAR category
                })
        
        # Filtrer si nécessaire
        filtered_items = items
        if only_top:
            filtered_items = [item for item in items if item.get("isTop", False)]
        
        # Statistiques
        stats = {
            "total_sports": len(items),
            "filtered_sports": len(filtered_items),
            "top_sports": sum(1 for item in items if item.get("isTop", False)),
            "esports_included": include_esports,
            "esports_count": sum(1 for item in items if item.get("id") == 40)  # ID esports
        }
        
        response_data = {
            "stats": stats,
            "count": len(filtered_items),
            "data": filtered_items
        }
        
        # Stocker en cache
        await app.state.cache.set(cache_key, response_data, ttl=Config.CACHE_TTL['sports'])
        
        return {
            "success": True,
            "from_cache": False,
            **response_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_sports_simple: {e}")
        # Retourner une liste vide plutôt qu'une erreur
        return {
            "success": True,
            "stats": {
                "total_sports": 0,
                "filtered_sports": 0,
                "top_sports": 0,
                "esports_included": include_esports,
                "esports_count": 0
            },
            "count": 0,
            "data": [],
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/v1/multi-endpoint", response_model=Dict, tags=["Nouveaux Endpoints"])
async def get_multi_endpoint_data(
    endpoints: str = Query("express_day,top_games", description="Liste d'endpoints séparés par des virgules"),
    cache_timeout: int = Query(60, description="Timeout du cache en secondes")
):
    """Récupère plusieurs endpoints en une seule requête"""
    try:
        endpoint_list = [ep.strip() for ep in endpoints.split(",")]
        results = {}
        from_cache_counts = {"total": 0, "from_cache": 0}
        
        for endpoint in endpoint_list:
            if endpoint in app.state.collector.endpoints:
                cache_key = f"{endpoint}:latest"
                cached = await app.state.cache.get(cache_key)
                
                if cached:
                    results[endpoint] = {
                        "success": True,
                        "from_cache": True,
                        "data": cached,
                        "fetched_at": datetime.now().isoformat()
                    }
                    from_cache_counts["from_cache"] += 1
                else:
                    data = await app.state.collector.fetch_endpoint_async(endpoint)
                    results[endpoint] = {
                        "success": data is not None,
                        "from_cache": False,
                        "data": data,
                        "fetched_at": datetime.now().isoformat()
                    }
                    
                    # Stocker en cache si succès
                    if data:
                        await app.state.cache.set(cache_key, data, ttl=cache_timeout)
                from_cache_counts["total"] += 1
            else:
                results[endpoint] = {
                    "success": False,
                    "error": f"Endpoint '{endpoint}' non trouvé",
                    "fetched_at": datetime.now().isoformat()
                }
        
        cache_ratio = (from_cache_counts["from_cache"] / from_cache_counts["total"]) * 100 if from_cache_counts["total"] > 0 else 0
        
        return {
            "success": True,
            "endpoints_requested": endpoint_list,
            "endpoints_found": [ep for ep in endpoint_list if ep in app.state.collector.endpoints],
            "cache_statistics": {
                **from_cache_counts,
                "cache_hit_ratio": f"{cache_ratio:.1f}%"
            },
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Erreur get_multi_endpoint_data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# AUTHENTICATION ENDPOINTS (inchangés)
# ============================================================================

# REMPLACER la fonction login actuelle par CELLE-CI :
@app.post("/api/v1/auth/login", response_model=Token, tags=["Authentication"])
async def login(
    username: str = Form(...),
    password: str = Form(...),
    request: Request = None
):
    """Connexion avec username/password (format form-data)"""
    user = app.state.user_db.get_user(username)
    if not user or not verify_password(password, user.hashed_password):
        app.state.user_db.add_login_history(
            user_id=username if user else "unknown",
            ip_address=request.client.host if request and request.client else None,
            user_agent=request.headers.get("user-agent") if request else None,
            success=False,
            method="password"
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nom d'utilisateur ou mot de passe incorrect",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if user.disabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Compte désactivé"
        )
    
    access_token_expires = timedelta(minutes=Config.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username, "roles": user.roles},
        expires_delta=access_token_expires
    )
    
    refresh_token = create_refresh_token(data={"sub": user.username})
    
    refresh_expires = datetime.utcnow() + timedelta(days=Config.REFRESH_TOKEN_EXPIRE_DAYS)
    app.state.user_db.store_refresh_token(user.username, refresh_token, refresh_expires)
    
    app.state.user_db.update_user_last_login(user.username)
    
    app.state.user_db.add_login_history(
        user_id=user.username,
        ip_address=request.client.host if request and request.client else None,
        user_agent=request.headers.get("user-agent") if request else None,
        success=True,
        method="password"
    )
    
    return Token(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=Config.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )

@app.post("/api/v1/auth/refresh", response_model=Token, tags=["Authentication"])
async def refresh_token(
    refresh_token: str = Form(...),  # <-- Ajoutez = Form(...)
    request: Request = None  # <-- Changez Optional[dict] en Request
):
    """Rafraîchir un token JWT"""
    payload = verify_token(refresh_token)
    if not payload or payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token invalide"
        )
    
    username = payload.get("sub")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token invalide"
        )
    
    if not app.state.user_db.validate_refresh_token(username, refresh_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token expiré ou révoqué"
        )
    
    user = app.state.user_db.get_user(username)
    if not user or user.disabled:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Utilisateur non trouvé"
        )
    
    access_token_expires = timedelta(minutes=Config.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username, "roles": user.roles},
        expires_delta=access_token_expires
    )
    
    app.state.user_db.add_login_history(
        user_id=user.username,
        ip_address=request.client.host if request and request.client else None,
        user_agent=request.headers.get("user-agent") if request else None,
        success=True,
        method="refresh_token"
    )
    
    return Token(
        access_token=access_token,
        expires_in=Config.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )

@app.post("/api/v1/auth/logout", tags=["Authentication"])
async def logout(
    current_user: UserInDB = Depends(get_current_active_user),
    refresh_token: Optional[str] = None
):
    """Déconnexion - Révoque les refresh tokens"""
    app.state.user_db.revoke_refresh_tokens(current_user.username)
    
    return {"message": "Déconnexion réussie"}

@app.post("/api/v1/auth/register", response_model=dict, tags=["Authentication"])
async def register(
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    full_name: Optional[str] = Form(None)
):
    """Inscription d'un nouvel utilisateur"""
    existing_user = app.state.user_db.get_user(username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nom d'utilisateur déjà utilisé"
        )
    
    hashed_password = get_password_hash(password)
    user = UserInDB(
        username=username,
        email=email,
        full_name=full_name,
        hashed_password=hashed_password,
        roles=["user"]
    )
    
    if app.state.user_db.create_user(user):
        return {
            "message": "Utilisateur créé avec succès",
            "username": username
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la création de l'utilisateur"
        )

@app.post("/api/v1/auth/api-keys", response_model=APIKeyInfo, tags=["Authentication"])
async def create_api_key(
    name: str = Form(...),
    expires_days: Optional[int] = Form(None),
    current_user: UserInDB = Depends(get_current_active_user)
):
    """Créer une nouvelle clé API (format form-data)"""
    api_key = app.state.user_db.create_api_key(
        username=current_user.username,
        name=name,
        expires_days=expires_days
    )
    
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur lors de la création de la clé API"
        )
    
    expires_at = None
    if expires_days:
        expires_at = datetime.now() + timedelta(days=expires_days)
    
    return APIKeyInfo(
        name=name,
        key=api_key,
        created_at=datetime.now(),
        expires_at=expires_at,
        last_used=None
    )

@app.get("/api/v1/auth/api-keys", response_model=List[dict], tags=["Authentication"])
async def get_api_keys(
    current_user: UserInDB = Depends(get_current_active_user)
):
    """Lister les clés API de l'utilisateur"""
    return app.state.user_db.get_user_api_keys(current_user.username)

@app.delete("/api/v1/auth/api-keys/{key_hash}", tags=["Authentication"])
async def revoke_api_key(
    key_hash: str,
    current_user: UserInDB = Depends(get_current_active_user)
):
    """Révoquer une clé API"""
    if app.state.user_db.revoke_api_key(current_user.username, key_hash):
        return {"message": "Clé API révoquée"}
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Clé API non trouvée"
        )

@app.get("/api/v1/auth/profile", response_model=User, tags=["Authentication"])
async def get_profile(
    current_user: UserInDB = Depends(get_current_active_user)
):
    """Récupérer le profil de l'utilisateur"""
    return current_user

@app.put("/api/v1/auth/profile", response_model=User, tags=["Authentication"])
async def update_profile(
    email: Optional[str] = None,
    full_name: Optional[str] = None,
    current_user: UserInDB = Depends(get_current_active_user)
):
    """Mettre à jour le profil utilisateur"""
    try:
        updates = []
        params = []
        
        if email is not None:
            updates.append("email = ?")
            params.append(email)
        
        if full_name is not None:
            updates.append("full_name = ?")
            params.append(full_name)
        
        if updates:
            params.append(current_user.username)
            query = f"UPDATE users SET {', '.join(updates)} WHERE username = ?"
            app.state.db.cursor.execute(query, params)
            app.state.db.conn.commit()
            
            updated_user = app.state.user_db.get_user(current_user.username)
            return updated_user
        
        return current_user
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur mise à jour profil: {e}"
        )

@app.post("/api/v1/auth/change-password", tags=["Authentication"])
async def change_password(
    current_password: str,
    new_password: str,
    current_user: UserInDB = Depends(get_current_active_user)
):
    """Changer le mot de passe"""
    if not verify_password(current_password, current_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Mot de passe actuel incorrect"
        )
    
    new_hashed_password = get_password_hash(new_password)
    app.state.db.cursor.execute(
        "UPDATE users SET hashed_password = ? WHERE username = ?",
        (new_hashed_password, current_user.username)
    )
    app.state.db.conn.commit()
    
    app.state.user_db.revoke_refresh_tokens(current_user.username)
    
    return {"message": "Mot de passe changé avec succès"}

# ============================================================================
# ENDPOINTS EXISTANTS (inchangés)
# ============================================================================

@app.get("/api/v1/sports", response_model=Dict)
async def get_sports(
    category: Optional[int] = Query(None, description="Catégorie de sport (CID)"),
    live_only: bool = Query(False, description="Sports avec événements en direct uniquement"),
    include_virtual: bool = Query(True, description="Inclure les sports virtuels"),
    skip_cache: bool = Query(False, description="Ignorer le cache")
):
    """Récupère la liste des sports (public)"""
    return await get_sports_impl(
        category, live_only, include_virtual, skip_cache
    )

@app.get("/api/v1/events", response_model=Dict)
async def get_events(
    sport_id: Optional[int] = Query(None, description="ID du sport"),
    competition_id: Optional[int] = Query(None, description="ID de la compétition"),
    status: Optional[MatchStatus] = Query(None, description="Statut du match"),
    country_id: Optional[int] = Query(None, description="ID du pays"),
    is_virtual: Optional[bool] = Query(None, description="Événements virtuels uniquement"),
    limit: int = Query(50, ge=1, le=1000, description="Nombre maximum d'événements"),
    offset: int = Query(0, ge=0, description="Offset pour la pagination"),
    skip_cache: bool = Query(False, description="Ignorer le cache")
):
    """Récupère la liste des événements (public)"""
    return await get_events_impl(
        sport_id, competition_id, status, country_id, 
        is_virtual, limit, offset, skip_cache
    )

@app.get("/api/v1/events/{event_id}", response_model=Dict)
async def get_event_detail(
    event_id: int,
    include_history: bool = Query(False, description="Inclure l'historique des cotes"),
    history_hours: int = Query(24, ge=1, le=168, description="Nombre d'heures d'historique"),
    include_analysis: bool = Query(False, description="Inclure l'analyse des données")
):
    """Récupère le détail COMPLET d'un événement (public)"""
    try:
        cache_key = f"event:{event_id}"
        cached = await app.state.cache.get(cache_key)
        
        event_data = None
        from_cache = False
        
        if cached:
            event_data = cached
            from_cache = True
        elif app.state.db.conn:
            app.state.db.cursor.execute(
                "SELECT * FROM events WHERE event_id = ?", 
                (event_id,)
            )
            row = app.state.db.cursor.fetchone()
            if row:
                event_data = dict(row)
                for field in ['home_team', 'away_team', 'main_markets', 'additional_markets', 
                            'live_score', 'win_probabilities', 'match_info', 'metadata']:
                    if event_data.get(field):
                        event_data[field] = json.loads(event_data[field])
        
        if not event_data:
            raise HTTPException(status_code=404, detail="Événement non trouvé")
        
        response = {
            "success": True,
            "data": event_data,
            "from_cache": from_cache,
            "parsed_data": {},
            "timestamp": datetime.now().isoformat()
        }
        
        if event_data.get("live_score"):
            response["parsed_data"]["period_scores"] = app.state.processor.parse_period_scores(
                event_data["live_score"].get("PS", [])
            )
            response["parsed_data"]["statistics"] = app.state.processor.parse_statistics(
                event_data["live_score"].get("ST", [])
            )
        
        if event_data.get("metadata"):
            response["parsed_data"]["metadata_structured"] = app.state.processor.parse_metadata(
                event_data["metadata"]
            )
        
        if event_data.get("additional_markets"):
            response["parsed_data"]["additional_markets_parsed"] = (
                app.state.processor.extract_additional_markets(event_data["additional_markets"])
            )
        
        if event_data.get("win_probabilities"):
            response["parsed_data"]["implied_odds"] = (
                app.state.processor.calculate_implied_probabilities(event_data["win_probabilities"])
            )
        
        if event_data.get("main_markets"):
            response["parsed_data"]["main_odds"] = (
                app.state.processor.extract_main_odds(event_data)
            )
        
        if include_analysis:
            is_complete, warnings = app.state.processor.validate_event_completeness(event_data)
            response["analysis"] = {
                "is_complete": is_complete,
                "warnings": warnings,
                "status_text": app.state.processor.get_status_text(
                    event_data.get("SS", 0),
                    event_data.get("live_score")
                )
            }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_event_detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/events/{event_id}/odds", response_model=Dict)
async def get_event_odds(
    event_id: int,
    format_type: Optional[str] = Query("decimal", description="Format des cotes: decimal, fractional, american")
):
    """Récupère les cotes d'un événement avec formatage (public)"""
    try:
        cache_key = f"event:{event_id}"
        event_data = await app.state.cache.get(cache_key)
        
        if not event_data and app.state.db.conn:
            app.state.db.cursor.execute(
                "SELECT * FROM events WHERE event_id = ?", 
                (event_id,)
            )
            row = app.state.db.cursor.fetchone()
            if row:
                event_data = dict(row)
                for field in ['main_markets', 'additional_markets']:
                    if event_data.get(field):
                        event_data[field] = json.loads(event_data[field])
        
        if not event_data:
            raise HTTPException(status_code=404, detail="Événement non trouvé")
        
        main_odds = event_data.get("main_markets", event_data.get("E", []))
        additional_odds = event_data.get("additional_markets", event_data.get("AE", []))
        
        odds_type = event_data.get("odds_type", event_data.get("KI", 3))
        formatted_odds = []
        
        for odd in main_odds:
            if isinstance(odd, dict) and odd.get("C"):
                formatted_odd = odd.copy()
                formatted_odd["formatted"] = app.state.processor.format_odds_display(
                    odd["C"], odds_type
                )
                formatted_odds.append(formatted_odd)
        
        return {
            "success": True,
            "event_id": event_id,
            "odds_type": OddsType(odds_type).name if odds_type in [e.value for e in OddsType] else "UNKNOWN",
            "main_odds": formatted_odds,
            "additional_odds": additional_odds,
            "total_markets": event_data.get("total_markets", event_data.get("EC", 0)),
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_event_odds: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/events/{event_id}/stats", response_model=Dict)
async def get_event_stats(event_id: int):
    """Récupère les statistiques détaillées d'un événement (public)"""
    try:
        cache_key = f"event:{event_id}"
        event_data = await app.state.cache.get(cache_key)
        
        if not event_data:
            raise HTTPException(status_code=404, detail="Événement non trouvé")
        
        live_score = event_data.get("live_score", {})
        
        # DEBUG: Log pour voir la structure des données
        logging.info(f"Event {event_id} - live_score keys: {live_score.keys()}")
        logging.info(f"Event {event_id} - ST field: {live_score.get('ST')}")
        logging.info(f"Event {event_id} - PS field: {live_score.get('PS')}")
        
        # 1. Vérifier si les statistiques sont disponibles
        stats_data = live_score.get("ST")
        if not stats_data:
            # Retourner des statistiques vides mais avec succès
            return {
                "success": True,
                "event_id": event_id,
                "statistics": {
                    "attacks": [],
                    "shots": [],
                    "possession": [],
                    "discipline": [],
                    "set_pieces": [],
                    "other": []
                },
                "period_scores": live_score.get("PS", []),  # Utiliser directement PS
                "current_period": live_score.get("CP"),
                "period_name": live_score.get("CPS"),
                "elapsed_time": live_score.get("TS"),
                "remaining_time": live_score.get("TR"),
                "timestamp": datetime.now().isoformat()
            }
        
        # 2. Parser les statistiques
        stats = app.state.processor.parse_statistics(stats_data)
        
        # 3. Améliorer la catégorisation
        stats_by_category = {
            "attacks": [],
            "shots": [],
            "possession": [],
            "discipline": [],
            "set_pieces": [],
            "other": []
        }
        
        for period, period_stats in stats.items():
            if not isinstance(period_stats, list):
                continue
                
            for stat in period_stats:
                if not isinstance(stat, dict):
                    continue
                    
                # Utiliser le nom anglais comme fallback
                stat_name = str(stat.get("name_fr", stat.get("name", ""))).lower()
                stat_item = {
                    "period": period,
                    "name": stat.get("name_fr", stat.get("name", "Unknown")),
                    "home": stat.get("home_value"),
                    "away": stat.get("away_value"),
                    "id": stat.get("id")
                }
                
                # Catégorisation améliorée
                added = False
                
                # Attaques
                attack_keywords = ["attaque", "danger", "offensi", "attack", "dangerous", "opportunit"]
                if any(keyword in stat_name for keyword in attack_keywords):
                    stats_by_category["attacks"].append(stat_item)
                    added = True
                
                # Tirs
                shot_keywords = ["tir", "shot", "but", "goal", "on target", "off target", "blocked"]
                if not added and any(keyword in stat_name for keyword in shot_keywords):
                    stats_by_category["shots"].append(stat_item)
                    added = True
                
                # Possession
                possession_keywords = ["possession", "ball", "ballon"]
                if not added and any(keyword in stat_name for keyword in possession_keywords):
                    stats_by_category["possession"].append(stat_item)
                    added = True
                
                # Discipline
                discipline_keywords = ["faute", "carton", "avertissement", "foul", "card", "yellow", "red"]
                if not added and any(keyword in stat_name for keyword in discipline_keywords):
                    stats_by_category["discipline"].append(stat_item)
                    added = True
                
                # Coups de pied arrêtés
                set_piece_keywords = ["corner", "hors-jeu", "coup franc", "free kick", "offside", "throw", "penalty"]
                if not added and any(keyword in stat_name for keyword in set_piece_keywords):
                    stats_by_category["set_pieces"].append(stat_item)
                    added = True
                
                # Autres
                if not added:
                    stats_by_category["other"].append(stat_item)
        
        # 4. Parser les scores par période
        period_scores = live_score.get("PS", [])
        parsed_period_scores = []
        if isinstance(period_scores, list):
            parsed_period_scores = app.state.processor.parse_period_scores(period_scores)
        elif period_scores:
            parsed_period_scores = app.state.processor.parse_period_scores([period_scores])
        
        return {
            "success": True,
            "event_id": event_id,
            "statistics": stats_by_category,
            "period_scores": parsed_period_scores,
            "current_period": live_score.get("CP"),
            "period_name": live_score.get("CPS"),
            "elapsed_time": live_score.get("TS"),
            "remaining_time": live_score.get("TR"),
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_event_stats pour event {event_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/analysis", response_model=Dict)
async def get_analysis(
    hours: int = Query(24, ge=1, le=168, description="Nombre d'heures à analyser")
):
    """Récupère l'analyse des collectes récentes (public)"""
    try:
        if not app.state.db.conn:
            return {
                "success": True,
                "data": {"message": "SQLite non disponible pour l'analyse"},
                "timestamp": datetime.now().isoformat()
            }
        
        from_time = datetime.now() - timedelta(hours=hours)
        
        app.state.db.cursor.execute('''
            SELECT endpoint, status, count, success, timestamp 
            FROM analysis 
            WHERE timestamp >= ?
            ORDER BY timestamp DESC
            LIMIT 100
        ''', (from_time.isoformat(),))
        
        rows = app.state.db.cursor.fetchall()
        analysis_data = [dict(row) for row in rows]
        
        stats = {
            "total_requests": len(analysis_data),
            "successful_requests": sum(1 for a in analysis_data if a.get("success")),
            "failed_requests": sum(1 for a in analysis_data if not a.get("success")),
            "by_endpoint": {},
            "success_rate": 0
        }
        
        for item in analysis_data:
            endpoint = item.get("endpoint")
            if endpoint not in stats["by_endpoint"]:
                stats["by_endpoint"][endpoint] = {
                    "count": 0,
                    "success": 0,
                    "failed": 0
                }
            
            stats["by_endpoint"][endpoint]["count"] += 1
            if item.get("success"):
                stats["by_endpoint"][endpoint]["success"] += 1
            else:
                stats["by_endpoint"][endpoint]["failed"] += 1
        
        if stats["total_requests"] > 0:
            stats["success_rate"] = (stats["successful_requests"] / stats["total_requests"]) * 100
        
        for endpoint in stats["by_endpoint"]:
            endpoint_stats = stats["by_endpoint"][endpoint]
            if endpoint_stats["count"] > 0:
                endpoint_stats["success_rate"] = (
                    endpoint_stats["success"] / endpoint_stats["count"]
                ) * 100
        
        return {
            "success": True,
            "period_hours": hours,
            "statistics": stats,
            "recent_data": analysis_data[:10],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Erreur get_analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/reference/{table_name}", response_model=Dict)
async def get_reference_table(table_name: str):
    """Récupère les tables de référence (public)"""
    try:
        tables = {
            "sport_categories": {e.value: e.name for e in SportCategory},
            "match_status": {e.value: e.name for e in MatchStatus},
            "odds_types": {e.value: e.name for e in OddsType},
            "market_groups": {e.value: e.name for e in MarketGroup},
            "event_types": {e.value: e.name for e in EventType},
            "country_codes": {e.value: e.name for e in CountryCode},
            "sport_ids": {e.value: e.name for e in SportID},
            "statistic_ids": {e.value: e.name for e in StatisticID},
            "special_markets": {e.value: e.name for e in SpecialMarket},
            "metadata_keys": {e.value: e.name for e in MetadataKey},
            "bet_types": {e.value: e.name for e in BetType}
        }
        
        if table_name not in tables:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' non trouvée")
        
        return {
            "success": True,
            "table_name": table_name,
            "data": tables[table_name],
            "count": len(tables[table_name]),
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_reference_table: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/competitions", response_model=Dict)
async def get_competitions(
    sport_id: Optional[int] = Query(None, description="ID du sport"),
    country_id: Optional[int] = Query(None, description="ID du pays"),
    live_only: bool = Query(False, description="Compétitions avec événements en direct uniquement")
):
    """Récupère la liste des compétitions (public)"""
    try:
        endpoint = "live_odds" if live_only else "prematch_odds"
        
        events_data = await app.state.collector.fetch_endpoint_async(endpoint)
        if not events_data or not events_data.get("Value"):
            return {
                "success": True,
                "data": [],
                "count": 0,
                "timestamp": datetime.now().isoformat()
            }
        
        events = events_data["Value"]
        competitions = {}
        
        for event in events:
            if sport_id and event.get("SI") != sport_id:
                continue
            if country_id and event.get("COI") != country_id:
                continue
            
            comp_id = event.get("CI")
            if not comp_id:
                continue
            
            if comp_id not in competitions:
                competitions[comp_id] = {
                    "id": comp_id,
                    "name_fr": event.get("L", ""),
                    "name_en": event.get("LE", ""),
                    "name_ru": event.get("LR", ""),
                    "sport_id": event.get("SI"),
                    "sport_name": event.get("SN", "").strip(),
                    "country_id": event.get("COI"),
                    "country_name_fr": event.get("CN", ""),
                    "country_name_en": event.get("CE", ""),
                    "event_count": 1,
                    "live_event_count": 1 if event.get("SS") == MatchStatus.LIVE.value else 0,
                    "image": event.get("CHIMG")
                }
            else:
                competitions[comp_id]["event_count"] += 1
                if event.get("SS") == MatchStatus.LIVE.value:
                    competitions[comp_id]["live_event_count"] += 1
        
        competitions_list = list(competitions.values())
        competitions_list.sort(key=lambda x: x["event_count"], reverse=True)
        
        return {
            "success": True,
            "data": competitions_list,
            "count": len(competitions_list),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Erreur get_competitions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/stats", response_model=Dict)
async def get_system_stats():
    """Statistiques du système (public)"""
    try:
        event_keys = []
        if app.state.cache.redis:
            event_keys = await app.state.cache.redis.keys("event:*")
        
        live_count = 0
        for key in event_keys:
            event = await app.state.cache.get(key)
            if event and event.get("SS") == MatchStatus.LIVE.value:
                live_count += 1
        
        db_stats = {}
        if app.state.db.cursor:
            tables = ["events", "odds_history", "score_history", "analysis"]
            for table in tables:
                app.state.db.cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                row = app.state.db.cursor.fetchone()
                db_stats[table] = row['count'] if row else 0
        
        redis_info = {}
        if app.state.cache.redis:
            try:
                redis_info = await app.state.cache.redis.info()
            except:
                redis_info = {}
        
        return {
            "success": True,
            "data": {
                "cache": {
                    "total_events": len(event_keys),
                    "live_events": live_count,
                    "memory_used_mb": int(redis_info.get("used_memory", 0)) / 1024 / 1024 if redis_info else 0,
                    "connected_clients": redis_info.get("connected_clients", 0) if redis_info else 0
                },
                "database": db_stats,
                "system": {
                    "collection_running": not app.state.collection_task.done() if app.state.collection_task else False,
                    "collection_interval": Config.COLLECTION_INTERVAL,
                    "cache_ttl": Config.CACHE_TTL,
                    "monitoring_rules": len(app.state.monitor.alert_rules)
                }
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Erreur get_system_stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# PROTECTED ENDPOINTS
# ============================================================================

@app.get("/api/v1/protected/sports", response_model=Dict, tags=["Protected"])
async def get_protected_sports(
    current_user: UserInDB = Depends(get_current_active_user),
    category: Optional[int] = Query(None, description="Catégorie de sport (CID)"),
    live_only: bool = Query(False, description="Sports avec événements en direct uniquement"),
    include_virtual: bool = Query(True, description="Inclure les sports virtuels"),
    skip_cache: bool = Query(False, description="Ignorer le cache")
):
    """Version protégée de get_sports"""
    return await get_sports_impl(category, live_only, include_virtual, skip_cache)

@app.get("/api/v1/protected/events", response_model=Dict, tags=["Protected"])
async def get_protected_events(
    current_user: UserInDB = Depends(get_current_active_user),
    sport_id: Optional[int] = Query(None, description="ID du sport"),
    competition_id: Optional[int] = Query(None, description="ID de la compétition"),
    status: Optional[MatchStatus] = Query(None, description="Statut du match"),
    country_id: Optional[int] = Query(None, description="ID du pays"),
    is_virtual: Optional[bool] = Query(None, description="Événements virtuels uniquement"),
    limit: int = Query(50, ge=1, le=1000, description="Nombre maximum d'événements"),
    offset: int = Query(0, ge=0, description="Offset pour la pagination"),
    skip_cache: bool = Query(False, description="Ignorer le cache")
):
    """Version protégée de get_events"""
    return await get_events_impl(
        sport_id, competition_id, status, country_id, 
        is_virtual, limit, offset, skip_cache
    )

# ============================================================================
# ADMIN ENDPOINTS
# ============================================================================

# ============================================================================
# ADMIN ENDPOINTS COMPLÉMENTAIRES
# ============================================================================

@app.get("/api/v1/admin/audit", response_model=Dict, tags=["Admin"])
async def get_audit_logs(
    current_user: UserInDB = Depends(require_role("admin")),
    hours: int = Query(24, description="Nombre d'heures à remonter"),
    action_type: Optional[str] = Query(None, description="Type d'action (auth, api, etc.)")
):
    """Récupère les logs d'audit"""
    try:
        from_time = datetime.now() - timedelta(hours=hours)
        
        query = '''
            SELECT * FROM login_history 
            WHERE login_time >= ?
        '''
        params = [from_time.isoformat()]
        
        if action_type:
            query += " AND method = ?"
            params.append(action_type)
        
        query += " ORDER BY login_time DESC LIMIT 100"
        
        app.state.db.cursor.execute(query, params)
        rows = app.state.db.cursor.fetchall()
        
        logs = [dict(row) for row in rows]
        
        return {
            "success": True,
            "count": len(logs),
            "logs": logs,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logging.error(f"Erreur get_audit_logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/admin/users/{username}/suspend", tags=["Admin"])
async def suspend_user(
    username: str,
    current_user: UserInDB = Depends(require_role("admin"))
):
    """Suspendre un utilisateur"""
    try:
        if username == current_user.username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Vous ne pouvez pas suspendre votre propre compte"
            )
        
        app.state.db.cursor.execute(
            "UPDATE users SET disabled = TRUE WHERE username = ?",
            (username,)
        )
        
        affected = app.state.db.cursor.rowcount
        app.state.db.conn.commit()
        
        if affected > 0:
            return {"message": f"Utilisateur {username} suspendu"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Utilisateur {username} non trouvé"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur suspend_user: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/admin/stats", response_model=Dict, tags=["Admin"])
async def get_admin_stats(
    current_user: UserInDB = Depends(require_role("admin"))
):
    """Statistiques administratives"""
    try:
        app.state.db.cursor.execute('''
            SELECT 
                COUNT(*) as total_users,
                SUM(CASE WHEN disabled = TRUE THEN 1 ELSE 0 END) as disabled_users,
                COUNT(DISTINCT roles) as unique_roles
            FROM users
        ''')
        user_stats = dict(app.state.db.cursor.fetchone())

        app.state.db.cursor.execute('''
            SELECT 
                COUNT(*) as total_logins,
                SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) as successful_logins,
                SUM(CASE WHEN success = FALSE THEN 1 ELSE 0 END) as failed_logins,
                MIN(login_time) as first_login,
                MAX(login_time) as last_login
            FROM login_history
        ''')
        login_stats = dict(app.state.db.cursor.fetchone())

        app.state.db.cursor.execute('''
            SELECT 
                COUNT(*) as total_api_keys,
                SUM(CASE WHEN revoked = TRUE THEN 1 ELSE 0 END) as revoked_keys,
                SUM(CASE WHEN expires_at < ? THEN 1 ELSE 0 END) as expired_keys
            FROM api_keys
        ''', (datetime.now(),))
        api_key_stats = dict(app.state.db.cursor.fetchone())

        return {
            "success": True,
            "users": user_stats,
            "logins": login_stats,
            "api_keys": api_key_stats,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logging.error(f"Erreur get_admin_stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/admin/users", response_model=List[User], tags=["Admin"])
async def get_all_users(
    current_user: UserInDB = Depends(require_role("admin"))
):
    """Liste tous les utilisateurs (admin seulement)"""
    try:
        app.state.db.cursor.execute("SELECT username, email, full_name, disabled, roles, created_at, last_login FROM users")
        rows = app.state.db.cursor.fetchall()

        users = []
        for row in rows:
            user_dict = dict(row)
            user_dict["roles"] = json.loads(user_dict.get("roles", "[]"))
            users.append(User(**user_dict))

        return users

    except Exception as e:
        logging.error(f"Erreur get_all_users: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# IMPLEMENTATIONS INTERNES
# ============================================================================

async def get_sports_impl(
    category: Optional[int] = None,
    live_only: bool = False,
    include_virtual: bool = True,
    skip_cache: bool = False
):
    """Implémentation interne de get_sports"""
    try:
        endpoint = "live_sports" if live_only else "prematch_sports"
        cache_key = f"sports:{endpoint}:cat_{category}:virtual_{include_virtual}"

        if not skip_cache:
            cached = await app.state.cache.get(cache_key)
            if cached:
                return {
                    "success": True,
                    "data": cached,
                    "from_cache": True,
                    "timestamp": datetime.now().isoformat()
                }

        sports_data = await app.state.collector.fetch_endpoint_async(endpoint)
        if not sports_data or not sports_data.get("Value"):
            raise HTTPException(status_code=404, detail="Aucun sport trouvé")

        sports = sports_data["Value"]
        filtered_sports = []

        for sport in sports:
            if category and sport.get("CID") != category:
                continue

            if not include_virtual and sport.get("ICY", False):
                continue

            normalized = app.state.processor.normalize_sport(sport)
            filtered_sports.append(normalized)

        await app.state.cache.set(cache_key, filtered_sports, ttl=Config.CACHE_TTL['sports'])

        return {
            "success": True,
            "data": filtered_sports,
            "from_cache": False,
            "count": len(filtered_sports),
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Erreur get_sports_impl: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def get_events_impl(
    sport_id: Optional[int] = None,
    competition_id: Optional[int] = None,
    status: Optional[MatchStatus] = None,
    country_id: Optional[int] = None,
    is_virtual: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
    skip_cache: bool = False
):
    """Implémentation interne de get_events"""
    try:
        endpoint = "live_odds" if status == MatchStatus.LIVE else "prematch_odds"

        cache_key_parts = [
            f"events:{endpoint}",
            f"sport_{sport_id}" if sport_id else "all_sports",
            f"comp_{competition_id}" if competition_id else "all_comps",
            f"status_{status.value}" if status else "all_status",
            f"country_{country_id}" if country_id else "all_countries",
            f"virtual_{is_virtual}" if is_virtual is not None else "all_types",
            f"limit_{limit}",
            f"offset_{offset}"
        ]
        cache_key = ":".join(cache_key_parts)

        if not skip_cache:
            cached = await app.state.cache.get(cache_key)
            if cached:
                return {
                    "success": True,
                    "data": cached,
                    "from_cache": True,
                    "timestamp": datetime.now().isoformat()
                }

        events_data = await app.state.collector.fetch_endpoint_async(endpoint)
        if not events_data or not events_data.get("Value"):
            return {
                "success": True,
                "data": [],
                "from_cache": False,
                "count": 0,
                "timestamp": datetime.now().isoformat()
            }

        events = events_data["Value"]
        filtered_events = []

        for event_data in events:
            if sport_id and event_data.get("SI") != sport_id:
                continue
            if competition_id and event_data.get("CI") != competition_id:
                continue
            if status and event_data.get("SS") != status.value:
                continue
            if country_id and event_data.get("COI") != country_id:
                continue
            if is_virtual is not None:
                event_is_virtual = event_data.get("ICY", False)
                if is_virtual != event_is_virtual:
                    continue

            normalized = app.state.processor.normalize_event(event_data)
            if normalized:
                filtered_events.append(normalized)

        total_count = len(filtered_events)
        paginated_events = filtered_events[offset:offset + limit]

        await app.state.cache.set(cache_key, paginated_events, ttl=Config.CACHE_TTL['events'])

        return {
            "success": True,
            "data": paginated_events,
            "from_cache": False,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": offset + limit < total_count
            },
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logging.error(f"Erreur get_events_impl: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# WEBSOCKET ENDPOINTS AUTHENTIFIÉS
# ============================================================================

@app.websocket("/ws/events")
async def websocket_events_auth(websocket: WebSocket):
    """WebSocket authentifié pour les mises à jour d'événements"""
    await websocket.accept()
    
    params = {}
    try:
        query_string = str(websocket.query_params)
        if query_string:
            params = dict(param.split("=") for param in query_string.split("&") if "=" in param)
    except:
        pass
    
    token = params.get("token")
    api_key = params.get("api_key")
    
    user = await WebSocketAuthenticator.authenticate_websocket(
        websocket, token, api_key
    )
    
    if not user:
        return
    
    connection_id = str(uuid.uuid4())
    
    try:
        pubsub = await app.state.cache.subscribe("events:live")
        if not pubsub:
            await websocket.close(code=1011, reason="Redis non disponible")
            return
        
        await websocket.send_json({
            "type": "connected",
            "connection_id": connection_id,
            "user": user.username,
            "roles": user.roles,
            "message": "Connecté au service d'événements",
            "timestamp": datetime.now().isoformat()
        })
        
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    await websocket.send_json({
                        "type": "event_update",
                        "data": data,
                        "channel": message["channel"],
                        "timestamp": datetime.now().isoformat()
                    })
                except json.JSONDecodeError:
                    continue
                
    except WebSocketDisconnect:
        logging.info(f"WebSocket déconnecté: {connection_id}")
    except Exception as e:
        logging.error(f"WebSocket error: {e}")
    finally:
        if pubsub:
            await pubsub.unsubscribe()

@app.websocket("/ws/events/{event_id}")
async def websocket_event_detail_auth(websocket: WebSocket, event_id: int):
    """WebSocket authentifié pour un événement spécifique"""
    await websocket.accept()
    
    params = {}
    try:
        query_string = str(websocket.query_params)
        if query_string:
            params = dict(param.split("=") for param in query_string.split("&") if "=" in param)
    except:
        pass
    
    token = params.get("token")
    api_key = params.get("api_key")
    
    user = await WebSocketAuthenticator.authenticate_websocket(
        websocket, token, api_key
    )
    
    if not user:
        return
    
    try:
        event_data = await app.state.cache.get(f"event:{event_id}")
        if not event_data:
            await websocket.send_json({
                "type": "error",
                "message": f"Événement {event_id} non trouvé",
                "timestamp": datetime.now().isoformat()
            })
            await websocket.close(code=1008, reason="Événement non trouvé")
            return
        
        await websocket.send_json({
            "type": "initial",
            "event_id": event_id,
            "user": user.username,
            "data": event_data,
            "timestamp": datetime.now().isoformat()
        })
        
        pubsub = await app.state.cache.subscribe(f"event:{event_id}:updates")
        if not pubsub:
            await websocket.close(code=1011, reason="Redis non disponible")
            return
        
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    await websocket.send_json({
                        "type": "update",
                        "event_id": event_id,
                        "data": data,
                        "timestamp": datetime.now().isoformat()
                    })
                except json.JSONDecodeError:
                    continue
                
    except WebSocketDisconnect:
        logging.info(f"WebSocket déconnecté pour event {event_id}")
    except Exception as e:
        logging.error(f"WebSocket error for event {event_id}: {e}")
    finally:
        if pubsub:
            await pubsub.unsubscribe()

# ============================================================================
# AUTH DOCUMENTATION
# ============================================================================

@app.get("/api/v1/auth/docs", tags=["Authentication"])
async def auth_documentation():
    """Documentation de l'authentification"""
    return {
        "authentication_methods": {
            "jwt": {
                "description": "JSON Web Token (recommandé)",
                "endpoints": {
                    "login": {
                        "url": "/api/v1/auth/login",
                        "method": "POST",
                        "body": "form-data avec username et password",
                        "response": "Token JWT avec access_token et refresh_token"
                    },
                    "refresh": {
                        "url": "/api/v1/auth/refresh",
                        "method": "POST",
                        "body": "refresh_token",
                        "response": "Nouveau access_token"
                    },
                    "logout": {
                        "url": "/api/v1/auth/logout",
                        "method": "POST",
                        "headers": "Authorization: Bearer {token}",
                        "response": "Message de déconnexion"
                    }
                },
                "usage": "Ajouter l'en-tête: Authorization: Bearer {access_token}"
            },
            "api_key": {
                "description": "Clé API simple",
                "endpoints": {
                    "create": {
                        "url": "/api/v1/auth/api-keys",
                        "method": "POST",
                        "headers": "Authorization: Bearer {token}",
                        "body": "name et expires_days (optionnel)",
                        "response": "Clé API générée"
                    },
                    "list": {
                        "url": "/api/v1/auth/api-keys",
                        "method": "GET",
                        "headers": "Authorization: Bearer {token}",
                        "response": "Liste des clés API"
                    },
                    "revoke": {
                        "url": "/api/v1/auth/api-keys/{key_hash}",
                        "method": "DELETE",
                        "headers": "Authorization: Bearer {token}",
                        "response": "Message de révocation"
                    }
                },
                "usage": "Ajouter l'en-tête: Authorization: Bearer {api_key}"
            }
        },
        "webSocket_authentication": {
            "description": "Authentification via query parameters",
            "methods": {
                "jwt": "ws://host:port/ws/events?token={jwt_token}",
                "api_key": "ws://host:port/ws/events?api_key={api_key}"
            }
        },
        "default_credentials": {
            "admin": {
                "username": "admin",
                "password": "admin123",
                "roles": ["admin", "user"]
            }
        },
        "security_notes": [
            "En production, changez SECRET_KEY dans Config",
            "Utilisez HTTPS pour toutes les communications",
            "Limitez les tentatives de connexion échouées",
            "Stockez les mots de passe avec bcrypt",
            "Utilisez des tokens JWT avec expiration courte"
        ]
    }

# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Fonction principale mise à jour"""
    print("""
    🚀 1xBet Data Collector API - Version 100% Complète avec Authentification
    ===========================================================================

    🔐 NOUVEAU : Système d'authentification complet
    • ✅ JWT Tokens (access + refresh)
    • ✅ Clés API avec gestion complète
    • ✅ Rôles utilisateurs (user, admin)
    • ✅ WebSocket authentifiés
    • ✅ Rate limiting
    • ✅ Historique des connexions

    🆕 NOUVEAUX ENDPOINTS API:
    • /api/v1/express-day           - Paris express du jour
    • /api/v1/championships         - Liste des championnats
    • /api/v1/game-results          - Résultats des matchs
    • /api/v1/top-games             - Matchs les plus populaires
    • /api/v1/sports-simple         - Liste simplifiée des sports
    • /api/v1/multi-endpoint        - Multiple endpoints en une requête

    📊 Features IMPLÉMENTÉES:
    • ✅ Structure COMPLÈTE des données (100% documentation)
    • ✅ Tables de référence COMPLÈTES (CID, SS, KI, G, etc.)
    • ✅ Schémas Pydantic COMPLETS (tous les champs)
    • ✅ Traitement des données COMPLET (PS, ST, MIS, WP, etc.)
    • ✅ API REST COMPLÈTE avec authentification
    • ✅ WebSocket temps réel avec auth
    • ✅ Cache Redis + Persistance SQLite
    • ✅ Monitoring et alertes
    • ✅ Analyse des données
    • ✅ Validation complète

    🎯 Endpoints d'authentification:
    • /api/v1/auth/login           - Connexion JWT
    • /api/v1/auth/register        - Inscription
    • /api/v1/auth/refresh         - Rafraîchir token
    • /api/v1/auth/logout          - Déconnexion
    • /api/v1/auth/api-keys        - Gestion clés API
    • /api/v1/auth/profile         - Profil utilisateur
    • /api/v1/auth/docs            - Documentation auth

    🎯 Endpoints API publics:
    • /api/v1/sports               - Sports (publique)
    • /api/v1/events               - Événements (publique)
    • /api/v1/events/{id}          - Détail événement
    • /api/v1/analysis             - Analyse des données
    • /health                      - Health check

    🎯 Endpoints API protégés:
    • /api/v1/protected/sports     - Sports (auth requise)
    • /api/v1/protected/events     - Événements (auth requise)
    • /api/v1/admin/*              - Endpoints admin

    🎯 WebSocket authentifiés:
    • /ws/events?token={jwt}       - Tous événements
    • /ws/events/{id}?api_key={key}- Événement spécifique

    📡 Accès:
    • API: http://localhost:8000
    • Docs: http://localhost:8000/docs
    • Redoc: http://localhost:8000/redoc

    🔐 Identifiants par défaut:
    • Username: admin
    • Password: admin123

    💾 Stockage:
    • Redis: localhost:6379
    • SQLite: ~/1xbet_data/1xbet.db (avec tables users)

    🚀 Démarrage...
    """)

# ============================================================================
# EXÉCUTION
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Configuration du logging pour Railway
    logging.basicConfig(
        level=logging.INFO if Config.DEBUG else logging.WARNING,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    # Message de démarrage
    print(f"""
    🚀 1xBet Data Collector API - Railway Edition
    =============================================
    
    🔧 Configuration:
    • Port: {Config.PORT}
    • Redis: {'Connecté' if Config.REDIS_URL or Config.REDIS_HOST else 'Non configuré'}
    • SQLite: {Config.SQLITE_DB_PATH}
    • Debug: {Config.DEBUG}
    
    📡 URLs:
    • Local: http://localhost:{Config.PORT}
    • Docs: http://localhost:{Config.PORT}/docs
    • Health: http://localhost:{Config.PORT}/health
    
    🚄 Démarrage sur Railway...
    """)
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=Config.PORT,
        reload=Config.DEBUG,
        log_level="info" if Config.DEBUG else "warning"
    )
