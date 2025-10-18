#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SISTEMA PRIVACY PROTECTION COMPLETO - VERSIONE FINALE CON CONSISTENCY CHECKS
FIX DEFINITIVI:
1. Consistency check postumo per titoli
2. Consistency check per metadati protetti
3. Anonimizzazione solo metadati autori (whitelist approach)
"""

import pandas as pd
import requests
import sys
import os
import json
import time
import asyncio
import aiohttp
import logging
from dataclasses import dataclass
import threading
from typing import List, Dict, Tuple, Optional, Set
import random
import traceback
from asyncio import Semaphore
import psutil
import gc
from urllib.parse import quote, urlparse  
import signal
import shutil
import socket
import hashlib
import datetime
from pathlib import Path
import subprocess

try:
    from tqdm import tqdm
    from tqdm.asyncio import tqdm as atqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("⚠️ tqdm non disponibile - installa con: pip install tqdm")

# === CONFIGURAZIONE COMPLETA ===
BLACKLIST_FILE = "blacklist.xlsx"
WHITELIST_FILE = "whitelist.xlsx"
BLAZEGRAPH_UPDATE_ENDPOINT = "http://localhost:10214/blazegraph/sparql"
GRAPH_URI = "http://ficlit.unibo.it/ArchivioEvangelisti/works"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
RDF_VALUE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#value"
UPDATED_RELATIONS_GRAPH = "http://ficlit.unibo.it/ArchivioEvangelisti/updated_relations"

NAMED_GRAPH_URIS = {
    "RS1_RS1": "http://ficlit.unibo.it/ArchivioEvangelisti/structure/RS1_RS1",
    "RS1_RS2": "http://ficlit.unibo.it/ArchivioEvangelisti/structure/RS1_RS2",
    "RS1_RS3": "http://ficlit.unibo.it/ArchivioEvangelisti/structure/RS1_RS3",
}

TECHNICAL_METADATA_GRAPH_URIS = [
    "http://ficlit.unibo.it/ArchivioEvangelisti/AT_TechMeta_floppy",
    "http://ficlit.unibo.it/ArchivioEvangelisti/AT_TechMeta_hd",
    "http://ficlit.unibo.it/ArchivioEvangelisti/AT_TechMeta_hdesterno",
    "http://ficlit.unibo.it/ArchivioEvangelisti/ET_TechMeta_hd",
    "http://ficlit.unibo.it/ArchivioEvangelisti/ET_TechMeta_floppy",
    "http://ficlit.unibo.it/ArchivioEvangelisti/ET_TechMeta_hdesterno",
    "http://ficlit.unibo.it/ArchivioEvangelisti/FS_TechMeta_hd",
    "http://ficlit.unibo.it/ArchivioEvangelisti/FS_TechMeta_hdesterno",
    "http://ficlit.unibo.it/ArchivioEvangelisti/FS_TechMeta_floppy"
]

BLAZEGRAPH_POSSIBLE_PATHS = [
    "/Users/luciagiagnolini/Documents/GitHub/researchspace/runtime-data/blazegraph.jnl",
    "/Users/luciagiagnolini/Documents/GitHub/researchspace/runtime-data",
    os.path.expanduser("~/Documents/GitHub/researchspace/runtime-data/blazegraph.jnl"),
    os.path.expanduser("~/Documents/GitHub/researchspace/runtime-data"),
    "/var/lib/blazegraph", "/opt/blazegraph/data", "/usr/local/blazegraph",
    "blazegraph.jnl", "./blazegraph.jnl", "../blazegraph.jnl", 
    "data/blazegraph.jnl", "/tmp/blazegraph.jnl",
    os.path.expanduser("~/blazegraph.jnl"), os.path.expanduser("~/data/blazegraph.jnl")
]

BACKUP_DIR = "./blazegraph_backups"
MAX_BACKUP_COPIES = 5
MIN_FREE_SPACE_GB = 2

# === CONFIGURAZIONE PERFORMANCE ===
MAX_WORKERS = 20
BATCH_SIZE = 500
ENTITY_BATCH_SIZE = 200
QUERY_TIMEOUT = 300
UPDATE_TIMEOUT = 180
MAX_RETRIES = 2
CONNECTION_POOL_SIZE = 50
MEMORY_LIMIT_MB = 16384
OPERATION_DELAY = 0.01
BATCH_DELAY = 0.05
PARALLEL_BATCHES = 8
SMART_CHUNK_SIZE = 2000
HIERARCHY_CHUNK_SIZE = 500
TECH_METADATA_CHUNK_SIZE = 1000
INSTANTIATION_BATCH_SIZE = 300
MEMORY_CHECK_INTERVAL = 100
RECONNECTION_DELAY = 5.0
TECH_METADATA_BATCH_SIZE = 50
CHECKPOINT_INTERVAL = 2000
CHECKPOINT_FILE = "fast_title_structure_checkpoint.json"

PROGRESS_UPDATE_INTERVAL = 5
STATS_LOG_INTERVAL = 30
MEMORY_LOG_INTERVAL = 60

OMITTED_LABEL = "Anonymized information"
OMITTED_INFORMATION_VALUE = "yes"
NOT_OMITTED_INFORMATION_VALUE = "no"

AUTHORIZED_AUTHORS = {
    "valerio", "evangelisti", "valerio evangelisti", "eymerich", 
    "evang", "evangelisti valerio", "eval"
}

NEUTRAL_AUTHOR_PATTERNS = {
    'admin', 'administrator', 'user', 'owner', 'microsoft', 'windows', 'system', 
    'desktop', 'computer', 'pc-', 'Curtiss, Harriette Augusta', 'settore', 
    'Dr. Otto Henne', 'BlackBerry Limited dc:creator', 'Denifle, Heinric', 
    'Ramón de Luanco', 'Jean Baptiste abate Christoph', 'Alexander Wilder', 
    'sconosciuto', 'Adobe InDesign', 'Office 98', 'Montet, Édouard Louis', 
    'Stowell, Myron R', 'QuarkXPress', 'Raymond Lulle', 'Kirtas Technologies', 
    'S. P. Melgunov', 'Frankenstein', 'Iaroslavski, E.', 'disco', 'Louis Blanc', 
    'Vaucher, Robert', 'Abram Herbert Lewis', 'utente', 'Westcott, W', 'ds author', 
    'Rose, Elzéar', 'Randall, Edward', 'mac', 'François Tommy Perrens', 'writer', 
    'Peeke, Margaret', "win", "Bittard des Portes", "Andrews, Herbert Tom, 1864-1928", 
    "Adobe InDesign", 
}

AUTHOR_METADATA_TYPES = {
    "Creator", "meta:last-author", "dc:creator", "Author", "LastModifiedBy",
    "dcterms:creator", "meta:author", "LastAuthor"
}

AUTHOR_METADATA_TYPES_TO_CHECK = {
    "Creator", "dc:creator", "Author", "LastModifiedBy", "meta:last-author"
}

# ✅ Lista metadati protetti (per reference e verifiche)
PROTECTED_TECH_METADATA_TYPES = {
    "FileSize", "Software", "CreateDate", "dcterms:modified", "st_mtime", 
    "Content-Length", "MediaModifyDate", "MediaCreateDate", "dcterms:created",
    "FileModifyDate", "FileAccessDate", "File Size", "st_size", "MIMEType",
    "hierarchyDepth", "FileType", "FileTypeExtension", "Content-Type",
    "file_type", "st_atime", "st_ctime", "st_blksize", "st_blocks",
    "st_dev", "st_gid", "st_ino", "st_mode", "st_nlink", "st_uid" , "FilePermissions", "FileInodeChangeDate"
}

# === NAMESPACE DEFINITIONS ===
RICO_TITLE = "https://www.ica.org/standards/RiC/ontology#title"
RICO_HAS_OR_HAD_TITLE = "https://www.ica.org/standards/RiC/ontology#hasOrHadTitle"
RICO_IS_RELATED_TO = "https://www.ica.org/standards/RiC/ontology#isRelatedTo"
RICO_IS_INCLUDED_IN = "https://www.ica.org/standards/RiC/ontology#isOrWasIncludedIn"
LRMOO_F1_WORK = "http://iflastandards.info/ns/lrm/lrmoo/F1_Work"
RICO_RECORD = "https://www.ica.org/standards/RiC/ontology#Record"
RICO_RECORDSET = "https://www.ica.org/standards/RiC/ontology#RecordSet"
RICO_INSTANTIATION = "https://www.ica.org/standards/RiC/ontology#Instantiation"
RICO_HAS_INSTANTIATION = "https://www.ica.org/standards/RiC/ontology#hasOrHadInstantiation"
RICO_BODI_OMITTED = "http://w3id.org/bodi#redactedInformation"
RICO_BODI_HAS_TECHNICAL_METADATA = "http://w3id.org/bodi#hasTechnicalMetadata"
RICO_BODI_TECHNICAL_METADATA = "http://w3id.org/bodi#TechnicalMetadata"
RICO_BODI_HAS_TECHNICAL_METADATA_TYPE = "http://w3id.org/bodi#hasTechnicalMetadataType"
RICO_BODI_TECHNICAL_METADATA_TYPE = "http://w3id.org/bodi#TechnicalMetadataType"

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fast_title_anonymization.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# === CLASSE MONITORING ===
class ProgressMonitor:
    def __init__(self, total_entities=0):
        self.total_entities = total_entities
        self.processed_entities = 0
        self.failed_entities = 0
        self.start_time = time.time()
        self.phase_start_time = time.time()
        self.last_update = time.time()
        self.last_stats_log = time.time()
        self.last_memory_log = time.time()
        
        self.records_processed = 0
        self.recordsets_processed = 0
        self.titles_processed = 0
        self.instantiations_processed = 0
        self.tech_metadata_processed = 0
        
        self.query_count = 0
        self.update_count = 0
        
        self._lock = threading.Lock()
        self.pbar = None
        
    def initialize_progress_bar(self, total, description="Processing"):
        if TQDM_AVAILABLE:
            self.pbar = tqdm(
                total=total,
                desc=description,
                unit="entities",
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                colour='green'
            )
    
    def update(self, n=1, success=True):
        with self._lock:
            if success:
                self.processed_entities += n
            else:
                self.failed_entities += n
            
            if self.pbar:
                self.pbar.update(n)
            
            current_time = time.time()
            if current_time - self.last_update >= PROGRESS_UPDATE_INTERVAL:
                self._log_progress()
                self.last_update = current_time
            
            if current_time - self.last_stats_log >= STATS_LOG_INTERVAL:
                self._log_stats()
                self.last_stats_log = current_time
            
            if current_time - self.last_memory_log >= MEMORY_LOG_INTERVAL:
                self._log_memory()
                self.last_memory_log = current_time
    
    def _log_progress(self):
        elapsed = time.time() - self.start_time
        if self.processed_entities > 0:
            rate = self.processed_entities / elapsed
            remaining = (self.total_entities - self.processed_entities) / rate if rate > 0 else 0
            
            logger.info(
                f"📊 PROGRESSO: {self.processed_entities}/{self.total_entities} "
                f"({self.processed_entities/max(self.total_entities,1)*100:.1f}%) | "
                f"Velocità: {rate:.1f} ent/s | ETA: {remaining/60:.1f} min"
            )
    
    def _log_stats(self):
        logger.info(
            f"📈 STATS: Record={self.records_processed} | RecordSet={self.recordsets_processed} | "
            f"Titles={self.titles_processed} | Inst={self.instantiations_processed} | "
            f"TechMeta={self.tech_metadata_processed} | Queries={self.query_count} | Failed={self.failed_entities}"
        )
    
    def _log_memory(self):
        try:
            process = psutil.Process()
            mem_mb = process.memory_info().rss / 1024 / 1024
            mem_percent = process.memory_percent()
            logger.info(f"💾 MEMORIA: {mem_mb:.1f} MB ({mem_percent:.1f}%)")
        except:
            pass
    
    def start_phase(self, phase_name):
        self.phase_start_time = time.time()
        logger.info(f"🚀 INIZIO FASE: {phase_name}")
    
    def end_phase(self, phase_name):
        phase_time = time.time() - self.phase_start_time
        logger.info(f"✅ FINE FASE: {phase_name} - Tempo: {phase_time:.1f}s")
    
    def increment_counter(self, counter_name, n=1):
        with self._lock:
            if counter_name == 'records':
                self.records_processed += n
            elif counter_name == 'recordsets':
                self.recordsets_processed += n
            elif counter_name == 'titles':
                self.titles_processed += n
            elif counter_name == 'instantiations':
                self.instantiations_processed += n
            elif counter_name == 'tech_metadata':
                self.tech_metadata_processed += n
            elif counter_name == 'queries':
                self.query_count += n
            elif counter_name == 'updates':
                self.update_count += n
    
    def close(self):
        if self.pbar:
            self.pbar.close()
    
    def get_summary(self):
        elapsed = time.time() - self.start_time
        rate = self.processed_entities / elapsed if elapsed > 0 else 0
        
        return {
            'total_time': elapsed,
            'processed': self.processed_entities,
            'failed': self.failed_entities,
            'rate': rate,
            'records': self.records_processed,
            'recordsets': self.recordsets_processed,
            'titles': self.titles_processed,
            'instantiations': self.instantiations_processed,
            'tech_metadata': self.tech_metadata_processed,
            'queries': self.query_count,
            'updates': self.update_count
        }

monitor = None

# === BACKUP FUNCTIONS (mantenute per brevità) ===
def ensure_backup_directory():
    backup_path = Path(BACKUP_DIR)
    backup_path.mkdir(exist_ok=True)
    logger.info(f"📁 Directory backup: {backup_path.absolute()}")
    return backup_path

def find_blazegraph_journal():
    logger.info("🔍 Ricerca journal Blazegraph...")
    for path in BLAZEGRAPH_POSSIBLE_PATHS:
        journal_path = Path(path)
        if journal_path.exists() and journal_path.is_file():
            if journal_path.suffix == '.jnl' or 'blazegraph' in journal_path.name.lower():
                logger.info(f"   ✅ Journal trovato: {journal_path.absolute()}")
                return journal_path
    logger.error("   ❌ Journal NON trovato!")
    return None

def check_disk_space(source_path, backup_dir):
    try:
        source_size = source_path.stat().st_size
        source_size_gb = source_size / (1024**3)
        statvfs = os.statvfs(backup_dir)
        free_space = statvfs.f_frsize * statvfs.f_bavail
        free_space_gb = free_space / (1024**3)
        required_space_gb = source_size_gb + MIN_FREE_SPACE_GB
        logger.info(f"   📊 Journal: {source_size_gb:.2f} GB | Libero: {free_space_gb:.2f} GB")
        if free_space_gb < required_space_gb:
            logger.error(f"   ❌ Spazio insufficiente!")
            return False
        logger.info(f"   ✅ Spazio sufficiente")
        return True
    except Exception as e:
        logger.error(f"   ❌ Errore: {e}")
        return False

def calculate_file_hash(file_path, chunk_size=8192):
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        return None

def cleanup_old_backups(backup_dir):
    try:
        backup_files = list(backup_dir.glob("blazegraph_backup_*.jnl"))
        backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        if len(backup_files) >= MAX_BACKUP_COPIES:
            for old_backup in backup_files[MAX_BACKUP_COPIES-1:]:
                old_backup.unlink()
    except:
        pass

def create_blazegraph_backup():
    logger.info("📄 BACKUP JOURNAL")
    try:
        journal_path = find_blazegraph_journal()
        if not journal_path:
            return False
        backup_dir = ensure_backup_directory()
        if not check_disk_space(journal_path, backup_dir):
            return False
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"blazegraph_backup_{timestamp}.jnl"
        logger.info("   🔐 Calcolo hash sorgente...")
        original_hash = calculate_file_hash(journal_path)
        if not original_hash:
            return False
        logger.info("   📦 Copia in corso...")
        start_time = time.time()
        shutil.copy2(journal_path, backup_path)
        copy_time = time.time() - start_time
        logger.info("   🔐 Verifica integrità...")
        backup_hash = calculate_file_hash(backup_path)
        if backup_hash != original_hash:
            logger.error("❌ Hash non corrispondenti!")
            backup_path.unlink()
            return False
        cleanup_old_backups(backup_dir)
        logger.info(f"🎉 BACKUP OK in {copy_time:.1f}s")
        return True
    except Exception as e:
        logger.error(f"💥 Errore backup: {e}")
        return False

# === UTILITY FUNCTIONS (compatte per brevità) ===
def escape_sparql_string(value):
    if not value:
        return '""'
    value = str(value).strip()
    value = value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
    return f'"{value}"'

def check_memory_usage():
    try:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        if memory_mb > MEMORY_LIMIT_MB:
            gc.collect()
        return memory_mb
    except:
        return 0

def is_author_acceptable(combined_text):
    if not combined_text or not combined_text.strip():
        return True
    text_lower = combined_text.lower().strip()
    if any(auth.lower() in text_lower for auth in AUTHORIZED_AUTHORS):
        return True
    return any(neutral.lower() in text_lower for neutral in NEUTRAL_AUTHOR_PATTERNS)

# Altre utility compatte...
def _flexible_neutral_matching(text_lower):
    for neutral_pattern in NEUTRAL_AUTHOR_PATTERNS:
        if neutral_pattern.lower() in text_lower:
            return True
    return False

def _partial_name_similarity(text, pattern):
    if not text or not pattern or len(text) < 3 or len(pattern) < 3:
        return False
    return text.startswith(pattern[:3]) or pattern.startswith(text[:3])

def _word_similarity(word1, word2):
    if len(word1) < 3 or len(word2) < 3:
        return False
    return word1 == word2 or word1 in word2 or word2 in word1

def _remove_common_suffixes(word):
    suffixes = ['er', 'or', 'ar', 'o', 'a', 'e', 'i']
    for suffix in suffixes:
        if word.endswith(suffix) and len(word) > len(suffix) + 2:
            return word[:-len(suffix)]
    return word

def _simple_edit_distance(s1, s2):
    if len(s1) > len(s2):
        s1, s2 = s2, s1
    distances = range(len(s1) + 1)
    for i2, c2 in enumerate(s2):
        distances_ = [i2 + 1]
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1])
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
        distances = distances_
    return distances[-1]

# === TRACKER ===
class CompleteTitleStructureTracker:
    def __init__(self):
        self.processed_entities = set()
        self.processed_instantiations = set() 
        self.processed_technical_metadata = set()
        self.processed_titles = set()
        self.anonymization_attempts = {}
        self._lock = threading.Lock()
    
    def mark_entity_processed(self, entity_uri):
        with self._lock:
            self.processed_entities.add(entity_uri)
            self.anonymization_attempts[entity_uri] = self.anonymization_attempts.get(entity_uri, 0) + 1
    
    def is_entity_processed(self, entity_uri):
        with self._lock:
            return entity_uri in self.processed_entities
    
    def mark_instantiation_processed(self, inst_uri):
        with self._lock:
            self.processed_instantiations.add(inst_uri)
    
    def mark_title_processed(self, title_uri):
        with self._lock:
            self.processed_titles.add(title_uri)
    
    def get_duplication_report(self):
        with self._lock:
            duplicates = {uri: count for uri, count in self.anonymization_attempts.items() if count > 1}
            return {
                'total_entities_processed': len(self.processed_entities),
                'total_instantiations_processed': len(self.processed_instantiations),
                'total_titles_processed': len(self.processed_titles),
                'duplication_count': len(duplicates)
            }

tracker = CompleteTitleStructureTracker()

# === CLIENT SPARQL ===
class FastSPARQLClient:
    def __init__(self):
        self.session = None
        self.connector = None
        self.query_cache = {}
        self.stats = {'queries': 0, 'cache_hits': 0}
        self._lock = threading.Lock()
        
    async def __aenter__(self):
        await self._create_high_performance_session()
        return self
        
    async def __aexit__(self, *args):
        await self._cleanup()
        
    async def _create_high_performance_session(self):
        if self.session and not self.session.closed:
            await self.session.close()
        if self.connector:
            await self.connector.close()

        self.connector = aiohttp.TCPConnector(
            limit=CONNECTION_POOL_SIZE,
            limit_per_host=CONNECTION_POOL_SIZE // 2,
            enable_cleanup_closed=True,
            keepalive_timeout=90,
            use_dns_cache=True
        )
        
        timeout = aiohttp.ClientTimeout(total=QUERY_TIMEOUT, connect=10, sock_read=60)
        
        self.session = aiohttp.ClientSession(
            connector=self.connector,
            timeout=timeout,
            headers={'Keep-Alive': 'timeout=90, max=200', 'Connection': 'keep-alive'}
        )
        
        try:
            test_result = await self.query("SELECT (1 as ?test) {}", cache_enabled=False)
            if not test_result:
                raise Exception("Test query fallita")
            logger.info("✅ Connessione Blazegraph OK")
        except Exception as e:
            logger.error(f"❌ Test connessione fallito: {e}")
            await self._cleanup()
            raise

    async def _cleanup(self):
        try:
            if self.session and not self.session.closed:
                await self.session.close()
        except:
            pass
        try:
            if self.connector and not self.connector.closed:
                await self.connector.close()
        except:
            pass
        await asyncio.sleep(0.1)
        
    async def query(self, query: str, cache_enabled: bool = True, timeout: int = None) -> Optional[Dict]:
        await asyncio.sleep(OPERATION_DELAY)
        if monitor:
            monitor.increment_counter('queries')
        
        request_timeout = timeout or QUERY_TIMEOUT
        
        try:
            query_data = f"query={quote(query.strip())}"
            async with self.session.post(
                BLAZEGRAPH_UPDATE_ENDPOINT,
                data=query_data,
                headers={'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/sparql-results+json'},
                timeout=aiohttp.ClientTimeout(total=request_timeout)
            ) as response:
                if response.status == 200:
                    return await response.json()
                return None
        except:
            return None
            
    async def update(self, update_query: str, timeout: int = None) -> bool:
        await asyncio.sleep(OPERATION_DELAY)
        if monitor:
            monitor.increment_counter('updates')
        
        request_timeout = timeout or UPDATE_TIMEOUT
        
        try:
            async with self.session.post(
                BLAZEGRAPH_UPDATE_ENDPOINT,
                data=update_query.strip(),
                headers={'Content-Type': 'application/sparql-update', 'Accept': 'text/plain'},
                timeout=aiohttp.ClientTimeout(total=request_timeout)
            ) as response:
                return response.status in [200, 204]
        except:
            return False

# === FUNZIONI LETTURA ===
def read_xlsx_uris(xlsx_file_path, file_description):
    if not os.path.exists(xlsx_file_path):
        logger.info(f"File {file_description} non trovato")
        return []
    try:
        df = pd.read_excel(xlsx_file_path, engine='openpyxl')
        if df.shape[1] < 1:
            return []
        df_uris = df.iloc[:, 0].dropna()
        uri_list = []
        for uri_value in df_uris:
            uri_str = str(uri_value).strip()
            if uri_str.startswith('<') and uri_str.endswith('>'):
                uri_str = uri_str[1:-1]
            if uri_str and uri_str.startswith('http'):
                uri_list.append(uri_str)
        logger.info(f"Letti {len(uri_list)} URI")
        return uri_list
    except Exception as e:
        logger.error(f"Errore lettura: {e}")
        return []

def test_blazegraph_connection():
    try:
        test_query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o } LIMIT 1"
        response = requests.get(
            BLAZEGRAPH_UPDATE_ENDPOINT,
            params={'query': test_query},
            headers={'Accept': 'application/sparql-results+json'},
            timeout=60
        )
        if response.status_code == 200:
            result = response.json()
            count = int(result['results']['bindings'][0]['count']['value'])
            logger.info(f"✅ Blazegraph OK - {count:,} triple")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Errore connessione: {e}")
        return False

# === RECUPERO ENTITÀ (compatto) ===
async def complete_get_all_entities(client):
    logger.info("🚀 Recupero entità")
    entities = {'Record': [], 'RecordSet': [], 'entity_graphs': {}}
    all_graphs = list(NAMED_GRAPH_URIS.values())
    
    for graph_uri in all_graphs:
        graph_query = f"""
        PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
        SELECT ?entity ?type WHERE {{
            GRAPH <{graph_uri}> {{
                {{ ?entity a rico:Record . BIND("Record" as ?type) }}
                UNION
                {{ ?entity a rico:RecordSet . BIND("RecordSet" as ?type) }}
                FILTER(CONTAINS(STR(?entity), "ArchivioEvangelisti"))
            }}
        }}
        """
        result = await client.query(graph_query, cache_enabled=False)
        if result:
            for binding in result['results']['bindings']:
                entity_uri = binding['entity']['value']
                entity_type = binding['type']['value']
                entities[entity_type].append(entity_uri)
                entities['entity_graphs'][entity_uri] = graph_uri
    
    total = len(entities['Record']) + len(entities['RecordSet'])
    logger.info(f"✅ {total:,} entità")
    return entities

# === CLASSIFICAZIONE (versione compatta) ===
async def complete_find_work_relations(client, entity_uris):
    entities_with_works = set()
    batch_size = 500
    for i in range(0, len(entity_uris), batch_size):
        batch = entity_uris[i:i + batch_size]
        work_query = f"""
        PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
        SELECT DISTINCT ?entity WHERE {{
            VALUES ?entity {{ {' '.join([f'<{uri}>' for uri in batch])} }}
            ?entity rico:isRelatedTo ?work .
            ?work a lrmoo:F1_Work .
        }}
        """
        result = await client.query(work_query)
        if result and result.get('results', {}).get('bindings'):
            for binding in result['results']['bindings']:
                entities_with_works.add(binding['entity']['value'])
    logger.info(f"✅ {len(entities_with_works)} entità con opere")
    return entities_with_works

async def complete_find_hierarchy_relations(client, entity_uris):
    if not entity_uris:
        return set()
    hierarchy_entities = set()
    simple_query = """PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
    SELECT DISTINCT ?related WHERE {
        VALUES ?entity { %s }
        { ?related rico:isOrWasIncludedIn ?entity . }
        UNION { ?entity rico:isOrWasIncludedIn ?related . }
    }"""
    batch_size = 10
    for i in range(0, len(entity_uris), batch_size):
        batch = entity_uris[i:i + batch_size]
        query = simple_query % ' '.join([f'<{uri}>' for uri in batch])
        result = await client.query(query, timeout=120)
        if result and result.get('results', {}).get('bindings'):
            for binding in result['results']['bindings']:
                hierarchy_entities.add(binding['related']['value'])
    return hierarchy_entities - set(entity_uris)

async def complete_classification_logic(client, entities):
    logger.info("🎯 CLASSIFICAZIONE")
    all_entities = entities['Record'] + entities['RecordSet']
    
    entities_with_works = await complete_find_work_relations(client, all_entities)
    whitelist_uris = set(read_xlsx_uris(WHITELIST_FILE, "whitelist"))
    whitelist_entities = {uri for uri in whitelist_uris if uri in all_entities}
    blacklist_uris = set(read_xlsx_uris(BLACKLIST_FILE, "blacklist"))
    blacklist_entities = {uri for uri in blacklist_uris if uri in all_entities}
    
    protected_entities = entities_with_works.union(whitelist_entities)
    entities_to_anonymize = blacklist_entities
    
    if blacklist_entities:
        blacklist_hierarchy = await complete_find_hierarchy_relations(client, list(blacklist_entities))
        entities_to_anonymize.update(blacklist_hierarchy)
    
    entities_to_anonymize = entities_to_anonymize - protected_entities
    
    final_entities_to_anonymize = []
    final_entities_to_protect = []
    
    for record_uri in entities['Record']:
        if record_uri in protected_entities:
            final_entities_to_protect.append(record_uri)
        else:
            final_entities_to_anonymize.append((record_uri, "Record"))
    
    for recordset_uri in entities['RecordSet']:
        if recordset_uri in protected_entities:
            final_entities_to_protect.append(recordset_uri)
        elif recordset_uri in entities_to_anonymize:
            final_entities_to_anonymize.append((recordset_uri, "RecordSet"))
        else:
            final_entities_to_protect.append(recordset_uri)
    
    logger.info(f"✅ Anonimizzare: {len(final_entities_to_anonymize)} | Proteggere: {len(final_entities_to_protect)}")
    return final_entities_to_anonymize, final_entities_to_protect, {}

# === ANONIMIZZAZIONE BATCH ===
async def fast_batch_anonymize_entities(client, entities_batch, entities_dict):
    if not entities_batch:
        return 0, 0
    
    entities_by_graph = {}
    for entity_uri, entity_type in entities_batch:
        graph = entities_dict['entity_graphs'].get(entity_uri)
        if not graph:
            continue
        if graph not in entities_by_graph:
            entities_by_graph[graph] = {'Record': [], 'RecordSet': []}
        entities_by_graph[graph][entity_type].append(entity_uri)
    
    success_count = 0
    failed_count = 0
    
    for graph_uri, entities_by_type in entities_by_graph.items():
        # ============= GESTIONE RECORD =============
        if entities_by_type['Record']:
            record_uris = entities_by_type['Record']
            record_values = ' '.join([f'<{uri}>' for uri in record_uris])
            
            # ✅ QUERY UNIVERSALE - Non dipende dalla struttura dei grafi
            titles_query = f"""
            PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX bodi: <http://w3id.org/bodi#>
            
            DELETE {{ 
                GRAPH <{UPDATED_RELATIONS_GRAPH}> {{ 
                    ?title rdfs:label ?oldLabel . 
                }} 
            }}
            INSERT {{ 
                GRAPH <{UPDATED_RELATIONS_GRAPH}> {{ 
                    ?title rdfs:label "{OMITTED_LABEL}" . 
                }} 
            }}
            WHERE {{
                VALUES ?record {{ {record_values} }}
                
                # Cerca bodi:redactedInformation in QUALSIASI grafo
                GRAPH ?anyGraph {{
                    ?record bodi:redactedInformation "yes" .
                }}
                
                # Relazione e label in updated_relations
                
                    ?record rico:hasOrHadTitle ?title .
                    ?title a rico:Title .
                    ?title rdfs:label ?oldLabel .
                    FILTER(?oldLabel != "{OMITTED_LABEL}")
            }}
            """
            
            titles_success = await client.update(titles_query)
            
            # Record labels e properties
            records_update = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
            PREFIX bodi: <http://w3id.org/bodi#>
            
            DELETE {{
                GRAPH <{graph_uri}> {{
                    ?record rdfs:label ?oldLabel . 
                    ?record rico:title ?oldTitle . 
                    ?record bodi:redactedInformation ?oldOmitted .
                }}
            }}
            INSERT {{
                GRAPH <{graph_uri}> {{
                    ?record rdfs:label "{OMITTED_LABEL}" . 
                    ?record rico:title "{OMITTED_LABEL}" . 
                    ?record bodi:redactedInformation "{OMITTED_INFORMATION_VALUE}" .
                }}
            }}
            WHERE {{
                GRAPH <{graph_uri}> {{
                    VALUES ?record {{ {record_values} }}
                    OPTIONAL {{ ?record rdfs:label ?oldLabel }}
                    OPTIONAL {{ ?record rico:title ?oldTitle }}
                    OPTIONAL {{ ?record bodi:redactedInformation ?oldOmitted }}
                }}
            }}
            """
            
            records_success = await client.update(records_update)
            
            # ✅ Verifica ENTRAMBI i successi
            if titles_success and records_success:
                success_count += len(record_uris)
                for uri in record_uris:
                    tracker.mark_entity_processed(uri)
                if monitor:
                    monitor.increment_counter('records', len(record_uris))
                    monitor.increment_counter('titles', len(record_uris))
            else:
                failed_count += len(record_uris)
                logger.error(f"❌ Batch fallito: titles={titles_success}, records={records_success}")
        
        # ============= GESTIONE RECORDSET (uguale) =============
        if entities_by_type['RecordSet']:
            recordset_uris = entities_by_type['RecordSet']
            recordset_values = ' '.join([f'<{uri}>' for uri in recordset_uris])
            
            # ✅ Query universale per RecordSet
            recordset_titles_query = f"""
            PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX bodi: <http://w3id.org/bodi#>
            
            DELETE {{ 
                GRAPH <{UPDATED_RELATIONS_GRAPH}> {{ 
                    ?title rdfs:label ?oldLabel . 
                }} 
            }}
            INSERT {{ 
                GRAPH <{UPDATED_RELATIONS_GRAPH}> {{ 
                    ?title rdfs:label "{OMITTED_LABEL}" . 
                }} 
            }}
            WHERE {{
                VALUES ?recordset {{ {recordset_values} }}
                
                GRAPH ?anyGraph {{
                    ?recordset bodi:redactedInformation "yes" .
                }}
                
                GRAPH <{UPDATED_RELATIONS_GRAPH}> {{
                    ?recordset rico:hasOrHadTitle ?title .
                    ?title a rico:Title .
                    ?title rdfs:label ?oldLabel .
                    FILTER(?oldLabel != "{OMITTED_LABEL}")
                }}
            }}
            """
            
            recordset_titles_success = await client.update(recordset_titles_query)
            
            # RecordSet labels
            recordsets_update = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
            PREFIX bodi: <http://w3id.org/bodi#>
            
            DELETE {{ 
                GRAPH <{graph_uri}> {{ 
                    ?recordset rdfs:label ?oldLabel . 
                    ?recordset rico:title ?oldTitle .
                    ?recordset bodi:redactedInformation ?oldOmitted . 
                }} 
            }}
            INSERT {{ 
                GRAPH <{graph_uri}> {{ 
                    ?recordset rdfs:label "{OMITTED_LABEL}" . 
                    ?recordset rico:title "{OMITTED_LABEL}" .
                    ?recordset bodi:redactedInformation "{OMITTED_INFORMATION_VALUE}" . 
                }} 
            }}
            WHERE {{
                GRAPH <{graph_uri}> {{
                    VALUES ?recordset {{ {recordset_values} }}
                    OPTIONAL {{ ?recordset rdfs:label ?oldLabel }}
                    OPTIONAL {{ ?recordset rico:title ?oldTitle }}
                    OPTIONAL {{ ?recordset bodi:redactedInformation ?oldOmitted }}
                }}
            }}
            """
            
            recordsets_success = await client.update(recordsets_update)
            
            if recordset_titles_success and recordsets_success:
                success_count += len(recordset_uris)
                for uri in recordset_uris:
                    tracker.mark_entity_processed(uri)
                if monitor:
                    monitor.increment_counter('recordsets', len(recordset_uris))
                    monitor.increment_counter('titles', len(recordset_uris))
            else:
                failed_count += len(recordset_uris)
                logger.error(f"❌ Batch RecordSet fallito: titles={recordset_titles_success}, recordsets={recordsets_success}")
    
    return success_count, failed_count

async def fast_batch_anonymize_instantiations(client, entity_uris, graph_uri):
    inst_query = f"""
    PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
    SELECT DISTINCT ?instantiation WHERE {{
        GRAPH <{graph_uri}> {{
            VALUES ?entity {{ {' '.join([f'<{uri}>' for uri in entity_uris])} }}
            ?entity rico:hasOrHadInstantiation ?instantiation .
        }}
    }}
    """
    result = await client.query(inst_query)
    if not result or not result.get('results', {}).get('bindings'):
        return []
    
    inst_uris = [b['instantiation']['value'] for b in result['results']['bindings']]
    if not inst_uris:
        return []
    
    # ✅ CORRETTO: Senza GRAPH, cerca in qualsiasi grafo
    inst_update = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    DELETE {{ ?inst rdfs:label ?oldLabel . }}
    INSERT {{ ?inst rdfs:label "{OMITTED_LABEL}" . }}
    WHERE {{ 
        VALUES ?inst {{ {' '.join([f'<{uri}>' for uri in inst_uris])} }}
        OPTIONAL {{ ?inst rdfs:label ?oldLabel }}
    }}
    """
    await client.update(inst_update)
    
    for uri in inst_uris:
        tracker.mark_instantiation_processed(uri)
    if monitor:
        monitor.increment_counter('instantiations', len(inst_uris))
    return inst_uris
    

async def fast_batch_anonymize_technical_metadata(client, inst_uris):
    """✅ WHITELIST APPROACH: Anonimizza SOLO metadati autori"""
    author_types_to_anonymize = ', '.join([f'"{t}"' for t in AUTHOR_METADATA_TYPES_TO_CHECK])
    
    for tech_graph in TECHNICAL_METADATA_GRAPH_URIS:
        chunk_size = 200
        for i in range(0, len(inst_uris), chunk_size):
            chunk = inst_uris[i:i + chunk_size]
            
            tm_update = f"""
            PREFIX bodi: <http://w3id.org/bodi#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            DELETE {{ GRAPH <{tech_graph}> {{ ?tm rdf:value ?oldValue . ?tm rdfs:label ?oldLabel . }} }}
            INSERT {{ GRAPH <{tech_graph}> {{ ?tm rdf:value "{OMITTED_LABEL}" . ?tm rdfs:label "{OMITTED_LABEL}" . }} }}
            WHERE {{
                GRAPH <{tech_graph}> {{
                    VALUES ?inst {{ {' '.join([f'<{uri}>' for uri in chunk])} }}
                    ?inst bodi:hasTechnicalMetadata ?tm .
                    ?tm a bodi:TechnicalMetadata .
                    ?tm bodi:hasTechnicalMetadataType ?typeUri .
                    ?typeUri rdfs:label ?metadataType .
                    FILTER(?metadataType IN ({author_types_to_anonymize}))
                    OPTIONAL {{ ?tm rdf:value ?oldValue }}
                    OPTIONAL {{ ?tm rdfs:label ?oldLabel }}
                }}
            }}
            """
            await client.update(tm_update)

async def optimized_complete_anonymization(client, entities_to_anonymize, entities_dict):
    if not entities_to_anonymize:
        return
    
    global monitor
    monitor.start_phase("ANONIMIZZAZIONE")
    monitor.initialize_progress_bar(len(entities_to_anonymize), "Anonimizzazione")
    
    total_success = 0
    total_failed = 0
    
    batch_size = BATCH_SIZE
    batches = [entities_to_anonymize[i:i + batch_size] for i in range(0, len(entities_to_anonymize), batch_size)]
    
    for i in range(0, len(batches), PARALLEL_BATCHES):
        parallel_batches = batches[i:i + PARALLEL_BATCHES]
        tasks = [fast_batch_anonymize_entities(client, batch, entities_dict) for batch in parallel_batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, tuple):
                success, failed = result
                total_success += success
                total_failed += failed
                monitor.update(success, success=True)
                if failed > 0:
                    monitor.update(failed, success=False)
        await asyncio.sleep(BATCH_DELAY)
    
    monitor.end_phase("Fase 1")
    
    # Instantiation
    entities_by_graph = {}
    for entity_uri, entity_type in entities_to_anonymize:
        graph = entities_dict['entity_graphs'].get(entity_uri)
        if graph:
            if graph not in entities_by_graph:
                entities_by_graph[graph] = []
            entities_by_graph[graph].append(entity_uri)
    
    all_inst_uris = []
    for graph_uri, entity_uris in entities_by_graph.items():
        chunk_size = INSTANTIATION_BATCH_SIZE
        for i in range(0, len(entity_uris), chunk_size):
            chunk = entity_uris[i:i + chunk_size]
            inst_uris = await fast_batch_anonymize_instantiations(client, chunk, graph_uri)
            if inst_uris:
                all_inst_uris.extend(inst_uris)
    
    # TechMeta
    if all_inst_uris:
        await fast_batch_anonymize_technical_metadata(client, all_inst_uris)
    
    monitor.close()
    logger.info(f"🎉 ANONIMIZZAZIONE: {total_success} entità, {total_failed} falliti")

# === ✅ CONSISTENCY CHECKS - SAFETY NET ===
async def ensure_title_anonymization_consistency(client):
    """
    🛡️ SAFETY NET: Garantisce titoli anonimizzati per tutti i Record con redactedInformation=yes
    """
    logger.info("🔒 CONSISTENCY CHECK: Titoli")
    
    all_graphs = list(NAMED_GRAPH_URIS.values())
    total_fixed = 0
    
    for graph_uri in all_graphs:
        # ✅ CORRETTA: Cross-graph query
        find_query = f"""
        PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
        PREFIX bodi: <http://w3id.org/bodi#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT DISTINCT ?record ?title WHERE {{
            GRAPH <{graph_uri}> {{
                ?record bodi:redactedInformation "{OMITTED_INFORMATION_VALUE}" .
                ?record rico:hasOrHadTitle ?title .
            }}
            GRAPH <{UPDATED_RELATIONS_GRAPH}> {{
                ?title rdfs:label ?titleLabel .
                FILTER(?titleLabel != "{OMITTED_LABEL}")
            }}
        }}
        """
        
        result = await client.query(find_query)
        if not result or not result.get('results', {}).get('bindings'):
            continue
        
        title_uris = [b['title']['value'] for b in result['results']['bindings']]
        logger.warning(f"   ⚠️ {len(title_uris)} titoli da correggere in {graph_uri}")
        
        batch_size = 500
        for i in range(0, len(title_uris), batch_size):
            batch = title_uris[i:i + batch_size]
            title_values = ' '.join([f'<{uri}>' for uri in batch])
            
            # ✅ Fix titoli nel grafo updated_relations
            fix_titles = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            
            DELETE {{ GRAPH <{UPDATED_RELATIONS_GRAPH}> {{ ?title rdfs:label ?oldLabel . }} }}
            INSERT {{ GRAPH <{UPDATED_RELATIONS_GRAPH}> {{ ?title rdfs:label "{OMITTED_LABEL}" . }} }}
            WHERE {{
                GRAPH <{UPDATED_RELATIONS_GRAPH}> {{
                    VALUES ?title {{ {title_values} }}
                    OPTIONAL {{ ?title rdfs:label ?oldLabel }}
                }}
            }}
            """
            await client.update(fix_titles)
            total_fixed += len(batch)
    
    if total_fixed > 0:
        logger.warning(f"   ✅ CORRETTI {total_fixed} titoli")
    else:
        logger.info(f"   ✅ Tutti i titoli già coerenti")
    
    return total_fixed

async def ensure_protected_metadata_not_anonymized(client):
    """
    🛡️ SAFETY NET: Verifica che metadati protetti non siano stati anonimizzati
    """
    logger.info("🔒 CONSISTENCY CHECK: Metadati protetti")
    
    protected_types_filter = ', '.join([f'"{t}"' for t in PROTECTED_TECH_METADATA_TYPES])
    total_protected_anonymized = 0
    
    for tech_graph in TECHNICAL_METADATA_GRAPH_URIS:
        check_query = f"""
        PREFIX bodi: <http://w3id.org/bodi#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?metadataType (COUNT(*) as ?count) WHERE {{
            GRAPH <{tech_graph}> {{
                ?tm a bodi:TechnicalMetadata ;
                    bodi:hasTechnicalMetadataType ?typeUri ;
                    rdf:value "{OMITTED_LABEL}" .
                ?typeUri rdfs:label ?metadataType .
                FILTER(?metadataType IN ({protected_types_filter}))
            }}
        }}
        GROUP BY ?metadataType
        """
        
        result = await client.query(check_query)
        if result and result.get('results', {}).get('bindings'):
            for binding in result['results']['bindings']:
                type_name = binding['metadataType']['value']
                count = int(binding['count']['value'])
                total_protected_anonymized += count
                logger.error(f"   ❌ {type_name}: {count} anonimizzati (PROTETTO!)")
    
    if total_protected_anonymized == 0:
        logger.info(f"   ✅ Nessun metadato protetto anonimizzato")
    else:
        logger.error(f"   ❌ TOTALE: {total_protected_anonymized} metadati protetti anonimizzati!")
    
    return total_protected_anonymized

# === PROTEZIONE E VERIFICHE (compatte) ===
async def complete_mark_protected_entities(client, protected_entities, entities_dict):
    logger.info(f"✅ Marcatura {len(protected_entities)} protette")
    entities_by_graph = {}
    for entity_uri in protected_entities:
        if tracker.is_entity_processed(entity_uri):
            continue
        entity_graph = entities_dict['entity_graphs'].get(entity_uri)
        if entity_graph:
            if entity_graph not in entities_by_graph:
                entities_by_graph[entity_graph] = []
            entities_by_graph[entity_graph].append(entity_uri)
    
    for graph_uri, entity_uris in entities_by_graph.items():
        batch_size = 500
        for i in range(0, len(entity_uris), batch_size):
            batch = entity_uris[i:i + batch_size]
            update_query = f"""
            PREFIX bodi: <http://w3id.org/bodi#>
            DELETE {{ GRAPH <{graph_uri}> {{ ?entity bodi:redactedInformation ?anyOmitted }} }}
            INSERT {{ GRAPH <{graph_uri}> {{ ?entity bodi:redactedInformation "{NOT_OMITTED_INFORMATION_VALUE}" }} }}
            WHERE {{ GRAPH <{graph_uri}> {{ VALUES ?entity {{ {' '.join([f'<{uri}>' for uri in batch])} }} OPTIONAL {{ ?entity bodi:redactedInformation ?anyOmitted }} }} }}
            """
            await client.update(update_query)

async def complete_selective_author_check(client):
    logger.info("👥 Controllo autori")
    try:
        all_graphs = list(NAMED_GRAPH_URIS.values())
        unauthorized_tm_uris_by_graph = {}
        
        for main_graph in all_graphs:
            for tech_graph in TECHNICAL_METADATA_GRAPH_URIS:
                find_query = f"""
                PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
                PREFIX bodi: <http://w3id.org/bodi#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT DISTINCT ?tm ?tmValue ?tmLabel WHERE {{
                    GRAPH <{main_graph}> {{ ?entity bodi:redactedInformation "{NOT_OMITTED_INFORMATION_VALUE}" ; rico:hasOrHadInstantiation ?instantiation . }}
                    GRAPH <{tech_graph}> {{
                        ?instantiation bodi:hasTechnicalMetadata ?tm .
                        ?tm a bodi:TechnicalMetadata ; bodi:hasTechnicalMetadataType ?tmTypeUri ; rdf:value ?tmValue ; rdfs:label ?tmLabel .
                        ?tmTypeUri rdfs:label ?tmType .
                        FILTER(STR(?tmType) IN ({', '.join([f'"{t}"' for t in AUTHOR_METADATA_TYPES_TO_CHECK])}))
                        FILTER(?tmValue != "{OMITTED_LABEL}")
                    }}
                }}
                """
                result = await client.query(find_query, timeout=300)
                if result and result.get('results', {}).get('bindings'):
                    for binding in result['results']['bindings']:
                        try:
                            tm_uri = binding['tm']['value']
                            combined = f"{binding['tmValue']['value']} {binding['tmLabel']['value']}".strip()
                            if not is_author_acceptable(combined):
                                if tech_graph not in unauthorized_tm_uris_by_graph:
                                    unauthorized_tm_uris_by_graph[tech_graph] = []
                                unauthorized_tm_uris_by_graph[tech_graph].append(tm_uri)
                        except:
                            continue
        
        total = 0
        for tech_graph, tm_uris in unauthorized_tm_uris_by_graph.items():
            if tm_uris:
                for i in range(0, len(tm_uris), 100):
                    chunk = tm_uris[i:i + 100]
                    batch_update = f"""
                    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    DELETE {{ GRAPH <{tech_graph}> {{ ?tm rdf:value ?oldValue . ?tm rdfs:label ?oldLabel . }} }}
                    INSERT {{ GRAPH <{tech_graph}> {{ ?tm rdf:value "{OMITTED_LABEL}" . ?tm rdfs:label "{OMITTED_LABEL}" . }} }}
                    WHERE {{ GRAPH <{tech_graph}> {{ VALUES ?tm {{ {' '.join([f'<{uri}>' for uri in chunk])} }} OPTIONAL {{ ?tm rdf:value ?oldValue }} OPTIONAL {{ ?tm rdfs:label ?oldLabel }} }} }}
                    """
                    await client.update(batch_update)
                total += len(tm_uris)
        logger.info(f"   ✅ {total} autori non autorizzati anonimizzati")
    except Exception as e:
        logger.error(f"❌ Errore: {e}")

async def verify_work_protection(client):
    logger.info("🔍 Verifica opere")
    try:
        work_query = """
        PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
        PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
        PREFIX bodi: <http://w3id.org/bodi#>
        SELECT ?entity WHERE {
            ?entity rico:isRelatedTo ?work .
            ?work a lrmoo:F1_Work .
            ?entity bodi:redactedInformation "yes" .
        }
        """
        result = await client.query(work_query)
        if result and result.get('results', {}).get('bindings'):
            problematic = [b['entity']['value'] for b in result['results']['bindings']]
            logger.error(f"🚨 {len(problematic)} opere anonimizzate!")
            return False
        logger.info("   ✅ Opere protette")
        return True
    except:
        return False

# === MAIN ===
async def fast_privacy_protection(skip_backup=False):
    logger.info("=" * 80)
    logger.info("⚡ SISTEMA PRIVACY - VERSIONE CON CONSISTENCY CHECKS")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    global tracker, monitor
    tracker = CompleteTitleStructureTracker()
    
    if not skip_backup:
        logger.info("🛡️ BACKUP")
        if not create_blazegraph_backup():
            logger.error("❌ BACKUP FALLITO")
            return False
    
    if not test_blazegraph_connection():
        return False
    
    try:
        async with FastSPARQLClient() as client:
            logger.info("📊 FASE 1: Recupero")
            entities = await complete_get_all_entities(client)
            total_entities = len(entities['Record']) + len(entities['RecordSet'])
            
            monitor = ProgressMonitor(total_entities)
            
            logger.info("🎯 FASE 2: Classificazione")
            entities_to_anonymize, entities_to_protect, info = await complete_classification_logic(client, entities)
            
            logger.info("🔒 FASE 3: Anonimizzazione")
            await optimized_complete_anonymization(client, entities_to_anonymize, entities)
            
            # ✅ NUOVA FASE 3.5: CONSISTENCY CHECK TITOLI
            logger.info("🔒 FASE 3.5: Consistency Check Titoli")
            fixed_titles = await ensure_title_anonymization_consistency(client)
            
            if entities_to_protect:
                logger.info("🛡️ FASE 4: Protezione")
                await complete_mark_protected_entities(client, entities_to_protect, entities)
            
            logger.info("👥 FASE 5: Autori")
            await complete_selective_author_check(client)
            
            # ✅ NUOVA FASE 5.5: CONSISTENCY CHECK METADATI PROTETTI
            logger.info("🔒 FASE 5.5: Consistency Check Metadati Protetti")
            protected_anonymized = await ensure_protected_metadata_not_anonymized(client)
            
            logger.info("🔍 FASE 6: Verifiche")
            work_ok = await verify_work_protection(client)
            
            total_time = time.time() - start_time
            summary = monitor.get_summary()
            
            logger.info("=" * 80)
            logger.info("🎉 COMPLETATO")
            logger.info("=" * 80)
            logger.info(f"⏱️ Tempo: {total_time:.1f}s ({total_time/60:.1f} min)")
            logger.info(f"🚀 Velocità: {summary['rate']:.1f} ent/s")
            logger.info("")
            logger.info(f"📊 RISULTATI:")
            logger.info(f"   🔒 Anonimizzate: {len(entities_to_anonymize)}")
            logger.info(f"   ✅ Protette: {len(entities_to_protect)}")
            logger.info(f"   🔧 Titoli corretti: {fixed_titles}")
            logger.info(f"   ⚠️ Metadati protetti anonimizzati: {protected_anonymized}")
            logger.info("")
            
            # Valutazione finale
            all_ok = work_ok and (protected_anonymized == 0)
            
            if all_ok:
                logger.info("✅ TUTTI I CONTROLLI SUPERATI!")
            else:
                if protected_anonymized > 0:
                    logger.error("❌ ATTENZIONE: Metadati protetti anonimizzati!")
                if not work_ok:
                    logger.error("❌ ATTENZIONE: Opere non protette correttamente!")
            
            logger.info("=" * 80)
            return all_ok
            
    except Exception as e:
        logger.error(f"❌ Errore critico: {e}")
        logger.error(traceback.format_exc())
        return False

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-backup', action='store_true')
    args = parser.parse_args()
    
    try:
        success = asyncio.run(fast_privacy_protection(skip_backup=args.skip_backup))
        exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("⚠️ Interrotto")
        exit(1)
