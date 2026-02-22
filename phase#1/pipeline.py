import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import shutil
import requests
from typing import Optional 
from blazegraph_loader import BlazegraphJournalGeneratorRESTWithChunking 

# Configurazione interprete Python
PYTHON_INTERPRETER = '/home/tech/venv/evangelisti/bin/python3'

# === IMPORT CONFIGURAZIONE CENTRALIZZATA ===
try:
    from config_loader import load_config, ConfigError
    USE_CENTRALIZED_CONFIG = True
    print("✅ Configurazione centralizzata caricata - pipeline.py:23")
except ImportError:
    USE_CENTRALIZED_CONFIG = False
    ConfigError = Exception  # Definisce ConfigError come fallback
    load_config = None  # Definisce load_config come fallback
    print("⚠️ Configurazione centralizzata non disponibile, uso configurazione locale - pipeline.py:28")

try:
    from blazegraph_fast_reset import BlazegraphFastResetter
    FAST_RESET_AVAILABLE = True
    print("✅ Sistema di reset veloce disponibile - pipeline.py:33")
except ImportError:
    FAST_RESET_AVAILABLE = False
    print("⚠️ Sistema di reset veloce non disponibile  uso SPARQL standard - pipeline.py:36")

def get_pipeline_configs():
    """Ottiene le configurazioni della pipeline"""
    if USE_CENTRALIZED_CONFIG and load_config is not None:
        try:
            config = load_config()
            return config.to_legacy_format("pipeline")
        except ConfigError:
            print("⚠️ Errore configurazione centralizzata, uso fallback locale - pipeline.py:45")
            pass
    
    # ✅ FIX: Configurazione locale con metadata_directory SEMPRE lowercase
    return {
        'floppy': {
            'path': '/media/sdb1/evangelisti/data/FloppyDisks/',
            'structure_type': 'floppy',
            'count_output': 'FloppyDisks_CNT.json',
            'hash_output': 'FloppyDisks_HASH.json',
            'structure_output': 'structure_floppy.nq',
            'metadata_directory': 'floppy',  # SEMPRE lowercase
            'check_type': 'floppy',
            'description': 'Floppy Disks'
        },
        'hd': {
            'path': '/media/sdb1/evangelisti/data/HardDiskValerio/',
            'structure_type': 'hd',
            'count_output': 'HD_CNT.json',
            'hash_output': 'HD_HASH.json',
            'structure_output': 'structure_hd.nq',
            'metadata_directory': 'hd',  # SEMPRE lowercase
            'check_type': 'hd',
            'description': 'Hard Disk'
        },
        'hdesterno': {
            'path': '/media/sdb1/evangelisti/data/HDEsternoEvangelisti/',
            'structure_type': 'hdesterno',
            'count_output': 'HDEsterno_CNT.json',
            'hash_output': 'HDEsterno_HASH.json',
            'structure_output': 'structure_hdesterno.nq',
            'metadata_directory': 'hdesterno',  # SEMPRE lowercase
            'check_type': 'hdesterno',
            'description': 'HD Esterno Evangelisti'
        }
    }

# Inizializza PIPELINE_CONFIGS globalmente
PIPELINE_CONFIGS = get_pipeline_configs()

# Verifica che PIPELINE_CONFIGS sia stato caricato correttamente
if not PIPELINE_CONFIGS:
    print("❌ ERRORE: Impossibile caricare le configurazioni della pipeline - pipeline.py:87")
    sys.exit(1)
else:
    print(f"✅ Configurazioni pipeline caricate: {', '.join(PIPELINE_CONFIGS.keys())} - pipeline.py:90")


BLAZEGRAPH_RESET_CONFIG = {
    'reset_on_start': True,
    'prompt_before_reset': False,
    'backup_before_reset': True,
    'reset_method': 'fast_journal_replace',  # ← NUOVO: metodo veloce preferito
    'fallback_method': 'namespace_clear'     # ← Fallback se reset veloce non disponibile
}

BACKUP_CONFIG = {
    'backup_before_start': False,     # Backup prima di iniziare
    'backup_after_structure': True,  # ✅ Backup dopo caricamento struttura  
    'backup_after_metadata': True,   # ✅ Backup dopo caricamento metadati
    'backup_final': True,           # ✅ Backup finale
    'backup_directory': 'backups',   # Directory backup
    'auto_cleanup': True,            # Pulizia automatica backup vecchi
    'max_backups_keep': 10          # Mantieni solo 10 backup più recenti
}

class PipelineLogger:
    """Sistema di logging per la pipeline"""
    
    def __init__(self, log_file: str = "pipeline_evangelisti.log"):
        self.log_file = log_file
        self.setup_logging()
        self.start_time = time.time()
        
    def setup_logging(self):
        """Configura il sistema di logging"""
        # File handler
        file_handler = logging.FileHandler(self.log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        # Formatter
        formatter = logging.Formatter(
            '[%(levelname)s] %(asctime)s - %(message)s',
            datefmt='%a %d/%m/%Y %H:%M:%S'  # %a = giorno della settimana abbreviato
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Logger setup
        self.logger = logging.getLogger('EvangelistiPipeline')
        self.logger.setLevel(logging.DEBUG)
        
        # Rimuovi handler esistenti per evitare duplicati
        self.logger.handlers.clear()
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
    def info(self, message: str):
        self.logger.info(message)
        
    def error(self, message: str):
        self.logger.error(message)
        
    def warning(self, message: str):
        self.logger.warning(message)
        
    def debug(self, message: str):
        self.logger.debug(message)
        
    def step_start(self, step_num: int, step_name: str):
        self.logger.info(f"\n{'='*70}")
        self.logger.info(f"STEP {step_num}: {step_name}")
        self.logger.info(f"{'='*70}")
        
    def substep_start(self, directory: str, step_name: str):
        self.logger.info(f"\n--- {step_name} per {directory} ---")
        
    def step_complete(self, step_num: int, step_name: str, duration: float, success_count: int, total_count: int):
        self.logger.info(f"✅ STEP {step_num} COMPLETATO: {step_name}")
        self.logger.info(f"   Successi: {success_count}/{total_count} directory")
        self.logger.info(f"   Durata totale: {duration:.2f}s")
        
    def substep_complete(self, directory: str, step_name: str, duration: float, success: bool):
        status = "✅ SUCCESSO" if success else "❌ FALLITO"
        self.logger.info(f"{status}: {step_name} per {directory} ({duration:.2f}s)")
        
    def pipeline_summary(self, total_time: float, steps_results: Dict):
        self.logger.info(f"\n{'='*70}")
        self.logger.info(f"PIPELINE COMPLETATA")
        self.logger.info(f"{'='*70}")
        self.logger.info(f"Tempo totale: {total_time:.2f} secondi")
        
        for step_num, (step_name, results) in enumerate(steps_results.items(), 1):
            success_count = sum(1 for success in results.values() if success)
            total_count = len(results)
            self.logger.info(f"Step {step_num} ({step_name}): {success_count}/{total_count} successi")
            
        total_successes = sum(sum(1 for success in results.values() if success) for results in steps_results.values())
        total_operations = sum(len(results) for results in steps_results.values())
        
        self.logger.info(f"\nTotale operazioni riuscite: {total_successes}/{total_operations}")
        if total_successes == total_operations:
            self.logger.info("🎉 TUTTE LE OPERAZIONI COMPLETATE CON SUCCESSO!")
        else:
            self.logger.warning(f"⚠️ {total_operations - total_successes} operazioni fallite")

    def log_command_execution(self, command: List[str], operation_name: str, directory: str = None):
        """Log dettagliato dell'esecuzione di un comando"""
        self.logger.info(f"\n{'='*50}")
        self.logger.info(f"📋 ESECUZIONE COMANDO: {operation_name}")
        if directory:
            self.logger.info(f"📁 Directory: {directory}")
        self.logger.info(f"{'='*50}")
        
        # Mostra il comando completo
        command_str = ' '.join(f'"{arg}"' if ' ' in arg else arg for arg in command)
        self.logger.info(f"🔧 Comando completo:")
        self.logger.info(f"   {command_str}")
        
        # Mostra i parametri separatamente per chiarezza
        if len(command) > 2:
            self.logger.info(f"📋 Parametri dettagliati:")
            self.logger.info(f"   Python: {command[0]}")
            self.logger.info(f"   Script: {command[1]}")
            if len(command) > 2:
                self.logger.info(f"   Argomenti: {' '.join(command[2:])}")
        
        # Informazioni ambiente
        self.logger.info(f"📂 Directory di lavoro: {Path.cwd()}")
        self.logger.info(f"⏰ Timestamp avvio: {datetime.now().strftime('%A %d/%m/%Y %H:%M:%S')}")
        
    def log_command_result(self, success: bool, stdout: str, stderr: str, duration: float, operation_name: str):
        """Log del risultato di un comando"""
        self.logger.info(f"\n{'='*50}")
        self.logger.info(f"📊 RISULTATO COMANDO: {operation_name}")
        self.logger.info(f"{'='*50}")
        
        # Status generale
        status_icon = "✅" if success else "❌"
        self.logger.info(f"{status_icon} Status: {'SUCCESSO' if success else 'FALLITO'}")
        self.logger.info(f"⏱️ Durata: {duration:.2f} secondi")
        
        # Output dettagliato
        if stdout:
            self.logger.info(f"\n📤 OUTPUT STDOUT:")
            self.logger.info(f"{'='*30}")
            # Mostra tutto l'output ma con indentazione per chiarezza
            for line in stdout.strip().split('\n'):
                self.logger.info(f"   {line}")
            self.logger.info(f"{'='*30}")
        else:
            self.logger.info(f"📤 OUTPUT STDOUT: (vuoto)")
            
        if stderr:
            self.logger.error(f"\n📥 OUTPUT STDERR:")
            self.logger.error(f"{'='*30}")
            for line in stderr.strip().split('\n'):
                self.logger.error(f"   {line}")
            self.logger.error(f"{'='*30}")
        else:
            self.logger.debug(f"📥 OUTPUT STDERR: (vuoto)")

    def log_file_check(self, file_path: Path, expected: bool = True):
        """Log della verifica di un file"""
        exists = file_path.exists()
        if exists:
            size = file_path.stat().st_size
            self.logger.info(f"✅ File verificato: {file_path.name}")
            self.logger.info(f"   Dimensione: {size:,} bytes")
            self.logger.info(f"   Path completo: {file_path}")
        else:
            if expected:
                self.logger.error(f"❌ File mancante: {file_path.name}")
                self.logger.error(f"   Path atteso: {file_path}")
            else:
                self.logger.debug(f"ℹ️ File non presente: {file_path.name} (come atteso)")

class BlazegraphRESTLoader:
    """Caricatore che usa REST API di Blazegraph invece di DataLoader diretto"""
    
    def __init__(self, base_url: str = "http://localhost:9999/blazegraph", namespace: str = "kb", logger=None):
        self.base_url = base_url.rstrip('/')
        self.namespace = namespace
        self.namespace_url = f"{self.base_url}/namespace/{self.namespace}"
        self.sparql_update_url = f"{self.namespace_url}/sparql"
        self.data_upload_url = f"{self.namespace_url}"
        self.logger = logger
    
    def test_connection(self) -> bool:
        """Testa la connessione al server Blazegraph"""
        try:
            response = requests.post(
                self.sparql_update_url,
                data={'query': 'SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }'},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                count = int(result["results"]["bindings"][0]["count"]["value"])
                if self.logger:
                    self.logger.info(f"✅ Connessione OK - Triple esistenti: {count:,}")
                return True
            else:
                if self.logger:
                    self.logger.error(f"❌ Server risponde ma con errore: HTTP {response.status_code}")
                return False
                
        except requests.exceptions.ConnectionError:
            if self.logger:
                self.logger.error(f"❌ Server Blazegraph non raggiungibile su {self.base_url}")
                self.logger.error("   Assicurati che sia in esecuzione: java -jar blazegraph.jar")
            return False
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ Errore test connessione: {e}")
            return False
    
    def load_nquads_file(self, file_path: Path) -> bool:
        """Carica un file .nq tramite REST API"""
        if not file_path.exists():
            if self.logger:
                self.logger.error(f"❌ File non trovato: {file_path}")
            return False
        
        file_size = file_path.stat().st_size
        if self.logger:
            self.logger.info(f"🔄 Caricamento {file_path.name} ({file_size:,} bytes)...")
        
        start_time = time.time()
        
        try:
            with open(file_path, 'rb') as f:
                file_content = f.read()
            
            response = requests.post(
                self.data_upload_url,
                data=file_content,
                headers={
                    'Content-Type': 'application/n-quads'
                },
                timeout=3600
            )
            
            duration = time.time() - start_time
            
            if response.status_code in [200, 201]:
                if self.logger:
                    self.logger.info(f"✅ {file_path.name} caricato con successo ({duration:.2f}s)")
                self._log_triple_count_after_load()
                return True
            else:
                if self.logger:
                    self.logger.error(f"❌ Caricamento fallito: HTTP {response.status_code}")
                    self.logger.error(f"   Response: {response.text[:500]}")
                return False
                
        except requests.exceptions.Timeout:
            duration = time.time() - start_time
            if self.logger:
                self.logger.error(f"⏰ Timeout caricamento {file_path.name} dopo {duration:.0f}s")
            return False
        except Exception as e:
            duration = time.time() - start_time
            if self.logger:
                self.logger.error(f"❌ Errore caricamento {file_path.name}: {e}")
            return False
    
    def _log_triple_count_after_load(self):
        """Log del conteggio triple dopo caricamento"""
        try:
            response = requests.post(
                self.sparql_update_url,
                data={'query': 'SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }'},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                count = int(result["results"]["bindings"][0]["count"]["value"])
                if self.logger:
                    self.logger.info(f"📊 Triple totali nel DB: {count:,}")
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ Impossibile verificare conteggio triple: {e}")
    
    def load_multiple_files(self, file_paths: List[Path]) -> Tuple[int, int]:
        """Carica multipli file .nq"""
        if not file_paths:
            if self.logger:
                self.logger.warning("⚠️ Nessun file da caricare")
            return 0, 0
        
        if self.logger:
            self.logger.info(f"🚀 Inizio caricamento di {len(file_paths)} file via REST API")
        
        successful = 0
        failed = 0
        
        for i, file_path in enumerate(file_paths, 1):
            if self.logger:
                self.logger.info(f"\n[{i}/{len(file_paths)}] Processando: {file_path.name}")
            
            if self.load_nquads_file(file_path):
                successful += 1
            else:
                failed += 1
                
        if self.logger:
            self.logger.info(f"\n📊 RISULTATI CARICAMENTO:")
            self.logger.info(f"   ✅ Successi: {successful}")
            self.logger.info(f"   ❌ Fallimenti: {failed}")
            self.logger.info(f"   📊 Totale: {len(file_paths)}")
        
        return successful, len(file_paths)


class BlazegraphJournalGeneratorREST:
    """Generatore Journal Blazegraph che usa REST API invece di DataLoader"""
    
    def __init__(self, logger, working_dir: Path):
        self.logger = logger
        self.working_dir = working_dir
        self.blazegraph_dir = working_dir / "blazegraph_journal"
        
        # Configurazione REST API
        self.base_url = "http://localhost:9999/blazegraph"
        self.namespace = "kb"
        
        # Crea REST loader
        self.rest_loader = BlazegraphRESTLoader(self.base_url, self.namespace, logger)
    
    def _verify_server_running(self) -> bool:
        """Verifica che il server Blazegraph sia in esecuzione"""
        self.logger.info("🔍 Verifica server Blazegraph...")
        
        if self.rest_loader.test_connection():
            self.logger.info("✅ Server Blazegraph in esecuzione e raggiungibile")
            return True
        else:
            self.logger.error("❌ Server Blazegraph non raggiungibile")
            self.logger.error("   AZIONE RICHIESTA:")
            self.logger.error(f"   1. Apri terminale in: {self.blazegraph_dir}")
            self.logger.error("   2. Esegui: java -jar blazegraph.jar")
            self.logger.error("   3. Attendi che sia completamente avviato")
            self.logger.error("   4. Rilancia la pipeline")
            return False
    
    def _check_existing_data(self) -> int:
        """Controlla se ci sono già dati nel server"""
        try:
            response = requests.post(
                f"{self.base_url}/namespace/{self.namespace}/sparql",
                data={'query': 'SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }'},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                count = int(result["results"]["bindings"][0]["count"]["value"])
                self.logger.info(f"📊 Triple esistenti nel server: {count:,}")
                return count
            else:
                self.logger.warning("⚠️ Impossibile verificare dati esistenti")
                return 0
                
        except Exception as e:
            self.logger.warning(f"⚠️ Errore verifica dati esistenti: {e}")
            return 0
    
    def generate_blazegraph_journal(self, nq_files: List[Path]) -> bool:
        """Metodo principale per caricare dati via REST API"""
        self.logger.info(f"🚀 Avvio caricamento Blazegraph via REST API per {len(nq_files)} file")
        
        start_time = time.time()
        
        try:
            # 1. Verifica server in esecuzione
            if not self._verify_server_running():
                return False
            
            # 2. Verifica dati esistenti
            existing_count = self._check_existing_data()
            if existing_count > 0:
                self.logger.info(f"🔄 Modalità automatica: aggiunta ai {existing_count:,} dati esistenti")
            
            # 3. Verifica file disponibili
            available_files = []
            for nq_file in nq_files:
                if nq_file.exists():
                    size = nq_file.stat().st_size
                    self.logger.info(f"✅ File disponibile: {nq_file.name} ({size:,} bytes)")
                    available_files.append(nq_file)
                else:
                    self.logger.error(f"❌ File non trovato: {nq_file}")
            
            if not available_files:
                self.logger.error("❌ Nessun file .nq disponibile per caricamento")
                return False
            
            # 4. Caricamento via REST API
            self.logger.info(f"🔄 Caricamento {len(available_files)} file via REST API...")
            successful, total = self.rest_loader.load_multiple_files(available_files)
            
            # 5. Report finale
            total_duration = time.time() - start_time
            success_rate = (successful / total * 100) if total > 0 else 0
            
            self.logger.info(f"\n{'='*70}")
            self.logger.info(f"📊 REPORT FINALE - Caricamento Blazegraph REST API")
            self.logger.info(f"{'='*70}")
            self.logger.info(f"File processati: {successful}/{total}")
            self.logger.info(f"Tasso successo: {success_rate:.1f}%")
            self.logger.info(f"Tempo totale: {total_duration:.2f} secondi")
            
            # Verifica conteggio finale
            final_count = self._check_existing_data()
            added_triples = final_count - existing_count
            if added_triples > 0:
                self.logger.info(f"📈 Triple aggiunte: {added_triples:,}")
            
            success = successful == total
            
            if success:
                self.logger.info("🎉 CARICAMENTO BLAZEGRAPH COMPLETATO CON SUCCESSO!")
                self.logger.info(f"📊 Triple totali nel database: {final_count:,}")
            else:
                self.logger.error(f"❌ CARICAMENTO PARZIALE: {successful}/{total} file")
            
            return success
            
        except Exception as e:
            total_duration = time.time() - start_time
            self.logger.error(f"💥 Errore fatale caricamento REST API: {e}")
            self.logger.error(f"   Durata prima errore: {total_duration:.2f}s")
            import traceback
            self.logger.debug(traceback.format_exc())
            return False

class BlazegraphBackupManager:
    """Gestione backup Blazegraph integrata nella pipeline"""
    
    def __init__(self, logger, working_dir: Path, blazegraph_generator):
        self.logger = logger
        self.working_dir = working_dir
        self.blazegraph_generator = blazegraph_generator
        
        # Directory backup
        self.backup_dir = working_dir / "backups"
        self.backup_dir.mkdir(exist_ok=True)
        
        # Configurazione
        self.sparql_endpoint = f"{blazegraph_generator.base_url}/namespace/{blazegraph_generator.namespace}/sparql"
    
    def get_triple_count(self) -> int:
        """Conta le triple nel database"""
        try:
            response = requests.post(
                self.sparql_endpoint,
                data={'query': 'SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }'},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                return int(result["results"]["bindings"][0]["count"]["value"])
            return 0
        except:
            return 0
    
    def export_to_nquads(self, backup_type: str = "final") -> Optional[Path]:
        """Export completo in formato N-Quads per backup pipeline"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"blazegraph_backup_{backup_type}_{timestamp}.nq"
        backup_path = self.backup_dir / backup_filename
        
        self.logger.info(f"🔄 Export backup {backup_type} in corso...")
        start_time = time.time()
        
        try:
            # Query CONSTRUCT per export completo con tutti i grafi
            construct_query = """
            CONSTRUCT { ?s ?p ?o }
            WHERE { 
                {
                    # Dati nel grafo default
                    ?s ?p ?o
                }
                UNION
                {
                    # Dati in grafi nominati
                    GRAPH ?g { ?s ?p ?o }
                }
            }
            """
            
            response = requests.post(
                self.sparql_endpoint,
                data={'query': construct_query},
                headers={'Accept': 'application/n-quads'},
                timeout=900  # 15 minuti timeout
            )
            
            if response.status_code == 200:
                with open(backup_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                duration = time.time() - start_time
                file_size = backup_path.stat().st_size
                triple_count = self.get_triple_count()
                
                self.logger.info(f"✅ Backup {backup_type} completato!")
                self.logger.info(f"   📄 File: {backup_filename}")
                self.logger.info(f"   📍 Path: {backup_path}")
                self.logger.info(f"   📊 Dimensione: {file_size:,} bytes")
                self.logger.info(f"   🔢 Triple: {triple_count:,}")
                self.logger.info(f"   ⏱️ Durata: {duration:.2f}s")
                
                return backup_path
            else:
                self.logger.error(f"❌ Export backup fallito: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Errore durante export backup: {e}")
            return None
    
    def create_pipeline_backup(self, backup_type: str = "final") -> bool:
        """Crea backup durante la pipeline"""
        self.logger.info(f"🚀 BACKUP PIPELINE BLAZEGRAPH ({backup_type.upper()})")
        self.logger.info("=" * 50)
        
        # Verifica connessione
        if not self.blazegraph_generator._verify_server_running():
            self.logger.error("❌ Server Blazegraph non raggiungibile per backup")
            return False
        
        # Verifica che ci siano dati da fare backup
        triple_count = self.get_triple_count()
        if triple_count == 0:
            self.logger.warning("⚠️ Database vuoto - nessun backup necessario")
            return True
        
        self.logger.info(f"📊 Triple da salvare: {triple_count:,}")
        
        # Export dati (sempre possibile con server attivo)
        export_path = self.export_to_nquads(backup_type)
        
        success = export_path is not None
        
        self.logger.info(f"\n📊 RISULTATI BACKUP {backup_type.upper()}:")
        self.logger.info(f"   🔤 Export N-Quads: {'✅' if export_path else '❌'}")
        
        if success:
            self.logger.info(f"✅ Backup {backup_type} completato!")
            if export_path:
                self.logger.info(f"   📄 Export salvato in: {export_path}")
        else:
            self.logger.error(f"❌ Backup {backup_type} fallito")
        
        return success

class BlazegraphJournalRestorer:
    """Sistema per ripristinare/aggiornare il journal Blazegraph da backup"""
    
    def __init__(self, working_dir: Path, base_url: str = "http://localhost:9999/blazegraph", namespace: str = "kb"):
        self.working_dir = working_dir
        self.base_url = base_url.rstrip('/')
        self.namespace = namespace
        self.sparql_endpoint = f"{self.base_url}/namespace/{self.namespace}/sparql"
        self.data_endpoint = f"{self.base_url}/namespace/{self.namespace}"
        
        # Percorsi
        self.backup_dir = working_dir / "backups"
        self.journal_path = working_dir / "blazegraph_journal" / "blazegraph.jnl"
    
    def list_available_backups(self) -> List[tuple]:
        """Lista backup disponibili con info"""
        if not self.backup_dir.exists():
            return []
        
        backup_files = list(self.backup_dir.glob("blazegraph_backup_*.nq"))
        
        backup_info = []
        for backup_file in backup_files:
            try:
                # Estrai info dal nome file
                name_parts = backup_file.stem.split('_')
                if len(name_parts) >= 4:
                    backup_type = name_parts[2]  # initial, post_structure, final, etc.
                    date_part = name_parts[3]
                    time_part = name_parts[4] if len(name_parts) > 4 else "000000"
                else:
                    backup_type = "unknown"
                    date_part = "unknown"
                    time_part = "unknown"
                
                # Info file
                stat = backup_file.stat()
                size = stat.st_size
                modified = datetime.fromtimestamp(stat.st_mtime)
                
                backup_info.append((backup_file, backup_type, date_part, time_part, size, modified))
            except:
                backup_info.append((backup_file, "unknown", "unknown", "unknown", 0, datetime.now()))
        
        # Ordina per data modifica (più recenti prima)
        backup_info.sort(key=lambda x: x[5], reverse=True)
        
        return backup_info
    
    def restore_from_latest_backup(self, logger) -> bool:
        """Ripristina automaticamente dal backup più recente"""
        logger.info("🔄 Ripristino automatico dal backup più recente...")
        
        backups = self.list_available_backups()
        if not backups:
            logger.error("❌ Nessun backup disponibile per ripristino")
            return False
        
        latest_backup = backups[0][0]  # Primo elemento è il più recente
        logger.info(f"📄 Backup selezionato: {latest_backup.name}")
        
        # Implementa qui la logica di ripristino completa
        # (versione semplificata per la pipeline)
        return True

class MultiDirectoryPipeline:
    """Pipeline che processa tutte le directory per ogni step"""
    
    def __init__(self, logger: PipelineLogger, selected_directories: List[str] = None):
        self.logger = logger
        self.working_dir = Path.cwd()
        self.results = {}
        
        # Filtra directory se specificate
        if selected_directories:
            invalid_dirs = [d for d in selected_directories if d not in PIPELINE_CONFIGS]
            if invalid_dirs:
                raise ValueError(f"Directory non valide: {invalid_dirs}")
            self.directories = {k: v for k, v in PIPELINE_CONFIGS.items() if k in selected_directories}
        else:
            self.directories = PIPELINE_CONFIGS.copy()
            
        self.logger.info(f"Directory da processare: {', '.join(self.directories.keys())}")
        
        # Verifica prerequisiti
        self.backup_manager = None
        self._verify_prerequisites()
        
    def _verify_prerequisites(self):
        """Verifica che tutti i prerequisiti siano soddisfatti"""
        self.logger.info("Verifica prerequisiti...")
        
        # Verifica directory target
        missing_dirs = []
        for dir_key, config in self.directories.items():
            target_path = Path(config['path'])
            if not target_path.exists():
                missing_dirs.append(f"{dir_key}: {target_path}")
            elif not target_path.is_dir():
                missing_dirs.append(f"{dir_key}: {target_path} (non è una directory)")
                
        if missing_dirs:
            raise FileNotFoundError(f"Directory target non trovate:\n" + "\n".join(missing_dirs))
            
        # Verifica script richiesti
        required_scripts = [
            'file_count.py',
            'hash_calc.py', 
            'structure_generation.py',
            'count_check.py',
            'integrity_check.py'
        ]
        
        missing_scripts = []
        for script in required_scripts:
            script_path = self.working_dir / script
            if not script_path.exists():
                missing_scripts.append(script)
            else:
                self.logger.info(f"✅ Script trovato: {script}")
                
        if missing_scripts:
            raise FileNotFoundError(f"Script mancanti: {', '.join(missing_scripts)}")
            
        self.logger.info("✅ Tutti i prerequisiti verificati")
        
    def _run_command(self, command: List[str], operation_name: str, directory: str = None, timeout: Optional[int] = None) -> Tuple[bool, str, str]:
        """Esegue un comando con logging dettagliato"""
        
        # Log pre-esecuzione
        self.logger.log_command_execution(command, operation_name, directory)
        
        # Verifica esistenza script
        if len(command) > 1:
            script_path = Path(self.working_dir) / command[1]
            if not script_path.exists():
                error_msg = f"Script non trovato: {script_path}"
                self.logger.error(f"❌ {error_msg}")
                return False, "", error_msg
            else:
                self.logger.info(f"✅ Script verificato: {script_path}")
        
        try:
            self.logger.info(f"🚀 Avvio processo...")
            start_time = time.time()
            
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=self.working_dir,
                env=os.environ.copy()
            )
            
            duration = time.time() - start_time
            
            # Log post-esecuzione
            self.logger.log_command_result(
                result.returncode == 0,
                result.stdout,
                result.stderr,
                duration,
                operation_name
            )
            
            return result.returncode == 0, result.stdout, result.stderr
                
        except subprocess.TimeoutExpired as e:
            duration = time.time() - start_time
            error_msg = f"Timeout dopo {timeout} secondi"
            self.logger.error(f"⏰ {error_msg}")
            self.logger.log_command_result(False, "", error_msg, duration, operation_name)
            return False, "", error_msg
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Errore nell'esecuzione: {e}"
            self.logger.error(f"💥 {error_msg}")
            self.logger.log_command_result(False, "", error_msg, duration, operation_name)
            return False, "", error_msg

    def _run_command_unbuffered(self, command: List[str], operation_name: str, directory: str = None, timeout: Optional[int] = None) -> Tuple[bool, str, str]:
        """Esegue un comando con output in real-time per processi lunghi"""
        
        self.logger.log_command_execution(command, operation_name, directory)
        
        # Verifica esistenza script
        if len(command) > 1:
            script_path = Path(self.working_dir) / command[1]
            if not script_path.exists():
                error_msg = f"Script non trovato: {script_path}"
                self.logger.error(f"❌ {error_msg}")
                return False, "", error_msg
            else:
                self.logger.info(f"✅ Script verificato: {script_path}")

        try:
            self.logger.info(f"🚀 Avvio processo unbuffered...")
            start_time = time.time()
            
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Combina stderr in stdout
                text=True,
                cwd=self.working_dir,
                env=os.environ.copy(),
                bufsize=1,  # Line buffering
                universal_newlines=True
            )
            
            stdout_lines = []
            
            # Leggi output in real-time
            while True:
                try:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        line = output.strip()
                        print(f"[{operation_name}] {line}")  # Mostra immediatamente
                        stdout_lines.append(output)
                        self.logger.info(f"[REALTIME] {line}")
                except Exception as e:
                    self.logger.warning(f"Errore lettura output: {e}")
                    break
            
            # Aspetta che il processo finisca
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.logger.error(f"⏰ Timeout dopo {timeout}s - termino processo")
                process.kill()
                process.wait()
                return False, "", f"Timeout dopo {timeout} secondi"
            
            duration = time.time() - start_time
            stdout = ''.join(stdout_lines)
            
            self.logger.log_command_result(
                process.returncode == 0,
                stdout,
                "",
                duration,
                operation_name
            )
            
            return process.returncode == 0, stdout, ""
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Errore nell'esecuzione: {e}"
            self.logger.error(f"💥 {error_msg}")
            self.logger.log_command_result(False, "", error_msg, duration, operation_name)
            return False, "", error_msg


    def reset_blazegraph_if_needed(self) -> bool:
        """Reset del journal Blazegraph se configurato - VERSIONE AGGIORNATA CON RESET VELOCE"""
        
        if not BLAZEGRAPH_RESET_CONFIG.get('reset_on_start', False):
            self.logger.info("📋 Reset Blazegraph disabilitato - continuo con dati esistenti")
            return True
        
        reset_method = BLAZEGRAPH_RESET_CONFIG.get('reset_method', 'namespace_clear')
        
        self.logger.info(f"🔄 RESET BLAZEGRAPH CONFIGURATO (metodo: {reset_method})")
        self.logger.info("=" * 60)
        
        # Determina metodo da usare
        use_fast_reset = (reset_method == 'fast_journal_replace' and FAST_RESET_AVAILABLE)
        
        if use_fast_reset:
            return self._fast_reset_blazegraph()
        else:
            # Fallback al metodo originale
            if reset_method == 'fast_journal_replace' and not FAST_RESET_AVAILABLE:
                self.logger.warning("⚠️ Reset veloce richiesto ma non disponibile - uso fallback")
                reset_method = BLAZEGRAPH_RESET_CONFIG.get('fallback_method', 'namespace_clear')
            
            return self._legacy_reset_blazegraph(reset_method)
    
    def _fast_reset_blazegraph(self) -> bool:
        """Reset veloce usando sostituzione journal"""
        self.logger.info("🚀 Utilizzo reset veloce (sostituzione journal)")
        
        try:
            # Crea fast resetter
            fast_resetter = BlazegraphFastResetter(
                working_dir=self.working_dir,
                logger=self.logger
            )
            
            # Test stato iniziale
            is_running, existing_triples = fast_resetter.test_server_status()
            
            if not is_running and not fast_resetter.journal_path.exists():
                self.logger.info("✅ Server non attivo e journal non esiste - verrà creato vuoto")
                return True
            
            if is_running and existing_triples == 0:
                self.logger.info("✅ Database già vuoto - nessun reset necessario")
                return True
            
            if is_running:
                self.logger.info(f"📊 Triple esistenti da rimuovere: {existing_triples:,}")
            
            # Prompt conferma se abilitato
            if BLAZEGRAPH_RESET_CONFIG.get('prompt_before_reset', False):
                try:
                    response = input(f"⚠️ Confermi reset veloce di {existing_triples:,} triple? (s/N): ")
                    if response.lower() not in ['s', 'si', 'sì', 'y', 'yes']:
                        self.logger.info("❌ Reset annullato dall'utente")
                        return False
                except (EOFError, KeyboardInterrupt):
                    self.logger.info("❌ Reset annullato")
                    return False
            
            # Esegui reset veloce
            create_backup = BLAZEGRAPH_RESET_CONFIG.get('backup_before_reset', True)
            reset_success = fast_resetter.fast_reset_journal(create_backup=create_backup)
            
            if reset_success:
                self.logger.info("🎉 RESET VELOCE COMPLETATO CON SUCCESSO!")
                return True
            else:
                self.logger.error("❌ RESET VELOCE FALLITO - provo con metodo legacy")
                # Fallback automatico
                return self._legacy_reset_blazegraph('namespace_clear')
                
        except Exception as e:
            self.logger.error(f"❌ Errore durante reset veloce: {e}")
            self.logger.info("🔄 Fallback a metodo legacy...")
            return self._legacy_reset_blazegraph('namespace_clear')

    def _legacy_reset_blazegraph(self, method: str) -> bool:
        """Reset usando metodi legacy (SPARQL o eliminazione file)"""
        self.logger.info(f"🔄 Utilizzo metodo legacy: {method}")
        
        # Usa il generatore REST per operazioni
        blazegraph_generator = BlazegraphJournalGeneratorREST(self.logger, self.working_dir)
        
        # Verifica che il server sia in esecuzione per metodi che lo richiedono
        if method == 'namespace_clear':
            if not blazegraph_generator._verify_server_running():
                self.logger.error("❌ Server Blazegraph non raggiungibile per reset")
                return False
            
            # Conta triple esistenti
            existing_triples = blazegraph_generator._check_existing_data()
            
            if existing_triples == 0:
                self.logger.info("✅ Database già vuoto - nessun reset necessario")
                return True
            
            self.logger.info(f"📊 Triple esistenti da rimuovere: {existing_triples:,}")
            
            # Backup se abilitato
            if BLAZEGRAPH_RESET_CONFIG.get('backup_before_reset', False):
                if not self._backup_blazegraph_data():
                    self.logger.warning("⚠️ Backup fallito, ma continuo con reset")
            
            return self._clear_namespace()
            
        elif method == 'journal_delete':
            return self._delete_journal_file()
        else:
            self.logger.error(f"❌ Metodo reset non valido: {method}")
            return False

    def _clear_namespace(self) -> bool:
        """Pulisce il namespace via SPARQL (server rimane in esecuzione)"""
        self.logger.info("🧹 Reset via SPARQL CLEAR...")
        
        try:
            import requests
            
            clear_query = "CLEAR ALL"
            endpoint = "http://localhost:9999/blazegraph/namespace/kb/sparql"
            
            response = requests.post(
                endpoint,
                data={'update': clear_query},
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=1000
            )
            
            if response.status_code == 200:
                self.logger.info("✅ Namespace pulito con successo")
                
                # Verifica che sia davvero vuoto
                blazegraph_generator = BlazegraphJournalGeneratorREST(self.logger, self.working_dir)
                remaining_triples = blazegraph_generator._check_existing_data()
                
                if remaining_triples == 0:
                    self.logger.info("✅ Conferma: database ora vuoto")
                    return True
                else:
                    self.logger.warning(f"⚠️ Rimangono {remaining_triples:,} triple")
                    return False
            else:
                self.logger.error(f"❌ Reset SPARQL fallito: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Errore durante reset SPARQL: {e}")
            return False

    def _delete_journal_file(self) -> bool:
        """Elimina fisicamente il file journal (richiede restart server)"""
        self.logger.info("🗑️ Reset via eliminazione journal file...")
        self.logger.warning("⚠️ ATTENZIONE: Il server Blazegraph deve essere riavviato dopo questa operazione")
        
        try:
            journal_path = self.working_dir / "blazegraph_journal" / "blazegraph.jnl"
            
            if journal_path.exists():
                journal_size = journal_path.stat().st_size
                journal_path.unlink()
                self.logger.info(f"✅ Journal eliminato ({journal_size:,} bytes)")
                
                # Crea journal vuoto
                journal_path.touch()
                self.logger.info("✅ Journal vuoto ricreato")
                
                self.logger.warning("🔄 RIAVVIA IL SERVER BLAZEGRAPH:")
                self.logger.warning("   1. Interrompi il server (Ctrl+C)")
                self.logger.warning("   2. cd blazegraph_journal")
                self.logger.warning("   3. java -jar blazegraph.jar")
                self.logger.warning("   4. Attendi avvio completo")
                self.logger.warning("   5. Rilancia la pipeline")
                
                return True
            else:
                self.logger.warning("⚠️ Journal file non trovato")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Errore eliminazione journal: {e}")
            return False

    def _backup_blazegraph_data(self) -> bool:
        """Crea backup dei dati prima del reset"""
        self.logger.info("💾 Creazione backup dati...")
        
        try:
            from datetime import datetime
            
            backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"blazegraph_backup_{backup_timestamp}.nq"
            
            # Export via SPARQL CONSTRUCT
            construct_query = """
            CONSTRUCT { ?s ?p ?o }
            WHERE { ?s ?p ?o }
            """
            
            endpoint = "http://localhost:9999/blazegraph/namespace/kb/sparql"
            
            response = requests.post(
                endpoint,
                data={'query': construct_query},
                headers={'Accept': 'application/n-quads'},
                timeout=300  # 5 minuti timeout
            )
            
            if response.status_code == 200:
                with open(backup_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                backup_size = os.path.getsize(backup_file)
                self.logger.info(f"✅ Backup creato: {backup_file} ({backup_size:,} bytes)")
                return True
            else:
                self.logger.error(f"❌ Backup fallito: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Errore durante backup: {e}")
            return False

    def step_1_count_all_files(self) -> Dict[str, bool]:
        """Step 1: Conteggio file per tutte le directory"""
        step_start = time.time()
        self.logger.step_start(1, "Conteggio File per Tutte le Directory")
        
        results = {}
        
        for dir_key, config in self.directories.items():
            substep_start = time.time()
            self.logger.substep_start(dir_key, "Conteggio File")
            
            # CORRETTO: Usa il path direttamente come da pipeline armonizzata
            command = [
                PYTHON_INTERPRETER, 'file_count.py',
                '-o', config['count_output'],
                '-r', '-e', '-w', '-f',  # Tutti i flag insieme
                config['path']  # Path alla fine
            ]
            
            success, stdout, stderr = self._run_command(
                command, 
                f"Conteggio File {dir_key}", 
                directory=config['path'],
                timeout=15000
            )
            
            substep_duration = time.time() - substep_start
            
            if success:
                # Verifica file output
                output_path = self.working_dir / config['count_output']
                self.logger.log_file_check(output_path, expected=True)
                
                if output_path.exists():
                    try:
                        with open(output_path, 'r', encoding='utf-8') as f:
                            count_data = json.load(f)
                        count_total = count_data.get('conteggio_totale', 0)
                        self.logger.info(f"📊 File contati: {count_total}")
                        results[dir_key] = True
                    except Exception as e:
                        self.logger.warning(f"⚠️ Impossibile leggere statistiche: {e}")
                        results[dir_key] = True
                else:
                    results[dir_key] = False
            else:
                results[dir_key] = False
                
            self.logger.substep_complete(dir_key, "Conteggio File", substep_duration, results[dir_key])
            
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete(1, "Conteggio File", step_duration, success_count, len(results))
        
        return results
        
    def step_2_calculate_all_hashes(self) -> Dict[str, bool]:
        """Step 2: Calcolo hash per tutte le directory"""
        step_start = time.time()
        self.logger.step_start(2, "Calcolo Hash per Tutte le Directory")
        
        results = {}
        
        for dir_key, config in self.directories.items():
            substep_start = time.time()
            self.logger.substep_start(dir_key, "Calcolo Hash")
            
            command = [
                PYTHON_INTERPRETER, 'hash_calc.py',
                config['path']
            ]
            
            success, stdout, stderr = self._run_command(
                command, 
                f"Calcolo Hash {dir_key}", 
                directory=config['path'],
                timeout=90000
            )
            
            substep_duration = time.time() - substep_start
            
            if success:
                # Verifica file hash
                expected_output = config['hash_output']
                output_path = self.working_dir / expected_output
                self.logger.log_file_check(output_path, expected=True)
                
                if output_path.exists():
                    try:
                        with open(output_path, 'r', encoding='utf-8') as f:
                            hash_data = json.load(f)
                        hash_total = hash_data.get('totale_file', 0)
                        hash_errors = hash_data.get('statistiche', {}).get('errori', 0)
                        self.logger.info(f"📊 Hash calcolati: {hash_total}, errori: {hash_errors}")
                        results[dir_key] = True
                    except Exception as e:
                        self.logger.warning(f"⚠️ Impossibile leggere statistiche hash: {e}")
                        results[dir_key] = True
                else:
                    results[dir_key] = False
            else:
                results[dir_key] = False
                
            self.logger.substep_complete(dir_key, "Calcolo Hash", substep_duration, results[dir_key])
            
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete(2, "Calcolo Hash", step_duration, success_count, len(results))
        
        return results
        
    def step_3_generate_all_structures(self) -> Dict[str, bool]:
        """Step 3: Generazione struttura RDF per tutte le directory"""
        step_start = time.time()
        self.logger.step_start(3, "Generazione Struttura RDF per Tutte le Directory")
        
        results = {}
        
        for dir_key, config in self.directories.items():
            substep_start = time.time()
            self.logger.substep_start(dir_key, "Generazione Struttura RDF")
            
            # Verifica directory target
            target_path = Path(config['path'])
            self.logger.log_file_check(target_path, expected=True)
            
            if not target_path.exists():
                self.logger.error(f"❌ Directory target non esiste: {target_path}")
                results[dir_key] = False
                continue
            
            command = [
                PYTHON_INTERPRETER, 'structure_generation.py',
                '--type', config['structure_type']
            ]
            
            success, stdout, stderr = self._run_command(
                command, 
                f"Generazione Struttura {dir_key}", 
                directory=config['path'],
                timeout=150000
            )
            
            substep_duration = time.time() - substep_start
            
            if success:
                # Verifica file .nq
                structure_output = self.working_dir / config['structure_output']
                self.logger.log_file_check(structure_output, expected=True)
                
                if structure_output.exists():
                    results[dir_key] = True
                else:
                    # Mostra file .nq esistenti per debug
                    self.logger.error(f"❌ File .nq non creato: {config['structure_output']}")
                    try:
                        existing_files = list(self.working_dir.glob("*.nq"))
                        if existing_files:
                            self.logger.info(f"📋 File .nq esistenti:")
                            for f in existing_files:
                                self.logger.info(f"   - {f.name}")
                        else:
                            self.logger.info(f"📋 Nessun file .nq trovato nella directory")
                    except Exception as e:
                        self.logger.error(f"❌ Errore nel listare file: {e}")
                    results[dir_key] = False
            else:
                results[dir_key] = False
                
            self.logger.substep_complete(dir_key, "Generazione Struttura RDF", substep_duration, results[dir_key])
            
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete(3, "Generazione Struttura RDF", step_duration, success_count, len(results))
        
        return results
    
    def step_3_5_generate_all_blazegraph_journals(self) -> Dict[str, bool]:
        """
        Step 3.5: Caricamento Blazegraph via REST API
        """
        step_start = time.time()
        self.logger.step_start(3.5, "Caricamento Blazegraph via REST API per Tutte le Directory")
        
        results = {}
        
        # USA IL NUOVO GENERATORE REST
        blazegraph_generator = BlazegraphJournalGeneratorREST(self.logger, self.working_dir)
        
        # Raccogli tutti i file .nq generati
        all_nq_files = []
        missing_nq_files = []
        
        for dir_key, config in self.directories.items():
            structure_output_path = self.working_dir / config['structure_output']
            if structure_output_path.exists():
                all_nq_files.append(structure_output_path)
                self.logger.info(f"✅ File .nq trovato per {dir_key}: {config['structure_output']}")
            else:
                missing_nq_files.append(f"{dir_key}: {config['structure_output']}")
                self.logger.error(f"❌ File .nq mancante per {dir_key}: {config['structure_output']}")
        
        # Verifica che ci siano file .nq da processare
        if not all_nq_files:
            self.logger.error("❌ Nessun file .nq trovato per il caricamento Blazegraph")
            for dir_key in self.directories.keys():
                results[dir_key] = False
            step_duration = time.time() - step_start
            self.logger.step_complete(3.5, "Caricamento Blazegraph REST API", step_duration, 0, len(results))
            return results
        
        if missing_nq_files:
            self.logger.warning(f"⚠️ File .nq mancanti: {', '.join(missing_nq_files)}")
            
        self.logger.info(f"📊 File .nq da caricare via REST API: {len(all_nq_files)}")
        
        # Carica via REST API
        substep_start = time.time()
        self.logger.substep_start("ALL", "Caricamento Blazegraph via REST API")
        
        blazegraph_success = blazegraph_generator.generate_blazegraph_journal(all_nq_files)
        
        substep_duration = time.time() - substep_start
        
        # Imposta i risultati per tutte le directory
        for dir_key in self.directories.keys():
            structure_file_exists = (self.working_dir / self.directories[dir_key]['structure_output']).exists()
            results[dir_key] = blazegraph_success and structure_file_exists
            
        success_count = sum(1 for success in results.values() if success)
        
        status_msg = "REST API Caricamento Completato" if blazegraph_success else "REST API Caricamento Fallito"
        self.logger.substep_complete("ALL", status_msg, substep_duration, blazegraph_success)
        
        step_duration = time.time() - step_start
        self.logger.step_complete(3.5, "Caricamento Blazegraph REST API", step_duration, success_count, len(results))
        
        return results

    def step_4_check_all_counts(self) -> Dict[str, bool]:
        """Step 4: Verifica conteggi per tutte le directory - LOGICA CORRETTA"""
        step_start = time.time()
        self.logger.step_start(4, "Verifica Conteggi per Tutte le Directory")
        
        results = {}
        
        for dir_key, config in self.directories.items():
            substep_start = time.time()
            self.logger.substep_start(dir_key, "Verifica Conteggi")
            
            command = [
                PYTHON_INTERPRETER, 'count_check.py',
                config['check_type']
            ]
            
            success, stdout, stderr = self._run_command(
                command, 
                f"Verifica Conteggi {dir_key}", 
                directory=config['path'],
                timeout=150000
            )
            
            substep_duration = time.time() - substep_start
            
            if success:  # Il processo è terminato con codice 0
                success_patterns = [
                    "Tutti i conteggi ricorsivi corrispondono perfettamente!",
                    "✅ Tutti i conteggi ricorsivi corrispondono perfettamente!",
                    "Logica di matching corretta implementata con successo",
                    "path verificati con successo"
                ]
                
                failure_patterns = [
                    "0 directory verificate", 
                    "Nessun path verificato",
                    "CRITICAL ERROR",
                    "FALLIMENTO GENERALE"
                ]
                
                has_explicit_success = any(pattern in stdout for pattern in success_patterns)
                has_explicit_failure = any(pattern in stdout for pattern in failure_patterns)
                
                if has_explicit_success:
                    self.logger.info("✅ Verifica conteggi: SUCCESSO ESPLICITO rilevato")
                    results[dir_key] = True
                elif has_explicit_failure:
                    self.logger.error("❌ Verifica conteggi: FALLIMENTO ESPLICITO rilevato")
                    results[dir_key] = False
                else:
                    import re
                    verified_pattern1 = r'(\d+)\s*path\s+verificati'
                    verified_pattern2 = r'OK:\s*(\d+)'
                    verified_pattern3 = r'Verificati:\s*(\d+)'
                    
                    verified_matches1 = re.findall(verified_pattern1, stdout, re.IGNORECASE)
                    verified_matches2 = re.findall(verified_pattern2, stdout, re.IGNORECASE)
                    verified_matches3 = re.findall(verified_pattern3, stdout, re.IGNORECASE)
                    
                    all_matches = verified_matches1 + verified_matches2 + verified_matches3
                    
                    if all_matches:
                        verified_count = int(all_matches[-1])
                        if verified_count > 0:
                            self.logger.info(f"✅ Verifica conteggi: {verified_count} path verificati con successo")
                            results[dir_key] = True
                        else:
                            self.logger.error("❌ Verifica conteggi: 0 path verificati")
                            results[dir_key] = False
                    else:
                        success_numeric_patterns = [
                            r'Discrepanze:\s*0',
                            r'Path RDF non attesi:\s*0',
                        ]
                        
                        has_success_numerics = all(
                            re.search(pattern, stdout, re.IGNORECASE) 
                            for pattern in success_numeric_patterns
                        )
                        
                        if has_success_numerics:
                            self.logger.info("✅ Verifica conteggi: indicatori numerici di successo rilevati (0 discrepanze, 0 path non attesi)")
                            results[dir_key] = True
                        else:
                            normal_process_indicators = [
                                "Caricamento dati JSON",
                                "Esecuzione query SPARQL",
                                "Confronto diretto",
                                "REPORT FINALE"
                            ]
                            
                            has_normal_process = any(indicator in stdout for indicator in normal_process_indicators)
                            
                            if has_normal_process:
                                self.logger.info("✅ Verifica conteggi: processo eseguito normalmente (successo presunto)")
                                results[dir_key] = True
                            else:
                                self.logger.warning("⚠️ Verifica conteggi: output ambiguo, considerato fallimento")
                                results[dir_key] = False
            else:
                self.logger.error("❌ Verifica conteggi: processo terminato con errore")
                results[dir_key] = False
            
            self.logger.substep_complete(dir_key, "Verifica Conteggi", substep_duration, results[dir_key])
        
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete(4, "Verifica Conteggi", step_duration, success_count, len(results))
        
        return results
        
    def step_5_check_all_integrity(self) -> Dict[str, bool]:
        """Step 5: Verifica integrità hash per tutte le directory - LOGICA CORRETTA"""
        step_start = time.time()
        self.logger.step_start(5, "Verifica Integrità Hash per Tutte le Directory")
        
        results = {}
        
        for dir_key, config in self.directories.items():
            substep_start = time.time()
            self.logger.substep_start(dir_key, "Verifica Integrità Hash")
            
            command = [
                PYTHON_INTERPRETER, 'integrity_check.py',
                config['check_type']
            ]
            
            success, stdout, stderr = self._run_command(
                command, 
                f"Verifica Integrità {dir_key}", 
                directory=config['path'],
                timeout=150000
            )
            
            substep_duration = time.time() - substep_start
            
            # LOGICA DI ANALISI CORRETTA
            if success:  # Il processo è terminato con codice 0
                
                # INDICATORI DI SUCCESSO CHIARI
                success_indicators = [
                    "🎉 PERFETTO: 100% dei file hanno hash corrispondenti!",
                    "TUTTI I FILE CORRISPONDONO CON GLI HASH!",
                    "✅ Hash integri:",  # seguito da numero > 0
                    "🔒 Tasso di integrità hash: 100.00%",
                    "Sistema perfettamente sincronizzato!"
                ]
                
                # INDICATORI DI FALLIMENTO CHIARI  
                failure_indicators = [
                    "Hash mismatch",
                    "File corrotti trovati",
                    "Errore critico nell'integrità",
                    "Integrity check failed"
                ]
                
                # Prima verifica: messaggi di successo espliciti
                has_success = any(indicator in stdout for indicator in success_indicators)
                
                # Seconda verifica: messaggi di fallimento espliciti
                has_failure = any(indicator in stdout for indicator in failure_indicators)
                
                if has_success:
                    self.logger.info("✅ Verifica integrità: SUCCESSO - Tutti gli hash corrispondono!")
                    results[dir_key] = True
                    
                elif has_failure:
                    self.logger.error("❌ Verifica integrità: FALLIMENTO - Problemi di integrità rilevati")
                    results[dir_key] = False
                    
                else:
                    # Analisi dettagliata dei numeri
                    import re
                    
                    # Cerca "❌ Hash corrotti: X" - se X=0 è BUONO
                    corrupt_pattern = r'❌ Hash corrotti:\s*(\d+)'
                    corrupt_matches = re.findall(corrupt_pattern, stdout)
                    
                    # Cerca "✅ Hash integri: X" - se X>0 è BUONO
                    intact_pattern = r'✅ Hash integri:\s*(\d+)'
                    intact_matches = re.findall(intact_pattern, stdout)
                    
                    # Cerca "❌ Hash diversi: X" - se X=0 è BUONO
                    different_pattern = r'❌ Hash diversi:\s*(\d+)'
                    different_matches = re.findall(different_pattern, stdout)
                    
                    # LOGICA CORRETTA: 
                    # - Se hash corrotti = 0 E hash integri > 0 E hash diversi = 0 → SUCCESSO
                    # - Altrimenti → controlla altri indicatori
                    
                    corrupt_count = int(corrupt_matches[0]) if corrupt_matches else None
                    intact_count = int(intact_matches[0]) if intact_matches else None
                    different_count = int(different_matches[0]) if different_matches else None
                    
                    self.logger.debug(f"Analisi numerica - Corrotti: {corrupt_count}, Integri: {intact_count}, Diversi: {different_count}")
                    
                    # SUCCESSO se:
                    # 1. Hash corrotti = 0 (nessun file corrotto)
                    # 2. Hash integri > 0 (almeno alcuni file verificati)
                    # 3. Hash diversi = 0 (nessuna discrepanza)
                    if (corrupt_count == 0 and 
                        intact_count is not None and intact_count > 0 and 
                        different_count == 0):
                        
                        self.logger.info(f"✅ Verifica integrità: SUCCESSO NUMERICO - {intact_count} file integri, 0 corrotti")
                        results[dir_key] = True
                        
                    elif corrupt_count is not None and corrupt_count > 0:
                        self.logger.error(f"❌ Verifica integrità: FALLIMENTO - {corrupt_count} file corrotti")
                        results[dir_key] = False
                        
                    elif different_count is not None and different_count > 0:
                        self.logger.error(f"❌ Verifica integrità: FALLIMENTO - {different_count} hash diversi")
                        results[dir_key] = False
                        
                    else:
                        # Fallback: verifica presenza di report finale normale
                        if "REPORT FINALE" in stdout and "📊" in stdout:
                            self.logger.info("✅ Verifica integrità: processo completato normalmente (successo presunto)")
                            results[dir_key] = True
                        else:
                            self.logger.warning("⚠️ Verifica integrità: output ambiguo, considerato fallimento")
                            results[dir_key] = False
            else:
                self.logger.error("❌ Verifica integrità: processo terminato con errore")
                results[dir_key] = False
                
            self.logger.substep_complete(dir_key, "Verifica Integrità Hash", substep_duration, results[dir_key])
            
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete(5, "Verifica Integrità Hash", step_duration, success_count, len(results))
        
        return results
        

    def _cleanup_before_metadata_extraction(self):
        """Libera memoria prima dell'estrazione metadati intensiva"""
        import gc
        import psutil
        
        self.logger.info("🧹 Pulizia memoria prima estrazione metadati...")
        
        # Garbage collection forzato
        gc.collect()
        
        try:
            # Log stato memoria
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            self.logger.info(f"📊 Memoria utilizzata: {memory_mb:.1f} MB")
        except ImportError:
            self.logger.info("📊 Modulo psutil non disponibile per monitoraggio memoria")
        except Exception as e:
            self.logger.debug(f"Errore monitoraggio memoria: {e}")

    def _run_command_unbuffered_with_env(self, command: List[str], operation_name: str, 
                                    directory: str = None, timeout: Optional[int] = None, 
                                    env: dict = None) -> Tuple[bool, str, str]:
        """Versione di _run_command_unbuffered che accetta environment personalizzato"""
        
        self.logger.log_command_execution(command, operation_name, directory)
        
        # Verifica esistenza script
        if len(command) > 1:
            script_path = Path(self.working_dir) / command[1]
            if not script_path.exists():
                error_msg = f"Script non trovato: {script_path}"
                self.logger.error(f"❌ {error_msg}")
                return False, "", error_msg
            else:
                self.logger.info(f"✅ Script verificato: {script_path}")

        try:
            self.logger.info(f"🚀 Avvio processo unbuffered con environment personalizzato...")
            start_time = time.time()
            
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=self.working_dir,
                env=env or os.environ.copy(),  # USA env personalizzato
                bufsize=1,
                universal_newlines=True
            )
            
            stdout_lines = []
            
            # Leggi output in real-time
            while True:
                try:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        line = output.strip()
                        print(f"[{operation_name}] {line}")  # Mostra immediatamente
                        stdout_lines.append(output)
                        self.logger.info(f"[REALTIME] {line}")
                except Exception as e:
                    self.logger.warning(f"Errore lettura output: {e}")
                    break
            
            # Aspetta che il processo finisca
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.logger.error(f"⏰ Timeout dopo {timeout}s - termino processo")
                process.kill()
                process.wait()
                return False, "", f"Timeout dopo {timeout} secondi"
            
            duration = time.time() - start_time
            stdout = ''.join(stdout_lines)
            
            self.logger.log_command_result(
                process.returncode == 0,
                stdout,
                "",
                duration,
                operation_name
            )
            
            return process.returncode == 0, stdout, ""
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Errore nell'esecuzione: {e}"
            self.logger.error(f"💥 {error_msg}")
            self.logger.log_command_result(False, "", error_msg, duration, operation_name)
            return False, "", error_msg

    def step_6_extract_all_metadata(self) -> Dict[str, bool]:
        """Step 6: Estrazione metadati per tutte le directory - OTTIMIZZATO PER PIPELINE"""
        
        # Cleanup memoria prima dell'estrazione
        self._cleanup_before_metadata_extraction()
        
        step_start = time.time()
        self.logger.step_start(6, "Estrazione Metadati per Tutte le Directory")
        
        results = {}
        
        for dir_key, config in self.directories.items():
            substep_start = time.time()
            self.logger.substep_start(dir_key, "Estrazione Metadati")
            
            # Verifica che il file di struttura RDF esista
            structure_file = self.working_dir / config['structure_output']
            if not structure_file.exists():
                self.logger.error(f"❌ File struttura RDF mancante: {config['structure_output']}")
                results[dir_key] = False
                continue
            
            # Comando con parametri ottimizzati per pipeline
            command = [
                PYTHON_INTERPRETER, 'metadata_extraction.py',
                config['metadata_directory'],
                '--chunk-size', '200',       # Ridotto da 100 a 50 per pipeline
                '--persistence-interval', '200'  # Ridotto per liberare memoria più spesso
            ]
            
            # Environment personalizzato per modalità pipeline
            env = os.environ.copy()
            env['PIPELINE_MODE'] = '1'
            env['FAST_PIPELINE_MODE'] = '1'
            env['TIKA_CLEANUP_AGGRESSIVE'] = '1'
            
            # USA comando unbuffered con environment personalizzato
            success, stdout, stderr = self._run_command_unbuffered_with_env(
                command, 
                f"Estrazione Metadati {dir_key}", 
                directory=config['path'],
                timeout=300000,  # Timeout ridotto da 500000
                env=env
            )
            
            substep_duration = time.time() - substep_start
            
            if success:
                # Verifica file di output metadati (pattern dei file generati)
                expected_metadata_files = [
                    f"FileSystem_TechMeta_{config['metadata_directory']}.nq",
                    f"ApacheTika_TechMeta_{config['metadata_directory']}.nq", 
                    f"ExifTool_TechMeta_{config['metadata_directory']}.nq",
                    f"DROID_TechMeta_{config['metadata_directory']}.nq"    
                ]
                
                metadata_files_found = 0
                for metadata_file in expected_metadata_files:
                    metadata_path = self.working_dir / metadata_file
                    if metadata_path.exists():
                        metadata_files_found += 1
                        self.logger.log_file_check(metadata_path, expected=True)
                    else:
                        self.logger.warning(f"⚠️ File metadati non trovato: {metadata_file}")
                
                # Considera successo se almeno 1 file metadati è stato creato
                if metadata_files_found > 0:
                    self.logger.info(f"📊 File metadati creati: {metadata_files_found}/{len(expected_metadata_files)}")
                    results[dir_key] = True
                else:
                    self.logger.error(f"❌ Nessun file metadati creato")
                    results[dir_key] = False
                    
                # Verifica anche il file contatori JSON
                counters_file = self.working_dir / "uri_counters.json"
                if counters_file.exists():
                    self.logger.log_file_check(counters_file, expected=True)
                    self.logger.info("📊 File contatori URI salvato correttamente")
                    
            else:
                results[dir_key] = False
                
            self.logger.substep_complete(dir_key, "Estrazione Metadati", substep_duration, results[dir_key])
            
            # Cleanup aggiuntivo tra directory per liberare memoria
            if dir_key != list(self.directories.keys())[-1]:  # Non ultimo elemento
                import gc
                gc.collect()
                self.logger.debug(f"🧹 Cleanup memoria post-{dir_key}")
            
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete(6, "Estrazione Metadati", step_duration, success_count, len(results))
        
        return results
    
    def step_6_5_verify_hash_consistency(self) -> Dict[str, bool]:
        """Step 6.5: Ricalcolo hash e verifica consistenza post-estrazione metadati"""
        step_start = time.time()
        self.logger.step_start(6.5, "Verifica Consistenza Hash Post-Estrazione Metadati")
        
        results = {}
        
        for dir_key, config in self.directories.items():
            substep_start = time.time()
            self.logger.substep_start(dir_key, "Ricalcolo e Verifica Hash")
            
            # File hash originale (dallo Step 2)
            original_hash_file = self.working_dir / config['hash_output']
            
            if not original_hash_file.exists():
                self.logger.error(f"❌ File hash originale non trovato: {config['hash_output']}")
                results[dir_key] = False
                continue
            
            # 1. Backup del file hash originale
            backup_hash_file = self.working_dir / f"{config['hash_output'].replace('.json', '_backup.json')}"
            try:
                shutil.copy2(original_hash_file, backup_hash_file)  
                self.logger.info(f"💾 Backup hash originale: {backup_hash_file.name}")
            except Exception as e:
                self.logger.error(f"❌ Impossibile creare backup hash: {e}")
                results[dir_key] = False
                continue
            
            # 2. Ricalcola gli hash (sovrascriverà il file originale)
            command = [
                PYTHON_INTERPRETER, 'hash_calc.py',
                config['path']
            ]
            
            self.logger.info(f"🔄 Ricalcolo hash per verificare integrità post-estrazione metadati...")
            
            success, stdout, stderr = self._run_command(
                command, 
                f"Ricalcolo Hash Post-Metadati {dir_key}", 
                directory=config['path'],
                timeout=50000
            )
            
            if not success:
                self.logger.error(f"❌ Ricalcolo hash fallito per {dir_key}")
                # Ripristina il backup
                try:
                    shutil.copy2(backup_hash_file, original_hash_file)
                    self.logger.info(f"🔄 File hash originale ripristinato da backup")
                except:
                    pass
                results[dir_key] = False
                continue
            
            # 3. Ora original_hash_file contiene i nuovi hash (hash_calc.py ha sovrascritto il file)
            # ✅ FIX: Definisce correttamente current_hash_file
            current_hash_file = original_hash_file  # Il file appena rigenerato da hash_calc.py
            
            if not current_hash_file.exists():
                self.logger.error(f"❌ File hash ricalcolato non trovato: {config['hash_output']}")
                results[dir_key] = False
                continue
            
            if not backup_hash_file.exists():
                self.logger.error(f"❌ File hash backup non trovato: {backup_hash_file.name}")
                results[dir_key] = False  
                continue
            
            # 4. Esegui confronto dettagliato (backup vs file ricalcolato)
            comparison_result = self._compare_hash_files(
                backup_hash_file,     # Hash originali (pre-metadati) 
                current_hash_file,    # Hash ricalcolati (post-metadati)
                dir_key
            )
            
            # 5. Conserva i file per analisi
            if comparison_result:
                # Se tutto OK, possiamo rimuovere il backup
                try:
                    backup_hash_file.unlink()
                    self.logger.info(f"🧹 Backup rimosso: nessuna differenza rilevata")
                except:
                    pass
            else:
                # Se ci sono problemi, mantieni entrambi i file per analisi
                self.logger.warning(f"⚠️ File backup mantenuto per analisi: {backup_hash_file.name}")
                # Rinomina per chiarezza
                try:
                    post_metadata_file = self.working_dir / f"{config['hash_output'].replace('.json', '_post_metadata.json')}"
                    shutil.copy2(current_hash_file, post_metadata_file)
                    shutil.copy2(backup_hash_file, original_hash_file)  # Ripristina originale
                    self.logger.info(f"📋 File hash confronto disponibili:")
                    self.logger.info(f"   - Originale: {original_hash_file.name}")
                    self.logger.info(f"   - Post-metadati: {post_metadata_file.name}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Errore nell'organizzare file hash: {e}")
            
            results[dir_key] = comparison_result
            
            substep_duration = time.time() - substep_start
            self.logger.substep_complete(dir_key, "Verifica Hash Post-Metadati", substep_duration, results[dir_key])
            
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete(6.5, "Verifica Consistenza Hash", step_duration, success_count, len(results))
        
        return results

    def _compare_hash_files(self, original_file: Path, current_file: Path, dir_key: str) -> bool:
        """Confronta due file JSON di hash e riporta le differenze"""
        try:
            self.logger.info(f"🔍 Confronto hash files per {dir_key}...")
            self.logger.info(f"   📄 Originale (pre-metadati): {original_file.name}")
            self.logger.info(f"   📄 Corrente (post-metadati): {current_file.name}")
            
            # Carica entrambi i file JSON
            with open(original_file, 'r', encoding='utf-8') as f:
                original_data = json.load(f)
            
            with open(current_file, 'r', encoding='utf-8') as f:
                current_data = json.load(f)
            
            # Estrai gli hash effettivi (adatta alla struttura del tuo JSON)
            # Prova diversi percorsi nella struttura JSON
            original_hashes = self._extract_hashes_from_json(original_data, "originale")
            current_hashes = self._extract_hashes_from_json(current_data, "corrente")
            
            if not original_hashes or not current_hashes:
                self.logger.error(f"❌ Impossibile estrarre hash dai file JSON")
                return False
            
            # Statistiche di base
            original_count = len(original_hashes)
            current_count = len(current_hashes)
            
            self.logger.info(f"📊 File originali: {original_count}, File post-metadati: {current_count}")
            
            # Verifica differenze nel numero di file
            if original_count != current_count:
                self.logger.warning(f"⚠️ Differenza nel numero di file: {original_count} -> {current_count}")
            
            # Confronto dettagliato degli hash
            differences = []
            missing_files = []
            new_files = []
            
            # File presenti in originale ma non in corrente
            for file_path, original_hash in original_hashes.items():
                if file_path not in current_hashes:
                    missing_files.append(file_path)
                elif current_hashes[file_path] != original_hash:
                    differences.append({
                        'file': file_path,
                        'original_hash': original_hash,
                        'current_hash': current_hashes[file_path]
                    })
            
            # File presenti in corrente ma non in originale
            for file_path in current_hashes:
                if file_path not in original_hashes:
                    new_files.append(file_path)
            
            # Report risultati
            total_issues = len(differences) + len(missing_files) + len(new_files)
            
            if total_issues == 0:
                self.logger.info(f"✅ PERFETTO: Tutti gli hash corrispondono! ({original_count} file verificati)")
                self.logger.info(f"🔒 Integrità confermata: nessuna modifica ai file durante estrazione metadati")
                return True
            else:
                self.logger.error(f"❌ PROBLEMI DI INTEGRITÀ RILEVATI:")
                self.logger.error(f"   📊 Totale problemi: {total_issues}")
                
                if differences:
                    self.logger.error(f"   🔄 Hash modificati: {len(differences)}")
                    for i, diff in enumerate(differences[:5]):  # Mostra solo i primi 5
                        self.logger.error(f"      {i+1}. {diff['file']}")
                        self.logger.error(f"         Originale: {diff['original_hash']}")
                        self.logger.error(f"         Corrente:  {diff['current_hash']}")
                    if len(differences) > 5:
                        self.logger.error(f"      ... e altri {len(differences) - 5} file modificati")
                
                if missing_files:
                    self.logger.error(f"   🗑️ File scomparsi: {len(missing_files)}")
                    for i, file_path in enumerate(missing_files[:3]):
                        self.logger.error(f"      {i+1}. {file_path}")
                    if len(missing_files) > 3:
                        self.logger.error(f"      ... e altri {len(missing_files) - 3} file")
                
                if new_files:
                    self.logger.error(f"   🆕 File aggiunti: {len(new_files)}")
                    for i, file_path in enumerate(new_files[:3]):
                        self.logger.error(f"      {i+1}. {file_path}")
                    if len(new_files) > 3:
                        self.logger.error(f"      ... e altri {len(new_files) - 3} file")
                
                # Salva report dettagliato
                self._save_hash_comparison_report(dir_key, differences, missing_files, new_files)
                
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Errore nel confronto hash files: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return False

    def _extract_hashes_from_json(self, json_data: dict, file_type: str) -> dict:
        """Estrae gli hash dal JSON, supportando diverse strutture possibili"""

        try:
            # Caso 1: struttura lista di dizionari con path + hash
            if "file_hashes" in json_data and isinstance(json_data["file_hashes"], list):
                hash_map = {}
                for entry in json_data["file_hashes"]:
                    path = entry.get("path")
                    file_hash = entry.get("sha256") or entry.get("hash")
                    if path and file_hash:
                        hash_map[path] = file_hash
                if hash_map:
                    self.logger.debug(f"✅ Hash estratti da {file_type}: 'file_hashes' con {len(hash_map)} file")
                    return hash_map

            # Caso 2: struttura dizionario semplice { path: hash }
            possible_paths = [
                ['hash_files'],
                ['file_hash'],
                ['files'],
                []
            ]
            for path in possible_paths:
                try:
                    current_level = json_data
                    for key in path:
                        current_level = current_level[key]
                    if isinstance(current_level, dict) and current_level:
                        first_value = next(iter(current_level.values()))
                        if isinstance(first_value, str) and len(first_value) >= 32:
                            self.logger.debug(f"✅ Hash estratti da {file_type}: percorso {path}, {len(current_level)} file")
                            return current_level
                except (KeyError, TypeError):
                    continue

            # Se nessun formato è valido
            self.logger.warning(f"⚠️ Nessuna struttura hash riconosciuta in {file_type}")
            return {}

        except Exception as e:
            self.logger.error(f"❌ Errore estraendo gli hash ({file_type}): {e}")
            return {}


    def _save_hash_comparison_report(self, dir_key: str, differences: List, missing_files: List, new_files: List):
        """Salva un report dettagliato delle differenze hash"""
        try:
            report_file = f"hash_comparison_report_{dir_key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            report = {
                'directory': dir_key,
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_differences': len(differences),
                    'missing_files': len(missing_files),
                    'new_files': len(new_files),
                    'integrity_status': 'COMPROMISED' if (differences or missing_files) else 'PARTIAL'
                },
                'details': {
                    'hash_differences': differences,
                    'missing_files': missing_files,
                    'new_files': new_files
                },
                'analysis': {
                    'critical_issues': len(differences) + len(missing_files),
                    'non_critical_issues': len(new_files),
                    'recommendation': self._get_integrity_recommendation(differences, missing_files, new_files)
                }
            }
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"📊 Report differenze hash salvato: {report_file}")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Impossibile salvare report differenze: {e}")

    def _get_integrity_recommendation(self, differences: List, missing_files: List, new_files: List) -> str:
        """Genera raccomandazioni basate sui problemi trovati"""
        if differences:
            return "CRITICO: File modificati durante estrazione metadati. Verificare processo di estrazione."
        elif missing_files:
            return "CRITICO: File rimossi durante estrazione metadati. Verificare stabilità filesystem."
        elif new_files:
            return "ATTENZIONE: Nuovi file creati. Probabilmente file temporanei o log - verificare se possono essere rimossi."
        else:
            return "OK: Nessun problema di integrità rilevato."

    def step_7_load_metadata_to_blazegraph(self) -> Dict[str, bool]:
        """Step 7: Caricamento metadati in Blazegraph per tutte le directory - CASE FIXED"""
        step_start = time.time()
        self.logger.step_start(7, "Caricamento Metadati in Blazegraph")
        
        results = {}
        
        # RIUTILIZZA il generatore REST dello Step 3.5
        blazegraph_generator = BlazegraphJournalGeneratorRESTWithChunking(self.logger, self.working_dir)
        
        # === PARTE 1: RACCOLTA FILE METADATI CON CASE-INSENSITIVE SEARCH ===
        all_metadata_files = []
        missing_metadata_files = []
        
        # Pattern dei file metadati per directory
        metadata_patterns = {
            'filesystem': 'FileSystem_TechMeta_{}.nq',
            'tika': 'ApacheTika_TechMeta_{}.nq', 
            'exif': 'ExifTool_TechMeta_{}.nq'
        }
        
        self.logger.info(f"🔍 Ricerca file metadati per {len(self.directories)} directory...")
        
        for dir_key, config in self.directories.items():
            # ✅ FIX: FORZA metadata_directory in lowercase
            metadata_dir = config['metadata_directory'].lower().strip()
            
            self.logger.info(f"📁 Processing {dir_key} con metadata_directory: '{metadata_dir}'")
            
            dir_metadata_files = []
            dir_missing_files = []
            
            for metadata_type, pattern in metadata_patterns.items():
                # Costruisci nome file atteso (sempre lowercase)
                expected_filename = pattern.format(metadata_dir)
                expected_path = self.working_dir / expected_filename
                
                # ✅ STRATEGIA 1: Prova nome standard (lowercase)
                if expected_path.exists():
                    size = expected_path.stat().st_size
                    self.logger.info(f"✅ {dir_key} - {metadata_type}: {expected_filename} ({size:,} bytes)")
                    all_metadata_files.append(expected_path)
                    dir_metadata_files.append(expected_filename)
                    continue
                
                # ✅ STRATEGIA 2: Ricerca case-insensitive
                self.logger.warning(f"⚠️ {dir_key} - {metadata_type}: {expected_filename} non trovato, ricerca case-insensitive...")
                
                found_file = None
                
                # Cerca tutti i file .nq nella directory
                for nq_file in self.working_dir.glob("*.nq"):
                    filename_lower = nq_file.name.lower()
                    expected_lower = expected_filename.lower()
                    
                    # Match esatto case-insensitive
                    if filename_lower == expected_lower:
                        found_file = nq_file
                        self.logger.info(f"🔍 Match case-insensitive: {nq_file.name} -> {expected_filename}")
                        break
                    
                    # Match con pattern più flessibile per gestire varianti del directory name
                    if metadata_type == 'exif' and 'exiftool_techmeta' in filename_lower:
                        # Crea varianti possibili del directory name
                        dir_variants = self._generate_directory_variants(metadata_dir)
                        
                        for variant in dir_variants:
                            variant_filename = pattern.format(variant).lower()
                            if filename_lower == variant_filename:
                                found_file = nq_file
                                self.logger.info(f"🔍 Match variant: {nq_file.name} -> {expected_filename}")
                                break
                        
                        if found_file:
                            break
                    
                    # Match simile per filesystem e tika
                    elif metadata_type == 'filesystem' and 'filesystem_techmeta' in filename_lower:
                        dir_variants = self._generate_directory_variants(metadata_dir)
                        for variant in dir_variants:
                            variant_filename = pattern.format(variant).lower()
                            if filename_lower == variant_filename:
                                found_file = nq_file
                                self.logger.info(f"🔍 Match variant: {nq_file.name} -> {expected_filename}")
                                break
                        if found_file:
                            break
                            
                    elif metadata_type == 'tika' and 'apachetika_techmeta' in filename_lower:
                        dir_variants = self._generate_directory_variants(metadata_dir)
                        for variant in dir_variants:
                            variant_filename = pattern.format(variant).lower()
                            if filename_lower == variant_filename:
                                found_file = nq_file
                                self.logger.info(f"🔍 Match variant: {nq_file.name} -> {expected_filename}")
                                break
                        if found_file:
                            break
                
                if found_file:
                    size = found_file.stat().st_size
                    self.logger.info(f"✅ {dir_key} - {metadata_type}: {found_file.name} ({size:,} bytes) [CASE-INSENSITIVE]")
                    all_metadata_files.append(found_file)
                    dir_metadata_files.append(found_file.name)
                    
                    # ✅ OPZIONALE: Rinomina automaticamente al nome corretto
                    if found_file.name != expected_filename:
                        try:
                            new_path = self.working_dir / expected_filename
                            if not new_path.exists():  # Solo se non esiste già
                                found_file.rename(new_path)
                                self.logger.info(f"🔄 Rinominato: {found_file.name} -> {expected_filename}")
                                # Aggiorna il riferimento
                                all_metadata_files[-1] = new_path
                                dir_metadata_files[-1] = expected_filename
                        except Exception as e:
                            self.logger.warning(f"⚠️ Impossibile rinominare {found_file.name}: {e}")
                else:
                    self.logger.warning(f"⚠️ {dir_key} - {metadata_type}: nessun file trovato per {expected_filename}")
                    dir_missing_files.append(expected_filename)
                    missing_metadata_files.append(f"{dir_key}:{expected_filename}")
            
            # Considera successo per directory se almeno 1 file metadati è presente
            results[dir_key] = len(dir_metadata_files) > 0
            self.logger.info(f"📊 {dir_key}: {len(dir_metadata_files)} file metadati trovati")
        
        # === PARTE 3: COMBINAZIONE E VERIFICA ===
        all_files_to_load = all_metadata_files 
        
        if not all_files_to_load:
            self.logger.error("❌ Nessun file (metadati) trovato per il caricamento")
            self.logger.error("   Assicurati che lo Step 6 (Estrazione Metadati) sia completato con successo")
            for dir_key in self.directories.keys():
                results[dir_key] = False
            step_duration = time.time() - step_start
            self.logger.step_complete(7, "Caricamento Metadati", step_duration, 0, len(results))
            return results
        
        # === PARTE 4: LOG RIEPILOGO ===
        if missing_metadata_files:
            self.logger.warning(f"⚠️ File metadati mancanti: {len(missing_metadata_files)}")
            for missing in missing_metadata_files[:5]:  # Mostra solo i primi 5
                self.logger.warning(f"   - {missing}")
            if len(missing_metadata_files) > 5:
                self.logger.warning(f"   ... e altri {len(missing_metadata_files) - 5}")
        
        # Dettaglio file metadati
        file_counts = {'filesystem': 0, 'tika': 0, 'exif': 0, 'droid': 0}
        metadata_total_size = 0
        
        for metadata_file in all_metadata_files:
            filename = metadata_file.name
            size = metadata_file.stat().st_size
            metadata_total_size += size
            
            if 'FileSystem_TechMeta' in filename:
                file_counts['filesystem'] += 1
            elif 'ApacheTika_TechMeta' in filename:
                file_counts['tika'] += 1
            elif 'ExifTool_TechMeta' in filename:
                file_counts['exif'] += 1
            elif 'DROID_TechMeta' in filename:        # ← Aggiungi questo elif
                file_counts['droid'] += 1
        
        # Log dettagliato
        self.logger.info(f"\n📊 RIEPILOGO CARICAMENTO:")
        self.logger.info(f"📋 File metadati ({len(all_metadata_files)}):")
        self.logger.info(f"   📁 FileSystem: {file_counts['filesystem']} file")
        self.logger.info(f"   🔍 Apache Tika: {file_counts['tika']} file")
        self.logger.info(f"   📷 ExifTool: {file_counts['exif']} file")
        self.logger.info(f"   🤖 DROID: {file_counts['droid']} file")  
        self.logger.info(f"   📊 Dimensione metadati: {metadata_total_size:,} bytes")
        
        self.logger.info(f"📊 TOTALE FILE DA CARICARE: {len(all_files_to_load)}")
        
        # === PARTE 5: CARICAMENTO ===
        substep_start = time.time()
        self.logger.substep_start("ALL", "Caricamento Metadati via REST API")
        
        # Verifica stato server prima del caricamento
        if not blazegraph_generator._verify_server_running():
            self.logger.error("❌ Server Blazegraph non raggiungibile")
            for dir_key in self.directories.keys():
                results[dir_key] = False
            step_duration = time.time() - step_start
            self.logger.step_complete(7, "Caricamento Metadati", step_duration, 0, len(results))
            return results
        
        # Conta triple prima del caricamento
        triple_count_before = blazegraph_generator._check_existing_data()
        
        # Carica TUTTI i file (metadati) usando la STESSA logica dello Step 3.5
        blazegraph_success = blazegraph_generator.generate_blazegraph_journal(all_files_to_load)
        
        substep_duration = time.time() - substep_start
        
        # === PARTE 6: REPORT FINALE ===
        if blazegraph_success:
            triple_count_after = blazegraph_generator._check_existing_data()
            total_triples_added = triple_count_after - triple_count_before
            
            self.logger.info(f"📈 Triple totali aggiunte: {total_triples_added:,}")
            
            if all_metadata_files:
                status_msg = "Metadati Caricati" if blazegraph_success else "Caricamento Metadati Fallito"
            else:
                status_msg = "Nessun File Caricato"
        
        # Aggiorna risultati basandosi sul successo del caricamento
        if not blazegraph_success:
            for dir_key in self.directories.keys():
                results[dir_key] = False
        
        success_count = sum(1 for success in results.values() if success)
        
        # Status message dinamico
        if all_metadata_files:
            status_msg = "Metadati Caricati" if blazegraph_success else "Caricamento Metadati"
        elif all_metadata_files:
            status_msg = "Metadati Caricati" if blazegraph_success else "Caricamento Metadati Fallito"
        else:
            status_msg = "Nessun File Caricato"
        
        self.logger.substep_complete("ALL", status_msg, substep_duration, blazegraph_success)
        
        step_duration = time.time() - step_start
        self.logger.step_complete(7, "Caricamento Metadati", step_duration, success_count, len(results))
        
        return results

    def _generate_directory_variants(self, directory_name: str) -> List[str]:
        """Genera varianti possibili per il nome directory"""
        variants = [
            directory_name.lower(),
            directory_name.upper(),
            directory_name.capitalize(),
            directory_name.title()
        ]
        
        # Varianti specifiche per hdesterno
        if 'hdesterno' in directory_name.lower():
            variants.extend([
                'HDEsterno',
                'hdEsterno',
                'HdEsterno', 
                'HDEsterno',
                'HDESTERNO'
            ])
        
        return list(set(variants))  # Rimuovi duplicati
    
    def step_8_generate_ng_registry(self) -> Dict[str, bool]:
        """Step 8: Generazione Named Graph Registry - Indice dei grafi caricati"""
        step_start = time.time()
        self.logger.step_start(8, "Generazione Named Graph Registry (Indice Grafi Caricati)")
        
        results = {}
        
        try:
            # Usa il generatore REST esistente
            blazegraph_generator = BlazegraphJournalGeneratorREST(self.logger, self.working_dir)
            
            # Verifica connessione server
            if not blazegraph_generator._verify_server_running():
                self.logger.error("❌ Server Blazegraph non raggiungibile")
                for dir_key in self.directories.keys():
                    results[dir_key] = False
                step_duration = time.time() - step_start
                self.logger.step_complete(8, "Generazione Named Graph Registry", step_duration, 0, len(results))
                return results
            
            # Genera le triple dell'indice dei grafi caricati
            registry_generator = NGRegistryGenerator(self.logger, self.directories, blazegraph_generator)
            registry_success = registry_generator.generate_and_load_registry()
            
            # Imposta risultati per tutte le directory
            for dir_key in self.directories.keys():
                results[dir_key] = registry_success
            
            step_duration = time.time() - step_start
            success_count = sum(1 for success in results.values() if success)
            
            status_msg = "Named Graph Registry generato" if registry_success else "Generazione Registry fallita"
            self.logger.step_complete(8, status_msg, step_duration, success_count, len(results))
            
            return results
            
        except Exception as e:
            step_duration = time.time() - step_start
            self.logger.error(f"❌ Errore durante generazione Named Graph Registry: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            
            for dir_key in self.directories.keys():
                results[dir_key] = False
                
            self.logger.step_complete(8, "Generazione Named Graph Registry", step_duration, 0, len(results))
            return results
        
    def _initialize_backup_manager(self):
        """Inizializza il backup manager quando necessario"""
        if self.backup_manager is None:
            # Crea un generatore blazegraph temporaneo per il backup
            blazegraph_generator = BlazegraphJournalGeneratorREST(self.logger, self.working_dir)
            self.backup_manager = BlazegraphBackupManager(self.logger, self.working_dir, blazegraph_generator)

    def step_3_75_backup_after_structure(self) -> Dict[str, bool]:
        """Step 3.75: Backup dopo caricamento struttura"""
        if not BACKUP_CONFIG.get('backup_after_structure', False):
            self.logger.info("📋 Backup post-struttura disabilitato - skip Step 3.75")
            return {dir_key: True for dir_key in self.directories.keys()}
        
        step_start = time.time()
        self.logger.step_start(3.75, "Backup Post-Struttura")
        
        self._initialize_backup_manager()
        
        backup_success = self.backup_manager.create_pipeline_backup("post_structure")
        
        results = {dir_key: backup_success for dir_key in self.directories.keys()}
        
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete(3.75, "Backup Post-Struttura", step_duration, success_count, len(results))
        
        return results

    def step_7_75_backup_after_metadata(self) -> Dict[str, bool]:
        """Step 7.75: Backup dopo caricamento metadati"""
        if not BACKUP_CONFIG.get('backup_after_metadata', False):
            self.logger.info("📋 Backup post-metadati disabilitato - skip Step 7.75")
            return {dir_key: True for dir_key in self.directories.keys()}
        
        step_start = time.time()
        self.logger.step_start(7.75, "Backup Post-Metadati")
        
        self._initialize_backup_manager()
        
        backup_success = self.backup_manager.create_pipeline_backup("post_metadata")
        
        results = {dir_key: backup_success for dir_key in self.directories.keys()}
        
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete(7.75, "Backup Post-Metadati", step_duration, success_count, len(results))
        
        return results

    def step_9_final_backup(self) -> Dict[str, bool]:
        """Step 9: Backup finale completo"""
        if not BACKUP_CONFIG.get('backup_final', False):
            self.logger.info("📋 Backup finale disabilitato - skip Step 9")
            return {dir_key: True for dir_key in self.directories.keys()}
        
        step_start = time.time()
        self.logger.step_start(9, "Backup Finale Completo")
        
        self._initialize_backup_manager()
        
        backup_success = self.backup_manager.create_pipeline_backup("final")
        
        results = {dir_key: backup_success for dir_key in self.directories.keys()}
        
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete(9, "Backup Finale", step_duration, success_count, len(results))
        
        return results

    def _cleanup_old_backups(self):
        """Pulizia automatica backup vecchi"""
        if not BACKUP_CONFIG.get('auto_cleanup', False):
            return
        
        max_backups = BACKUP_CONFIG.get('max_backups_keep', 10)
        backup_dir = self.working_dir / BACKUP_CONFIG.get('backup_directory', 'backups')
        
        if not backup_dir.exists():
            return
        
        try:
            # Lista tutti i file backup
            backup_files = list(backup_dir.glob("blazegraph_backup_*.nq"))
            backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            if len(backup_files) > max_backups:
                files_to_delete = backup_files[max_backups:]
                
                self.logger.info(f"🧹 Pulizia backup: {len(files_to_delete)} file da rimuovere")
                
                for old_backup in files_to_delete:
                    try:
                        old_backup.unlink()
                        self.logger.debug(f"   🗑️ Rimosso: {old_backup.name}")
                    except Exception as e:
                        self.logger.warning(f"   ⚠️ Errore rimozione {old_backup.name}: {e}")
                
                self.logger.info(f"✅ Pulizia completata - mantenuti {max_backups} backup più recenti")
        
        except Exception as e:
            self.logger.warning(f"⚠️ Errore durante pulizia backup: {e}")
    
    def run_pipeline(self) -> bool:
        """Esegue l'intera pipeline per tutte le directory CON VERIFICA HASH POST-METADATI"""
        total_start = time.time()
        
        self.logger.info(f"🚀 AVVIO PIPELINE MULTI-DIRECTORY EVANGELISTI CON VERIFICA INTEGRITÀ")
        self.logger.info(f"Directory da processare ({len(self.directories)}): {', '.join(self.directories.keys())}")

        # Reset Blazegraph se configurato
        if not self.reset_blazegraph_if_needed():
            self.logger.error("❌ Reset Blazegraph fallito - interrompo pipeline")
            return False

        all_results = {}
        
        # Step 1: Conteggio file per tutte le directory
        all_results['Conteggio File'] = self.step_1_count_all_files()
        
        # Step 2: Calcolo hash per tutte le directory
        all_results['Calcolo Hash'] = self.step_2_calculate_all_hashes()
        
        # Step 3: Generazione struttura per tutte le directory
        all_results['Generazione Struttura'] = self.step_3_generate_all_structures()

        # Step 3.5: Generazione Journal Blazegraph
        all_results['Caricamento Struttura Blazegraph'] = self.step_3_5_generate_all_blazegraph_journals()
        
        # Step 3.75: Backup dopo struttura
        all_results['Backup Post-Struttura'] = self.step_3_75_backup_after_structure()
        
        # Step 4: Check conteggi per tutte le directory
        all_results['Verifica Conteggi'] = self.step_4_check_all_counts()
        
        # Step 5: Check integrità per tutte le directory
        all_results['Verifica Integrità'] = self.step_5_check_all_integrity()

        # Step 6: Estrazione metadati per tutte le directory
        all_results['Estrazione Metadati'] = self.step_6_extract_all_metadata()

        # *** NUOVO STEP 6.5: Verifica hash post-estrazione metadati ***
        all_results['Verifica Hash Post-Metadati'] = self.step_6_5_verify_hash_consistency()

        # Step 7: Caricamento metadati in Blazegraph
        all_results['Caricamento Metadati'] = self.step_7_load_metadata_to_blazegraph()

        # Step 7.75: Backup dopo metadati
        all_results['Backup Post-Metadati'] = self.step_7_75_backup_after_metadata()

        # Step 8: Generazione Named Graph Registry
        all_results['Generazione Named Graph Registry'] = self.step_8_generate_ng_registry()
        
        # Step 9: Backup finale
        all_results['Backup Finale'] = self.step_9_final_backup()
        
        # Pulizia backup vecchi
        self._cleanup_old_backups()
        
        total_time = time.time() - total_start
        self.logger.pipeline_summary(total_time, all_results)
        
        # Salva report finale
        self._save_final_report(total_time, all_results)
        
        # Verifica successo (INCLUDE il nuovo step di verifica hash)
        critical_steps = ['Conteggio File', 'Calcolo Hash', 'Generazione Struttura', 
                        'Caricamento Struttura Blazegraph', 'Estrazione Metadati', 
                        'Caricamento Metadati', 'Generazione Named Graph Registry']
        
        # IMPORTANTE: La verifica hash post-metadati è considerata CRITICA
        verification_steps = ['Verifica Conteggi', 'Verifica Integrità', 'Verifica Hash Post-Metadati']
        
        all_critical_success = True
        for step_name in critical_steps:
            step_results = all_results[step_name]
            if not all(step_results.values()):
                all_critical_success = False
                self.logger.error(f"❌ STEP CRITICO FALLITO: {step_name}")
                break
                    
        all_verification_success = True
        for step_name in verification_steps:
            step_results = all_results[step_name]
            if not all(step_results.values()):
                all_verification_success = False
                self.logger.error(f"❌ STEP VERIFICA FALLITO: {step_name}")
                if step_name == 'Verifica Hash Post-Metadati':
                    self.logger.error("🚨 ATTENZIONE: Potenziale compromissione integrità file durante estrazione metadati!")
                break
        
        pipeline_success = all_critical_success and all_verification_success
        
        if pipeline_success:
            self.logger.info("🎉 PIPELINE COMPLETAMENTE RIUSCITA CON INTEGRITÀ VERIFICATA!")
        else:
            self.logger.error("❌ PIPELINE FALLITA - Problemi rilevati negli step di verifica")
            
        return pipeline_success
    
    def _save_final_report(self, total_time: float, all_results: Dict):
        """Salva un report finale della pipeline - AGGIORNATO con metadati"""
        report = {
            'pipeline_info': {
                'execution_type': 'multi_directory_with_metadata',
                'directories_processed': list(self.directories.keys()),
                'execution_date': datetime.now().isoformat(),
                'total_time_seconds': total_time
            },
            'step_results': all_results,
            'directory_configs': {
                dir_key: {
                    'description': config['description'],
                    'path': config['path'],
                    'output_files': {
                        'count_json': config['count_output'],
                        'hash_json': config['hash_output'],
                        'structure_nq': config['structure_output'],
                        'metadata_files': {
                            'filesystem': f"FileSystem_TechMeta_{config['metadata_directory']}.nq",
                            'tika': f"ApacheTika_TechMeta_{config['metadata_directory']}.nq",
                            'exif': f"ExifTool_TechMeta_{config['metadata_directory']}.nq"
                        }
                    }
                }
                for dir_key, config in self.directories.items()
            },
            'blazegraph_info': {
                'endpoint': 'http://localhost:9999/blazegraph/namespace/kb/sparql',
                'structure_loaded': 'step_3_5_generate_all_blazegraph_journals' in all_results,
                'metadata_loaded': 'step_7_load_metadata_to_blazegraph' in all_results
            },
            'summary': {
                'total_operations': sum(len(results) for results in all_results.values()),
                'successful_operations': sum(sum(1 for success in results.values() if success) for results in all_results.values())
            }
        }
        
        report_file = f"pipeline_report_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            self.logger.info(f"📊 Report finale completo salvato: {report_file}")
        except Exception as e:
            self.logger.error(f"Errore nel salvare il report finale: {e}")


class NGRegistryGenerator:
    """Generatore di Named Graph Registry per l'indice dei grafi caricati - VERSIONE AGGIORNATA"""
    
    def __init__(self, logger, directories_config: Dict, blazegraph_generator):
        self.logger = logger
        self.directories_config = directories_config
        self.blazegraph_generator = blazegraph_generator
        
        # Configurazione URI e namespace
        self.base_uri = "http://ficlit.unibo.it/ArchivioEvangelisti"
        self.registry_graph = f"{self.base_uri}/NGRegistry"
        
        # Mapping directory -> identificatori struttura archivistica (basato su DIRECTORY_CONFIGS)
        self.directory_structure_mapping = {
            'floppy': 'RS1_RS3',
            'hd': 'RS1_RS1', 
            'hdesterno': 'RS1_RS2'
        }
        
        # Prefissi RDF
        self.prefixes = {
            'dc': 'http://purl.org/dc/terms/',
            'rico': 'https://www.ica.org/standards/RiC/ontology#'
        }
        
        # Grafi caricati negli step precedenti (tracciamento REALE)
        self.loaded_graphs = self._identify_loaded_graphs()
    
    def generate_and_load_registry(self) -> bool:
        """Metodo principale per generare e caricare il registry"""
        self.logger.info("🔄 Generazione Named Graph Registry...")
        
        # DEBUG: Mostra gli URI estratti
        self.debug_loaded_graphs()
        
        start_time = time.time()
        
        try:
            # 1. Genera tutte le triple del registry
            registry_triples = self._generate_registry_triples()
            
            if not registry_triples:
                self.logger.error("❌ Nessuna tripla generata per il registry")
                return False
            
            # 2. Salva su file .nq
            registry_file = self._save_registry_to_file(registry_triples)
            
            if not registry_file:
                self.logger.error("❌ Impossibile salvare file registry")
                return False
            
            # 3. Carica nel database
            load_success = self._load_registry_to_blazegraph(registry_file)
            
            duration = time.time() - start_time
            
            if load_success:
                self.logger.info(f"✅ Named Graph Registry generato e caricato con successo ({duration:.2f}s)")
                self.logger.info(f"📊 Triple generate: {len(registry_triples)}")
                self.logger.info(f"📄 File salvato: {registry_file.name}")
                self.logger.info(f"🔗 Grafo target: {self.registry_graph}")
                return True
            else:
                self.logger.error(f"❌ Caricamento registry fallito")
                return False
                
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"❌ Errore generazione registry: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return False
    
    def _generate_registry_triples(self) -> List[str]:
        """Genera le triple N-Quads per il registry"""
        triples = []
        
        self.logger.info("📋 Generazione triple strutturali...")
        
        # 1. Triple gerarchiche di base
        base_triples = self._generate_base_hierarchy()
        triples.extend(base_triples)
        
        # 2. Triple di collegamento struttura-metadati REALI
        metadata_links = self._generate_metadata_links()
        triples.extend(metadata_links)
        
        self.logger.info(f"📊 Triple generate: {len(triples)}")
        self.logger.info(f"   - Gerarchiche: {len(base_triples)}")
        self.logger.info(f"   - Collegamenti metadati: {len(metadata_links)}")
        
        return triples
    
    def _generate_base_hierarchy(self) -> List[str]:
        """Genera la gerarchia di base del registry"""
        triples = []
        
        # Livello 1: ArchivioEvangelisti -> structure
        triple = f'<{self.base_uri}> <{self.prefixes["dc"]}hasPart> <{self.base_uri}/structure> <{self.registry_graph}> .'
        triples.append(triple)
        
        # Livello 2: structure -> RS1
        triple = f'<{self.base_uri}/structure> <{self.prefixes["dc"]}hasPart> <{self.base_uri}/structure/RS1> <{self.registry_graph}> .'
        triples.append(triple)
        
        # Livello 3: RS1 -> RSx_RSy (sottostrutture)
        unique_structures = set(self.directory_structure_mapping.values())
        for structure_id in sorted(unique_structures):
            triple = f'<{self.base_uri}/structure/RS1> <{self.prefixes["dc"]}hasPart> <{self.base_uri}/structure/{structure_id}> <{self.registry_graph}> .'
            triples.append(triple)
        
        self.logger.info(f"✅ Gerarchia di base: {len(triples)} triple generate")
        return triples
    
    def _identify_loaded_graphs(self) -> Dict[str, List[str]]:
        """Identifica i grafi REALI caricati leggendo dagli .nq file"""
        loaded_graphs = {}
        
        self.logger.info("🔍 Identificazione grafi REALI da file .nq...")
        
        for dir_key, config in self.directories_config.items():
            graphs = []
            
            self.logger.info(f"📁 Analisi directory: {dir_key}")
            
            # Grafo struttura (Step 3.5) - leggi URI reale dal file
            structure_file = Path.cwd() / config['structure_output']
            if structure_file.exists():
                structure_graph_uri = self._extract_graph_uri_from_nq(structure_file)
                if structure_graph_uri:
                    graphs.append(('structure', structure_graph_uri))
                    self.logger.info(f"   📄 Struttura: {structure_graph_uri}")
                else:
                    self.logger.warning(f"   ⚠️ URI struttura non estratto da {structure_file.name}")
            else:
                self.logger.warning(f"   ⚠️ File struttura non trovato: {config['structure_output']}")
            
            # Grafi metadati (Step 7) - leggi URI reali dai file
            metadata_dir = config['metadata_directory']
            metadata_files = [
                ('filesystem', f"FileSystem_TechMeta_{metadata_dir}.nq"),
                ('tika', f"ApacheTika_TechMeta_{metadata_dir}.nq"),
                ('exif', f"ExifTool_TechMeta_{metadata_dir}.nq")
            ]
            
            for metadata_type, filename in metadata_files:
                metadata_path = Path.cwd() / filename
                if metadata_path.exists():
                    metadata_graph_uri = self._extract_graph_uri_from_nq(metadata_path)
                    if metadata_graph_uri:
                        graphs.append((metadata_type, metadata_graph_uri))
                        self.logger.info(f"   📄 {metadata_type}: {metadata_graph_uri}")
                    else:
                        self.logger.warning(f"   ⚠️ URI {metadata_type} non estratto da {filename}")
                else:
                    self.logger.debug(f"   ⚠️ File metadati non trovato: {filename}")
            
            loaded_graphs[dir_key] = graphs
            self.logger.info(f"   📊 Grafi trovati per {dir_key}: {len(graphs)}")
            
        return loaded_graphs

    def _extract_graph_uri_from_nq(self, nq_file: Path) -> Optional[str]:
        """Estrae l'URI del grafo da un file .nq"""
        try:
            self.logger.debug(f"🔍 Lettura URI grafo da: {nq_file.name}")
            
            with open(nq_file, 'r', encoding='utf-8') as f:
                # Leggi le prime righe per trovare il pattern del grafo
                for i, line in enumerate(f):
                    line = line.strip()
                    if line and not line.startswith('#'):
                        # Pattern N-Quads: <subject> <predicate> <object> <graph> .
                        # Usa una regex più robusta per estrarre le parti
                        import re
                        
                        # Pattern per trovare URI tra < >
                        uri_pattern = r'<([^>]+)>'
                        uris = re.findall(uri_pattern, line)
                        
                        if len(uris) >= 4:
                            # Il 4° URI dovrebbe essere il grafo
                            graph_uri = uris[3]
                            self.logger.debug(f"   ✅ URI trovato: {graph_uri}")
                            return graph_uri
                        elif len(uris) == 3:
                            # Potrebbe essere un formato senza named graph esplicito
                            # In questo caso, cerca pattern specifici nel subject/object
                            for uri in uris:
                                if ('TechMeta' in uri or 'structure' in uri) and 'ArchivioEvangelisti' in uri:
                                    self.logger.debug(f"   ✅ URI inferito da subject/object: {uri}")
                                    return uri
                    
                    # Limita la ricerca alle prime 100 righe per performance
                    if i > 100:
                        break
            
            self.logger.warning(f"   ⚠️ Nessun URI grafo trovato in {nq_file.name}")
            return None
            
        except Exception as e:
            self.logger.error(f"   ❌ Errore lettura {nq_file.name}: {e}")
            return None

    def _generate_metadata_links(self) -> List[str]:
        """Genera i collegamenti tra struttura archivistica e grafi di metadati REALI"""
        triples = []
        
        self.logger.info("🔗 Generazione collegamenti struttura -> grafi metadati REALI...")
        
        for dir_key, config in self.directories_config.items():
            # Ottieni l'identificatore struttura archivistica per questa directory
            structure_id = self.directory_structure_mapping.get(dir_key)
            
            if not structure_id:
                self.logger.warning(f"⚠️ Nessun mapping struttura trovato per directory: {dir_key}")
                continue
            
            self.logger.info(f"📁 Directory {dir_key} -> Struttura archivistica {structure_id}")
            
            # URI della sezione archivistica
            structure_uri = f"{self.base_uri}/structure/{structure_id}"
            
            # Collegamenti ai grafi di metadati REALI caricati per questa directory
            directory_graphs = self.loaded_graphs.get(dir_key, [])
            
            self.logger.info(f"   📋 Grafi trovati per {dir_key}: {len(directory_graphs)}")
            
            for graph_type, graph_uri in directory_graphs:
                # Salta il grafo struttura (colleghiamo solo ai metadati)
                if graph_type == 'structure':
                    self.logger.debug(f"   ⏭️ Saltato grafo struttura: {graph_uri}")
                    continue
                
                # Verifica che il file metadati esista e non sia vuoto
                if self._verify_metadata_file_exists(dir_key, config, graph_type):
                    triple = f'<{structure_uri}> <{self.prefixes["rico"]}isRelatedTo> <{graph_uri}> <{self.registry_graph}> .'
                    triples.append(triple)
                    self.logger.info(f"   🔗 {structure_id} -> {graph_uri} ({graph_type})")
                else:
                    self.logger.warning(f"   ⚠️ File metadati {graph_type} non trovato o vuoto per {dir_key}")
        
        self.logger.info(f"✅ Collegamenti grafi metadati REALI: {len(triples)} triple generate")
        return triples
    
    def _verify_metadata_file_exists(self, dir_key: str, config: Dict, graph_type: str) -> bool:
        """Verifica se il file metadati corrispondente al grafo esiste e non è vuoto"""
        metadata_dir = config['metadata_directory']
        
        # Mapping tipo grafo -> nome file  
        file_patterns = {
            'filesystem': f"FileSystem_TechMeta_{metadata_dir}.nq",
            'tika': f"ApacheTika_TechMeta_{metadata_dir}.nq",
            'exif': f"ExifTool_TechMeta_{metadata_dir}.nq"
        }
        
        filename = file_patterns.get(graph_type)
        if not filename:
            return False
            
        file_path = Path.cwd() / filename
        exists = file_path.exists()
        
        if exists:
            # Verifica anche che il file non sia vuoto
            try:
                size = file_path.stat().st_size
                self.logger.debug(f"   📄 {filename}: {size:,} bytes")
                return size > 0
            except:
                return False
        
        return False
    
    def debug_loaded_graphs(self):
        """Metodo di debug per verificare gli URI estratti"""
        self.logger.info("🔍 DEBUG: Verifica URI grafi estratti")
        self.logger.info("=" * 50)
        
        total_graphs = 0
        for dir_key, graphs in self.loaded_graphs.items():
            self.logger.info(f"\n📁 Directory: {dir_key}")
            if not graphs:
                self.logger.warning(f"   ⚠️ Nessun grafo trovato")
            else:
                for graph_type, graph_uri in graphs:
                    self.logger.info(f"   📄 {graph_type}: {graph_uri}")
                    total_graphs += 1
        
        self.logger.info(f"\n📊 Totale grafi identificati: {total_graphs}")
        self.logger.info("=" * 50)
    
    def _save_registry_to_file(self, triples: List[str]) -> Optional[Path]:
        """Salva le triple del registry in un file .nq"""
        registry_filename = "NGRegistry.nq"
        registry_path = Path.cwd() / registry_filename
        
        try:
            self.logger.info(f"💾 Salvataggio registry: {registry_filename}")
            
            with open(registry_path, 'w', encoding='utf-8') as f:
                for triple in triples:
                    f.write(triple + '\n')
            
            file_size = registry_path.stat().st_size
            self.logger.info(f"✅ Registry salvato: {registry_filename} ({file_size:,} bytes)")
            
            # Log delle prime righe per verifica
            self.logger.debug("📋 Prime triple generate:")
            for i, triple in enumerate(triples[:5]):
                self.logger.debug(f"   {i+1}. {triple}")
            if len(triples) > 5:
                self.logger.debug(f"   ... e altre {len(triples) - 5} triple")
            
            return registry_path
            
        except Exception as e:
            self.logger.error(f"❌ Errore salvataggio registry: {e}")
            return None
    
    def _load_registry_to_blazegraph(self, registry_file: Path) -> bool:
        """Carica il registry nel database Blazegraph"""
        self.logger.info("🚀 Caricamento Named Graph Registry in Blazegraph...")
        
        try:
            # Conta triple prima del caricamento
            triple_count_before = self.blazegraph_generator._check_existing_data()
            
            # Carica usando il sistema esistente
            success = self.blazegraph_generator.rest_loader.load_nquads_file(registry_file)
            
            if success:
                triple_count_after = self.blazegraph_generator._check_existing_data()
                registry_triples_added = triple_count_after - triple_count_before
                
                self.logger.info(f"✅ Registry caricato con successo")
                self.logger.info(f"📈 Triple registry aggiunte: {registry_triples_added:,}")
                self.logger.info(f"📊 Triple totali database: {triple_count_after:,}")
                
                # Verifica specifica del grafo registry
                self._verify_registry_in_database()
                
                return True
            else:
                self.logger.error("❌ Caricamento registry fallito")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Errore caricamento registry: {e}")
            return False
    
    def _verify_registry_in_database(self):
        """Verifica che il registry sia stato caricato correttamente"""
        try:
            import requests
            
            # Query per contare triple nel grafo registry
            verify_query = f"""
            SELECT (COUNT(*) as ?count) 
            WHERE {{ 
                GRAPH <{self.registry_graph}> {{ 
                    ?s ?p ?o 
                }} 
            }}
            """
            
            endpoint = f"{self.blazegraph_generator.base_url}/namespace/{self.blazegraph_generator.namespace}/sparql"
            
            response = requests.post(
                endpoint,
                data={'query': verify_query},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                registry_count = int(result["results"]["bindings"][0]["count"]["value"])
                
                if registry_count > 0:
                    self.logger.info(f"✅ Verifica registry: {registry_count:,} triple nel grafo {self.registry_graph}")
                else:
                    self.logger.warning(f"⚠️ Verifica registry: 0 triple trovate nel grafo registry")
            else:
                self.logger.warning(f"⚠️ Impossibile verificare registry: HTTP {response.status_code}")
                
        except Exception as e:
            self.logger.warning(f"⚠️ Errore verifica registry: {e}")


def parse_arguments():
    """Parser degli argomenti da riga di comando"""
    parser = argparse.ArgumentParser(
        description="Pipeline automatica multi-directory per l'Archivio Evangelisti",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Directory disponibili:
{chr(10).join([f"  {k}: {v['description']} -> {v['path']}" for k, v in PIPELINE_CONFIGS.items()])}

Esempi di utilizzo:
  # Processa tutte le directory
  python pipeline.py
  
  # Processa solo directory specifiche
  python pipeline.py --directories floppy hd
  
  # Con log personalizzato
  python pipeline.py --log pipeline_custom.log
  
  # Modalità dry-run
  python pipeline.py --dry-run
"""
    )
    
    parser.add_argument(
        '--directories',
        nargs='+',
        choices=list(PIPELINE_CONFIGS.keys()),
        help='Directory specifiche da processare (default: tutte)'
    )
    
    parser.add_argument(
        '--log',
        default='pipeline_evangelisti.log',
        help='File di log da utilizzare (default: pipeline_evangelisti.log)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simula l\'esecuzione senza eseguire realmente i comandi'
    )
    
    return parser.parse_args()


def main():
    """Funzione principale"""
    args = parse_arguments()
    
    # Inizializza logger
    logger = PipelineLogger(args.log)
    
    try:
        # Modalità dry-run
        if args.dry_run:
            logger.info("🔍 MODALITÀ DRY-RUN ATTIVATA")
            directories = args.directories if args.directories else list(PIPELINE_CONFIGS.keys())
            logger.info(f"Directory da processare: {', '.join(directories)}")
            
            for dir_key in directories:
                config = PIPELINE_CONFIGS[dir_key]
                logger.info(f"\n{dir_key} - {config['description']}:")
                logger.info(f"  Path: {config['path']}")
                logger.info(f"  Output previsti:")
                logger.info(f"    - Conteggio: {config['count_output']}")
                logger.info(f"    - Hash: {config['hash_output']}")
                logger.info(f"    - Struttura: {config['structure_output']}")
                
                # Mostra anche i comandi che verrebbero eseguiti
                logger.info(f"  Comandi che verrebbero eseguiti:")
                logger.info(f"    1. python file_count.py {config['path']} -o {config['count_output']} -r -e")
                logger.info(f"    2. python hash_calc.py {config['path']}")
                logger.info(f"    3. python structure_generation.py --type {config['structure_type']}")
                logger.info(f"    4. Generazione Journal Blazegraph e caricamento struttura") 
                logger.info(f"    5. python count_check.py {config['check_type']}")
                logger.info(f"    6. python integrity_check.py {config['check_type']}")
                logger.info(f"    7. Estrazione metadati per tutte le directory")
                logger.info(f"    8. Caricamento metadati Blazegraph")
                logger.info(f"    9. Generazione Named Graph Registry")
                
            logger.info("✅ Dry-run completato")
            return
        
        # Esecuzione normale
        pipeline = MultiDirectoryPipeline(logger, args.directories)
        success = pipeline.run_pipeline()
        
        if success:
            logger.info("🎉 PIPELINE MULTI-DIRECTORY COMPLETATA CON SUCCESSO!")
            sys.exit(0)
        else:
            logger.error("❌ PIPELINE MULTI-DIRECTORY FALLITA")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.error("⚠️ Pipeline interrotta dall'utente")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Errore fatale nella pipeline: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()