# === DEDUPLICAZIONE E COUNTER - VERSIONE UNIFICATA CON URI STRUTTURATI, PERSISTENZA JSON E DROID ===
import os
import json
import stat
import subprocess
import sys
import duration
import platform
import time
import re 
import argparse
import csv
import tempfile
from datetime import datetime
from functools import wraps
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from rdflib import Dataset, URIRef, Literal, Namespace, Graph
from rdflib.namespace import RDF, RDFS
import locale
import subprocess
import requests
from rdflib.namespace import XSD
import glob
import shutil
import signal
from pathlib import Path


# === IMPORT CONFIGURAZIONE CENTRALIZZATA ===
try:
    from config_loader import load_config, ConfigError
    USE_CENTRALIZED_CONFIG = True
    print("✅ Configurazione centralizzata caricata - evangelisti_metadata_extraction.py:32")
except ImportError:
    USE_CENTRALIZED_CONFIG = False
    ConfigError = Exception  # Fallback
    load_config = None  # Fallback
    print("⚠️ Configurazione centralizzata non disponibile, uso configurazione locale - evangelisti_metadata_extraction.py:37")


class DROIDProcessManager:
    """Gestione sicura dei processi DROID basata sulla documentazione ufficiale"""
    
    def __init__(self, droid_jar_path):
        self.droid_jar_path = droid_jar_path
        self.active_processes = set()
    
    def create_isolated_workspace(self):
        """Crea workspace isolato per evitare conflitti sui profili"""
        # Usa directory temporanea dedicata
        workspace = tempfile.mkdtemp(prefix='droid_workspace_')
        
        return {
            'workspace_dir': Path(workspace),
            'profile_path': Path(workspace) / 'analysis.droid',
            'csv_output': Path(workspace) / 'results.csv'
        }
    
    def execute_droid_scan_safe(self, file_paths, timeout=120):
        """Esecuzione DROID sicura secondo best practices documentazione"""
        
        workspace = self.create_isolated_workspace()
        
        try:
            # 1. CREA NUOVO PROFILO (evita conflitti)
            profile_path = workspace['profile_path']
            
            # Comando per creare profilo vuoto
            create_cmd = [
                'java', '-Xmx1G', 
                '-jar', self.droid_jar_path,
                '-p', str(profile_path),
                '-q'  # quiet mode
            ]
            
            # Aggiungi file da analizzare (max 25 per pipeline)
            valid_files = [f for f in file_paths[:25] if os.path.exists(f)]
            
            for file_path in valid_files:
                create_cmd.extend(['-a', file_path])
            
            print(f"[DROID] Creazione profilo per {len(valid_files)} file")
            
            # 2. ESEGUI SCANSIONE CON PROCESSO TRACCIATO
            process = subprocess.Popen(
                create_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid  # Crea process group per cleanup
            )
            
            self.active_processes.add(process.pid)
            
            try:
                stdout, stderr = process.communicate(timeout=timeout)
                returncode = process.returncode
            except subprocess.TimeoutExpired:
                print(f"[DROID] Timeout - terminazione forzata processo {process.pid}")
                self._force_kill_process(process)
                return {path: ({}, 0) for path in file_paths}
            finally:
                self.active_processes.discard(process.pid)
            
            if returncode != 0:
                print(f"[DROID] Scansione fallita: {returncode}")
                if stderr:
                    print(f"[DROID] Stderr: {stderr.decode('utf-8', errors='ignore')[:200]}")
                return {path: ({}, 0) for path in file_paths}
            
            # 3. ATTENDI CHE DROID COMPLETI I/O SUI FILE
            time.sleep(1.0)
            
            # 4. EXPORT RISULTATI
            csv_path = workspace['csv_output']
            export_success = self._export_to_csv(profile_path, csv_path)
            
            if not export_success:
                return {path: ({}, 0) for path in file_paths}
            
            # 5. PARSING RISULTATI
            return self._parse_csv_results(csv_path, file_paths)
            
        finally:
            # 6. CLEANUP WORKSPACE COMPLETO
            self._cleanup_workspace(workspace)
    
    def _export_to_csv(self, profile_path, csv_path):
        """Export sicuro secondo specifiche DROID"""
        
        # Verifica che il profilo esista e sia valido
        if not profile_path.exists():
            print(f"[DROID] Profilo non trovato: {profile_path}")
            return False
        
        export_cmd = [
            'java', '-Xmx1G',
            '-jar', self.droid_jar_path,
            '-p', str(profile_path),
            '-e', str(csv_path),
            # Campi specifici dalla documentazione DROID
            '-co', 'ID,PARENT_ID,URI,FILE_PATH,NAME,METHOD,STATUS,SIZE,TYPE,EXT,LAST_MODIFIED,EXTENSION_MISMATCH,HASH,FORMAT_COUNT,PUID,MIME_TYPE,FORMAT_NAME,FORMAT_VERSION'
        ]
        
        try:
            result = subprocess.run(
                export_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
                check=False
            )
            
            if result.returncode == 0:
                # Verifica che il CSV sia stato creato e non sia vuoto
                if csv_path.exists() and csv_path.stat().st_size > 0:
                    print(f"[DROID] Export CSV completato: {csv_path.stat().st_size} bytes")
                    return True
                else:
                    print(f"[DROID] CSV vuoto o non creato")
                    return False
            else:
                print(f"[DROID] Export fallito: {result.returncode}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"[DROID] Timeout durante export CSV")
            return False
        except Exception as e:
            print(f"[DROID] Errore export: {e}")
            return False
    
    def _parse_csv_results(self, csv_path, original_file_paths):
        """Parsing CSV con gestione errori robusta"""
        
        results = {}
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                # Skip BOM se presente
                csvfile.seek(0)
                sample = csvfile.read(3)
                if sample.startswith('\ufeff'):
                    csvfile.seek(3)
                else:
                    csvfile.seek(0)
                
                reader = csv.DictReader(csvfile)
                
                # Mappa path -> metadati
                droid_results = {}
                
                for row_num, row in enumerate(reader, 1):
                    try:
                        file_path = row.get('FILE_PATH', '').strip()
                        
                        if not file_path:
                            continue
                        
                        # Normalizza path
                        normalized_path = os.path.abspath(file_path)
                        
                        # Estrai solo metadati significativi (escludi campi tecnici DROID)
                        metadata = {}
                        significant_fields = ['PUID', 'MIME_TYPE', 'FORMAT_NAME', 'FORMAT_VERSION', 'SIZE', 'LAST_MODIFIED']
                        
                        for field in significant_fields:
                            value = row.get(field, '').strip()
                            if value and value != 'null' and value != '':
                                metadata[field] = value
                        
                        # Aggiungi solo se ci sono metadati utili
                        if metadata:
                            droid_results[normalized_path] = metadata
                        
                    except Exception as row_error:
                        print(f"[DROID] Errore parsing riga {row_num}: {row_error}")
                        continue
                
                # Mappa risultati ai file originali
                for original_path in original_file_paths:
                    normalized = os.path.abspath(original_path)
                    if normalized in droid_results:
                        results[original_path] = (droid_results[normalized], 50.0)
                    else:
                        results[original_path] = ({}, 0.0)
                
                print(f"[DROID] Parsing completato: {len(droid_results)} file con metadati")
                
        except Exception as e:
            print(f"[DROID] Errore parsing CSV: {e}")
            # Restituisci risultati vuoti per tutti i file
            for path in original_file_paths:
                results[path] = ({}, 0.0)
        
        return results
    
    def _force_kill_process(self, process):
        """Terminazione forzata processo DROID"""
        try:
            # Termina il process group intero
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            time.sleep(2)
            
            # Force kill se ancora vivo
            if process.poll() is None:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                
        except (OSError, ProcessLookupError):
            pass  # Processo già morto
    
    def _cleanup_workspace(self, workspace):
        """Cleanup completo workspace con retry per file locked"""
        workspace_dir = workspace['workspace_dir']
        
        # Attendi che DROID rilasci tutti i lock
        time.sleep(2)
        
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                if workspace_dir.exists():
                    import shutil
                    shutil.rmtree(workspace_dir, ignore_errors=True)
                    
                    # Verifica rimozione
                    if not workspace_dir.exists():
                        print(f"[DROID] Workspace pulito: {workspace_dir}")
                        return True
                
            except Exception as e:
                if attempt < max_attempts - 1:
                    print(f"[DROID] Tentativo cleanup {attempt + 1}/{max_attempts} fallito: {e}")
                    time.sleep(1)
                else:
                    print(f"[DROID] Cleanup finale fallito: {e}")
                    return False
        
        return True

# Integrazione nel codice esistente
def extract_droid_metadata_batch_fixed(file_paths):
    """Versione corretta basata su documentazione DROID"""
    if not file_paths:
        return {}
    
    manager = DROIDProcessManager(DROID_JAR_PATH)
    
    try:
        return manager.execute_droid_scan_safe(file_paths, timeout=120)
    except Exception as e:
        print(f"[DROID] Errore fatale: {e}")
        return {path: ({}, 0) for path in file_paths}


def get_directory_configs():
    """Ottiene le configurazioni delle directory"""
    if USE_CENTRALIZED_CONFIG and load_config is not None:
        try:
            config = load_config()
            
            # Crea configurazione nel formato richiesto da questo script
            directory_configs = {}
            for dir_key, dir_config in config.get_directories().items():
                # ✅ FIX: NORMALIZZA SEMPRE IL SUFFIX IN LOWERCASE
                suffix = dir_config['structure']['output_suffix'].lower()
                
                directory_configs[dir_key] = {
                    'root_path': dir_config['path'],
                    'input_nquads': dir_config['files']['structure_output'],
                    'suffix': suffix  # SEMPRE lowercase
                }
            
            print(f"✅ Configurazioni caricate dalla configurazione centralizzata: {list(directory_configs.keys())} - evangelisti_metadata_extraction.py:59")
            return directory_configs
            
        except ConfigError as e:
            print(f"⚠️ Errore configurazione centralizzata: {e} - evangelisti_metadata_extraction.py:63")
            print("🔄 Fallback alla configurazione locale - evangelisti_metadata_extraction.py:64")
        except Exception as e:
            print(f"⚠️ Errore generico configurazione centralizzata: {e} - evangelisti_metadata_extraction.py:66")
            print("🔄 Fallback alla configurazione locale - evangelisti_metadata_extraction.py:67")
    
    # ✅ FIX: Configurazione locale con suffix SEMPRE lowercase
    print("ℹ️ Usando configurazione locale fallback - evangelisti_metadata_extraction.py:70")
    return {
        'floppy': {
            'root_path': "/media/sdb1/evangelisti/data/FloppyDisks/",
            'input_nquads': "structure_floppy.nq",
            'suffix': "floppy"  # SEMPRE lowercase
        },
        'hd': {
            'root_path': "/media/sdb1/evangelisti/data/HardDiskValerio/",
            'input_nquads': "structure_hd.nq", 
            'suffix': "hd"  # SEMPRE lowercase
        },
        'hdesterno': {
            'root_path': "/media/sdb1/evangelisti/data/HDEsternoEvangelisti/",
            'input_nquads': "structure_hdesterno.nq",
            'suffix': "hdesterno"  # SEMPRE lowercase
        }
    }

# === CONFIGURAZIONI PER DIRECTORY ===
DIRECTORY_CONFIGS = get_directory_configs()

# === CONFIGURAZIONE JAR TIKA E DROID ===
TIKA_JAR_PATH = "tika-server-standard-3.2.1.jar"  # SOSTITUISCI CON IL TUO PERCORSO
DROID_JAR_PATH = "droid-command-line-6.8.1.jar" 
DROID_SCRIPT_PATH = "./droid.s -d"  # O percorso assoluto alla tua directory DROID  # 🆕 NUOVO: Path al jar DROID



def get_tika_version(tika_url="http://localhost:9998"):
    """Ottiene la versione di Apache Tika dal server REST"""
    try:
        # Prova endpoint root del server
        response = requests.get(tika_url, timeout=5)
        if response.status_code == 200:
            content = response.text
            
            # Cerca pattern di versione nel testo
            import re
            version_patterns = [
                r'Apache Tika (\d+\.\d+(?:\.\d+)?)',
                r'Tika (\d+\.\d+(?:\.\d+)?)',
                r'tika-server[^\d]*(\d+\.\d+(?:\.\d+)?)'
            ]
            
            for pattern in version_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    return match.group(1)
                    
    except Exception as e:
        print(f"[TIKAVERSION] Errore nel recuperare versione: {e} - evangelisti_metadata_extraction.py:121")
    
    return None

def setup_tika_server(url="http://localhost:9998/tika"):
    """Verifica che il Tika Server sia attivo, altrimenti lo avvia automaticamente"""
    global current_tika_version
    
    try:
        response = requests.get(url, headers={"Accept": "text/plain"}, timeout=5)
        if response.status_code == 200:
            print(f"[TIKASETUP] ✅ Tika server già attivo su {url} - evangelisti_metadata_extraction.py:132")
            
            # Inizializza la versione come prima
            if current_tika_version is None:
                base_url = url.replace('/tika', '') if url.endswith('/tika') else url
                print(f"[TIKAVERSION] Recuperando versione da {base_url}... - evangelisti_metadata_extraction.py:137")
                
                version = get_tika_version(base_url)
                if version:
                    current_tika_version = version
                    print(f"[TIKAVERSION] Versione rilevata: {version} - evangelisti_metadata_extraction.py:142")
                else:
                    print(f"[TIKAVERSION] Impossibile rilevare versione - evangelisti_metadata_extraction.py:144")
            else:
                print(f"[TIKAVERSION] Versione già impostata: {current_tika_version} - evangelisti_metadata_extraction.py:146")
            
            return True
        else:
            print(f"[TIKASETUP] ⚠️ Tika server risponde con codice: {response.status_code} - evangelisti_metadata_extraction.py:150")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"[TIKASETUP] ⚠️ Tika server non raggiungibile: {e} - evangelisti_metadata_extraction.py:154")
        print(f"[TIKASETUP] 🔄 Tentativo di avvio automatico... - evangelisti_metadata_extraction.py:155")
        
        # Tenta di avviare il server automaticamente
        if start_tika_server():
            # Riprova la connessione dopo l'avvio
            return setup_tika_server(url)
        else:
            print(f"[TIKASETUP] ❌ Impossibile avviare Tika Server automaticamente - evangelisti_metadata_extraction.py:162")
            return False
    
def start_tika_server():
    """Avvia automaticamente il Tika Server se non è in esecuzione"""
    import subprocess
    import time
    
    if not os.path.exists(TIKA_JAR_PATH):
        print(f"[TIKASTART] ❌ TIKA jar non trovato: {TIKA_JAR_PATH} - evangelisti_metadata_extraction.py:171")
        return False
    
    print(f"[TIKASTART] 🚀 Avvio Tika Server... - evangelisti_metadata_extraction.py:174")
    
    try:
        # Avvia il processo in background
        process = subprocess.Popen(
            ['java', '-server', '-jar', TIKA_JAR_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True  # Detach dal processo principale
        )
        
        print(f"[TIKASTART] Processo avviato con PID: {process.pid} - evangelisti_metadata_extraction.py:185")
        print(f"[TIKASTART] ⏳ Attesa avvio server (30 secondi)... - evangelisti_metadata_extraction.py:186")
        
        # Attendi che il server sia pronto
        for i in range(30):  # 30 tentativi, 1 secondo ciascuno
            time.sleep(1)
            try:
                response = requests.get("http://localhost:9998", timeout=2)
                if response.status_code == 200:
                    print(f"[TIKASTART] ✅ Tika Server avviato con successo dopo {i+1} secondi - evangelisti_metadata_extraction.py:194")
                    return True
            except requests.exceptions.RequestException:
                pass  # Server non ancora pronto
            
            print(f"[TIKASTART] Tentativo {i+1}/30... - evangelisti_metadata_extraction.py:199", end='\r')
        
        print(f"\n[TIKASTART] ⚠️ Timeout: server non risponde dopo 30 secondi - evangelisti_metadata_extraction.py:201")
        return False
        
    except Exception as e:
        print(f"[TIKASTART] ❌ Errore nell'avvio: {e} - evangelisti_metadata_extraction.py:205")
        return False
    
    
def setup_droid():
    """Verifica che DROID jar sia disponibile e funzionante - CON SETUP AUTOMATICO"""
    global current_droid_version
    
    if not os.path.exists(DROID_JAR_PATH):
        print(f"[DROIDSETUP] ❌ DROID jar non trovato: {DROID_JAR_PATH} - evangelisti_metadata_extraction.py:214")
        return False
    
    try:
        # 1. Test base comando DROID
        print(f"[DROIDSETUP] 🔍 Verificando DROID jar... - evangelisti_metadata_extraction.py:219")
        result = subprocess.run(
            ['java', '-jar', DROID_JAR_PATH, '-h'], #aggiungi print
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10
        )
        #aggiungi print -- fine del processo in cui ho invocato DROID -- qual è il contenuto della componente error (se ce ne sono)
        #alla fine dell'esecuzione: 
        
        if result.returncode != 0:
            print(f"[DROIDSETUP] ❌ DROID jar non eseguibile: {result.returncode} - evangelisti_metadata_extraction.py:228")
            return False
        
        print(f"[DROIDSETUP] ✅ DROID jar eseguibile - evangelisti_metadata_extraction.py:231")
        
        # 2. Verifica signature files
        print(f"[DROIDSETUP] 🔍 Verificando signature files... - evangelisti_metadata_extraction.py:234")
        sig_result = subprocess.run(
            ['java', '-jar', DROID_JAR_PATH, '-x'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10
        )
        
        if sig_result.returncode == 0:
            output = sig_result.stdout.decode('utf-8', errors='ignore')
            print(f"[DROIDSETUP] Signature files output: {output.strip()} - evangelisti_metadata_extraction.py:244")
            
            # Controlla se ci sono signature files validi
            if 'DROID_SignatureFile' in output and 'container-signature' in output:
                print(f"[DROIDSETUP] ✅ Signature files trovati e validi - evangelisti_metadata_extraction.py:248")
                
                # Estrai versione DROID dalla linea di help
                version_patterns = [
                    r'DROID (\d+\.\d+(?:\.\d+)?)',
                    r'version (\d+\.\d+(?:\.\d+)?)',
                    r'v(\d+\.\d+(?:\.\d+)?)'
                ]
                
                help_output = result.stdout.decode('utf-8', errors='ignore')
                for pattern in version_patterns:
                    match = re.search(pattern, help_output, re.IGNORECASE)
                    if match:
                        current_droid_version = match.group(1)
                        print(f"[DROIDVERSION] Versione rilevata: {current_droid_version} - evangelisti_metadata_extraction.py:262")
                        break
                else:
                    current_droid_version = "6.8.1"  # Fallback dalla filename
                    print(f"[DROIDVERSION] Usando versione fallback: {current_droid_version} - evangelisti_metadata_extraction.py:266")
                
                return True
            
            else:
                print(f"[DROIDSETUP] ⚠️ Signature files mancanti o non validi - evangelisti_metadata_extraction.py:271")
                print(f"[DROIDSETUP] 🔄 Tentativo download automatico... - evangelisti_metadata_extraction.py:272")
                
                # 3. Download automatico signature files
                if download_droid_signatures():
                    # Verifica di nuovo dopo download
                    return verify_droid_signatures()
                else:
                    return False
        else:
            print(f"[DROIDSETUP] ❌ Errore verifica signature: {sig_result.returncode} - evangelisti_metadata_extraction.py:281")
            print(f"[DROIDSETUP] STDERR: {sig_result.stderr.decode('utf8', errors='ignore')[:200]} - evangelisti_metadata_extraction.py:282")
            
            # Prova download anche se c'è errore
            print(f"[DROIDSETUP] 🔄 Tentativo download automatico signature files... - evangelisti_metadata_extraction.py:285")
            if download_droid_signatures():
                return verify_droid_signatures()
            else:
                return False
            
    except subprocess.TimeoutExpired:
        print(f"[DROIDSETUP] ❌ Timeout nell'esecuzione DROID - evangelisti_metadata_extraction.py:292")
        return False
    except Exception as e:
        print(f"[DROIDSETUP] ❌ Errore setup DROID: {e} - evangelisti_metadata_extraction.py:295")
        return False

def download_droid_signatures():
    """Scarica automaticamente i signature files DROID"""
    try:
        print(f"[DROIDDOWNLOAD] 📥 Checking for signature updates... - evangelisti_metadata_extraction.py:301")
        
        # Comando per verificare aggiornamenti
        check_result = subprocess.run(
            ['java', '-jar', DROID_JAR_PATH, '-c'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30
        )
        
        if check_result.returncode == 0:
            check_output = check_result.stdout.decode('utf-8', errors='ignore')
            print(f"[DROIDDOWNLOAD] Check output: {check_output.strip()} - evangelisti_metadata_extraction.py:313")
            
            # Scarica aggiornamenti
            print(f"[DROIDDOWNLOAD] 📥 Downloading signature files... - evangelisti_metadata_extraction.py:316")
            download_result = subprocess.run(
                ['java', '-jar', DROID_JAR_PATH, '-d'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=60  # Più tempo per download
            )
            
            if download_result.returncode == 0:
                download_output = download_result.stdout.decode('utf-8', errors='ignore')
                print(f"[DROIDDOWNLOAD] ✅ Download completato: {download_output.strip()} - evangelisti_metadata_extraction.py:326")
                return True
            else:
                print(f"[DROIDDOWNLOAD] ❌ Download fallito: {download_result.returncode} - evangelisti_metadata_extraction.py:329")
                print(f"[DROIDDOWNLOAD] STDERR: {download_result.stderr.decode('utf8', errors='ignore')[:200]} - evangelisti_metadata_extraction.py:330")
                return False
        else:
            print(f"[DROIDDOWNLOAD] ❌ Check aggiornamenti fallito: {check_result.returncode} - evangelisti_metadata_extraction.py:333")
            print(f"[DROIDDOWNLOAD] STDERR: {check_result.stderr.decode('utf8', errors='ignore')[:200]} - evangelisti_metadata_extraction.py:334")
            
            # Prova download comunque
            print(f"[DROIDDOWNLOAD] 🔄 Tentativo download diretto... - evangelisti_metadata_extraction.py:337")
            download_result = subprocess.run(
                ['java', '-jar', DROID_JAR_PATH, '-d'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=60
            )
            
            if download_result.returncode == 0:
                print(f"[DROIDDOWNLOAD] ✅ Download diretto riuscito - evangelisti_metadata_extraction.py:346")
                return True
            else:
                print(f"[DROIDDOWNLOAD] ❌ Anche download diretto fallito - evangelisti_metadata_extraction.py:349")
                return False
            
    except subprocess.TimeoutExpired:
        print(f"[DROIDDOWNLOAD] ❌ Timeout nel download (>60s) - evangelisti_metadata_extraction.py:353")
        return False
    except Exception as e:
        print(f"[DROIDDOWNLOAD] ❌ Errore nel download: {e} - evangelisti_metadata_extraction.py:356")
        return False

def verify_droid_signatures():
    """Verifica che i signature files siano stati scaricati correttamente"""
    try:
        print(f"[DROIDVERIFY] 🔍 Verifica finale signature files... - evangelisti_metadata_extraction.py:362")
        
        result = subprocess.run(
            ['java', '-jar', DROID_JAR_PATH, '-x'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10
        )
        
        if result.returncode == 0:
            output = result.stdout.decode('utf-8', errors='ignore')
            print(f"[DROIDVERIFY] Signature files verificati: {output.strip()} - evangelisti_metadata_extraction.py:373")
            
            if 'DROID_SignatureFile' in output and 'container-signature' in output:
                print(f"[DROIDVERIFY] ✅ Signature files validi dopo download - evangelisti_metadata_extraction.py:376")
                
                # Test su file dummy per conferma funzionalità
                return test_droid_functionality()
            else:
                print(f"[DROIDVERIFY] ❌ Signature files ancora non validi - evangelisti_metadata_extraction.py:381")
                return False
        else:
            print(f"[DROIDVERIFY] ❌ Errore nella verifica finale: {result.returncode} - evangelisti_metadata_extraction.py:384")
            return False
            
    except Exception as e:
        print(f"[DROIDVERIFY] ❌ Errore verifica finale: {e} - evangelisti_metadata_extraction.py:388")
        return False

def test_droid_functionality():
    """Test finale di funzionalità DROID su file temporaneo"""
    try:
        print(f"[DROIDTEST] 🧪 Test funzionalità DROID... - evangelisti_metadata_extraction.py:394")
        
        # Crea file temporaneo per test
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as test_file:
            test_file.write("Hello DROID Test")
            test_file_path = test_file.name
        
        try:
            # Test identificazione singolo file
            test_result = subprocess.run(
                ['java', '-jar', DROID_JAR_PATH, '-a', test_file_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=15
            )
            
            if test_result.returncode == 0:
                test_output = test_result.stdout.decode('utf-8', errors='ignore')
                print(f"[DROIDTEST] ✅ Test identificazione riuscito - evangelisti_metadata_extraction.py:412")
                print(f"[DROIDTEST] Output: {test_output.strip()[:100]}... - evangelisti_metadata_extraction.py:413")
                return True
            else:
                print(f"[DROIDTEST] ❌ Test identificazione fallito: {test_result.returncode} - evangelisti_metadata_extraction.py:416")
                return False
                
        finally:
            # Cleanup file test
            try:
                os.unlink(test_file_path)
            except:
                pass
                
    except Exception as e:
        print(f"[DROIDTEST] ❌ Errore nel test: {e} - evangelisti_metadata_extraction.py:427")
        return False
    
def parse_arguments():
    """Parse command line arguments to select directory configuration"""
    parser = argparse.ArgumentParser(description='Evangelisti Metadata Extractor - Unified Version with DROID')
    parser.add_argument('directory', 
                       choices=list(DIRECTORY_CONFIGS.keys()),
                       help='Directory to process (floppy, hd, hdesterno)')
    parser.add_argument('--chunk-size', type=int, default=100,
                       help='Chunk size for processing (default: 100)')
    parser.add_argument('--persistence-interval', type=int, default=100,
                       help='Persistence interval for incremental serialization (default: 100)')
    parser.add_argument('--disable-droid', action='store_true', default=False,
                       help='Disable DROID metadata extraction (default: False - DROID enabled)')
    
    return parser.parse_args()

def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        elapsed_time = round(end - start, 2)  # Arrotonda subito
        func_name = str(func.__name__)  # Forza a stringa
        print(f"[TIMING] {func_name}: {elapsed_time}s - evangelisti_metadata_extraction.py:453")
        return result
    return wrapper


# === INIZIALIZZAZIONE CONFIGURAZIONE GLOBALE ===
def initialize_config(directory_key):
    """Initialize global configuration based on selected directory - FIXED CASE"""
    global ROOT_PATH, INPUT_NQUADS, OUTPUT_NQUADS_FS, OUTPUT_NQUADS_TIKA, OUTPUT_NQUADS_EXIF, OUTPUT_NQUADS_DROID  # 🆕 DROID
    global fs_graph_uri, tika_graph_uri, exif_graph_uri, droid_graph_uri  # 🆕 DROID
    
    print(f"[CONFIG] Inizializzazione configurazione per: {directory_key} - evangelisti_metadata_extraction.py:464")
    print(f"[CONFIG] Directory disponibili: {list(DIRECTORY_CONFIGS.keys())} - evangelisti_metadata_extraction.py:465")
    
    if directory_key not in DIRECTORY_CONFIGS:
        print(f"[ERROR] Directory '{directory_key}' non trovata in DIRECTORY_CONFIGS - evangelisti_metadata_extraction.py:468")
        print(f"[ERROR] Directory disponibili: {list(DIRECTORY_CONFIGS.keys())} - evangelisti_metadata_extraction.py:469")
        sys.exit(1)
    
    config = DIRECTORY_CONFIGS[directory_key]
    print(f"[CONFIG] Configurazione per {directory_key}: {config} - evangelisti_metadata_extraction.py:473")
    
    # Verifica che la configurazione abbia tutte le chiavi necessarie
    required_keys = ['root_path', 'input_nquads', 'suffix']
    for key in required_keys:
        if key not in config:
            print(f"[ERROR] Chiave mancante '{key}' nella configurazione per {directory_key} - evangelisti_metadata_extraction.py:479")
            print(f"[ERROR] Configurazione disponibile: {config} - evangelisti_metadata_extraction.py:480")
            sys.exit(1)
    
    # Configura percorsi
    ROOT_PATH = config['root_path']
    INPUT_NQUADS = config['input_nquads']
    
    # ✅ FIX CRITICO: FORZA SEMPRE LOWERCASE NEL SUFFIX
    suffix = config['suffix'].lower().strip()
    
    print(f"[CONFIG] Suffix originale da config: '{config['suffix']}' - evangelisti_metadata_extraction.py:490")
    print(f"[CONFIG] Suffix normalizzato: '{suffix}' - evangelisti_metadata_extraction.py:491")
    
    # Configura file di output con suffix rigorosamente lowercase
    OUTPUT_NQUADS_FS = f"FileSystem_TechMeta_{suffix}.nq"
    OUTPUT_NQUADS_TIKA = f"ApacheTika_TechMeta_{suffix}.nq"
    OUTPUT_NQUADS_EXIF = f"ExifTool_TechMeta_{suffix}.nq"
    OUTPUT_NQUADS_DROID = f"DROID_TechMeta_{suffix}.nq"  # 🆕 NUOVO
    
    # Configura URI dei grafi (anche questi con suffix lowercase)
    fs_graph_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/FS_TechMeta_{suffix}")
    tika_graph_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/AT_TechMeta_{suffix}")
    exif_graph_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/ET_TechMeta_{suffix}")
    droid_graph_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/DROID_TechMeta_{suffix}")  # 🆕 NUOVO
    
    print(f"[CONFIG] ✅ Configurazione inizializzata per: {directory_key} - evangelisti_metadata_extraction.py:505")
    print(f"[CONFIG] ROOT_PATH: {ROOT_PATH} - evangelisti_metadata_extraction.py:506")
    print(f"[CONFIG] INPUT_NQUADS: {INPUT_NQUADS} - evangelisti_metadata_extraction.py:507")
    print(f"[CONFIG] Suffix NORMALIZZATO: '{suffix}' - evangelisti_metadata_extraction.py:508")
    print(f"[CONFIG] File output generati: - evangelisti_metadata_extraction.py:509")
    print(f"[CONFIG]   FS: {OUTPUT_NQUADS_FS} - evangelisti_metadata_extraction.py:510")
    print(f"[CONFIG]   Tika: {OUTPUT_NQUADS_TIKA} - evangelisti_metadata_extraction.py:511")
    print(f"[CONFIG]   ExifTool: {OUTPUT_NQUADS_EXIF} - evangelisti_metadata_extraction.py:512")
    print(f"[CONFIG]   DROID: {OUTPUT_NQUADS_DROID} - evangelisti_metadata_extraction.py:513")  # 🆕 NUOVO

# === CONFIGURAZIONE FILE JSON PER PERSISTENZA CONTATORI ===
COUNTERS_JSON_FILE = "evangelisti_uri_counters.json"

# Configurazione per ottimizzazioni (verranno sovrascritte dagli argomenti)
CHUNK_SIZE = 100
PERSISTENCE_INTERVAL = 100
MAX_PARALLEL_WORKERS = min(4, multiprocessing.cpu_count())
ENABLE_PARALLEL_PROCESSING = True
ENABLE_DROID = True  # 🆕 NUOVO

# === BASE URIS STRUTTURATI ===
BASE_URIS = {
    'tika': "http://ficlit.unibo.it/ArchivioEvangelisti/apachetika_tmtype_",
    'exiftool': "http://ficlit.unibo.it/ArchivioEvangelisti/exiftool_tmtype_",
    'os': "http://ficlit.unibo.it/ArchivioEvangelisti/os_tmtype_",
    'droid': "http://ficlit.unibo.it/ArchivioEvangelisti/droid_tmtype_",  # 🆕 NUOVO
    'software_stack': "http://ficlit.unibo.it/ArchivioEvangelisti/software_stack_",
    'person': "http://ficlit.unibo.it/ArchivioEvangelisti/person_",
    'date': "http://ficlit.unibo.it/ArchivioEvangelisti/date_",
    'extent_type': "http://ficlit.unibo.it/ArchivioEvangelisti/extent_type_"
}

# === NAMESPACES ===
rico = Namespace("https://www.ica.org/standards/RiC/ontology#")
bodi = Namespace("http://w3id.org/bodi#")
prov = Namespace("http://www.w3.org/ns/prov#")
premis = Namespace("http://www.loc.gov/premis/v3")

# === CACHE GLOBALI PER OTTIMIZZAZIONI ===
created_metadata_instances = set()
created_metadata_types = set()
created_extent_types = set()
created_software_stacks = set()
shared_entities_initialized = set()

# === CONTATORI PER SERIALIZZAZIONE INCREMENTALE ===
total_instantiations_processed = 0
persistence_counter = 0
shared_entities_written = {"fs": False, "tika": False, "exif": False, "droid": False}  # 🆕 AGGIUNTO DROID

# === MAPPE GLOBALI PER DEDUPLICAZIONE CON URI STRUTTURATI - CARICATE DA JSON ===
global_metadata_type_map = {}
software_counters = {'tika': 0, 'exiftool': 0, 'os': 0, 'droid': 0}  # 🆕 AGGIUNTO DROID
instantiation_maps = {}
instantiation_counters = {}
software_stack_cache = {}
software_stack_counter = 0
exception_counters = {}
current_exiftool_version = None
current_tika_version = None
current_droid_version = None  # 🆕 NUOVO

# 🆕 NUOVE CACHE PER SOFTWARE E COMPONENTS
software_entities_cache = {}  # {software_key: uri}
software_components_cache = {}  # {component_key: uri}
software_counter = 0
software_component_counter = 0

# === FUNZIONI PER PERSISTENZA CONTATORI JSON ===

def load_counters_from_json():
    """Carica i contatori da file JSON se esiste"""
    global software_counters, software_stack_counter, global_metadata_type_map
    global software_stack_cache, exception_counters
    
    if not os.path.exists(COUNTERS_JSON_FILE):
        print(f"[INFO] File contatori non trovato: {COUNTERS_JSON_FILE} - evangelisti_metadata_extraction.py:581")
        print(f"[INFO] Inizializzazione con contatori vuoti - evangelisti_metadata_extraction.py:582")
        return
    
    try:
        with open(COUNTERS_JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Validazione struttura JSON
        required_keys = ['software_counters', 'software_stack_counter', 'last_updated']
        for key in required_keys:
            if key not in data:
                print(f"[WARNING] Chiave mancante nel JSON: {key} - evangelisti_metadata_extraction.py:593")
        
        # Carica contatori software
        if 'software_counters' in data:
            software_counters.update(data['software_counters'])
            print(f"[LOAD] Software counters: {software_counters} - evangelisti_metadata_extraction.py:598")
        
        # Carica contatore software stack
        if 'software_stack_counter' in data:
            software_stack_counter = data['software_stack_counter']
            print(f"[LOAD] Software stack counter: {software_stack_counter} - evangelisti_metadata_extraction.py:603")
        
        # Carica mappa metadata types
        if 'global_metadata_type_map' in data:
            for key_str, uri_str in data['global_metadata_type_map'].items():
                try:
                    key_tuple = eval(key_str) if key_str.startswith('(') else tuple(key_str.split('|'))
                    global_metadata_type_map[key_tuple] = URIRef(uri_str)
                except Exception as e:
                    print(f"[WARNING] Errore nel parsing chiave metadata type: {key_str} > {e} - evangelisti_metadata_extraction.py:612")
            print(f"[LOAD] Metadata type map: {len(global_metadata_type_map)} entries - evangelisti_metadata_extraction.py:613")
        
        # Carica cache software stack
        if 'software_stack_cache' in data:
            for cache_key, uri_str in data['software_stack_cache'].items():
                try:
                    software_stack_cache[cache_key] = URIRef(uri_str)
                except Exception as e:
                    print(f"[WARNING] Errore nel parsing software stack cache: {cache_key} > {e} - evangelisti_metadata_extraction.py:621")
            print(f"[LOAD] Software stack cache: {len(software_stack_cache)} entries - evangelisti_metadata_extraction.py:622")
        
        # Carica contatori eccezioni
        if 'exception_counters' in data:
            for key_str, count in data['exception_counters'].items():
                try:
                    key_tuple = eval(key_str) if key_str.startswith('(') else tuple(key_str.split('|'))
                    exception_counters[key_tuple] = count
                except Exception as e:
                    print(f"[WARNING] Errore nel parsing exception counter: {key_str} > {e} - evangelisti_metadata_extraction.py:631")
            print(f"[LOAD] Exception counters: {len(exception_counters)} entries - evangelisti_metadata_extraction.py:632")
        
        # Info sul file
        if 'last_updated' in data:
            print(f"[LOAD] File aggiornato: {data['last_updated']} - evangelisti_metadata_extraction.py:636")
        if 'metadata' in data:
            meta = data['metadata']
            print(f"[LOAD] Statistiche salvate: types={meta.get('total_metadata_types', 0)}, - evangelisti_metadata_extraction.py:639"
                  f"stacks={meta.get('total_software_stacks', 0)}, "
                  f"exceptions={meta.get('total_exception_contexts', 0)}")
        
        print(f"[SUCCESS] Contatori caricati da {COUNTERS_JSON_FILE} - evangelisti_metadata_extraction.py:643")
        
    except json.JSONDecodeError as e:
        print(f"[ERROR] File JSON corrotto: {e} - evangelisti_metadata_extraction.py:646")
        print(f"[INFO] Creo backup e continuo con contatori vuoti - evangelisti_metadata_extraction.py:647")
        backup_corrupted_file()
    except Exception as e:
        print(f"[ERROR] Errore nel caricamento contatori: {e} - evangelisti_metadata_extraction.py:650")
        print(f"[INFO] Continuo con contatori vuoti - evangelisti_metadata_extraction.py:651")

def backup_corrupted_file():
    """Crea backup di file JSON corrotto"""
    try:
        if os.path.exists(COUNTERS_JSON_FILE):
            backup_name = f"{COUNTERS_JSON_FILE}.corrupted.{int(time.time())}"
            os.rename(COUNTERS_JSON_FILE, backup_name)
            print(f"[BACKUP] File corrotto salvato come: {backup_name} - evangelisti_metadata_extraction.py:659")
    except Exception as e:
        print(f"[ERROR] Impossibile creare backup: {e} - evangelisti_metadata_extraction.py:661")

def save_counters_to_json():
    """Salva i contatori attuali su file JSON"""
    global software_counters, software_stack_counter, global_metadata_type_map
    global software_stack_cache, exception_counters
    
    try:
        data = {
            'software_counters': software_counters,
            'software_stack_counter': software_stack_counter,
            'global_metadata_type_map': {},
            'software_stack_cache': {},
            'exception_counters': {},
            'last_updated': datetime.now().isoformat(),
            'metadata': {
                'total_metadata_types': len(global_metadata_type_map),
                'total_software_stacks': len(software_stack_cache),
                'total_exception_contexts': len(exception_counters)
            }
        }
        
        # Serializza mappa metadata types
        for key_tuple, uri_ref in global_metadata_type_map.items():
            key_str = str(key_tuple)
            data['global_metadata_type_map'][key_str] = str(uri_ref)
        
        # Serializza cache software stack
        for cache_key, uri_ref in software_stack_cache.items():
            data['software_stack_cache'][cache_key] = str(uri_ref)
        
        # Serializza contatori eccezioni
        for key_tuple, count in exception_counters.items():
            key_str = str(key_tuple)
            data['exception_counters'][key_str] = count
        
        # Salva con backup del file precedente
        if os.path.exists(COUNTERS_JSON_FILE):
            backup_file = f"{COUNTERS_JSON_FILE}.backup"
            os.rename(COUNTERS_JSON_FILE, backup_file)
            print(f"[BACKUP] File precedente salvato come {backup_file} - evangelisti_metadata_extraction.py:701")
        
        with open(COUNTERS_JSON_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        file_size = os.path.getsize(COUNTERS_JSON_FILE)
        print(f"[SAVE] Contatori salvati in {COUNTERS_JSON_FILE} ({file_size} bytes) - evangelisti_metadata_extraction.py:707")
        
    except Exception as e:
        print(f"[ERROR] Errore nel salvataggio contatori: {e} - evangelisti_metadata_extraction.py:710")

def print_counters_summary():
    """Stampa un riepilogo dei contatori attuali"""
    print(f"\n=== RIEPILOGO CONTATORI === - evangelisti_metadata_extraction.py:714")
    print(f"Software counters: {software_counters} - evangelisti_metadata_extraction.py:715")
    print(f"Software stack counter: {software_stack_counter} - evangelisti_metadata_extraction.py:716")
    print(f"Metadata types registrati: {len(global_metadata_type_map)} - evangelisti_metadata_extraction.py:717")
    print(f"Software stacks in cache: {len(software_stack_cache)} - evangelisti_metadata_extraction.py:718")
    print(f"Exception contexts: {len(exception_counters)} - evangelisti_metadata_extraction.py:719")
    
    if global_metadata_type_map:
        print(f"\nEsempi metadata types: - evangelisti_metadata_extraction.py:722")
        for i, (key, uri) in enumerate(list(global_metadata_type_map.items())[:3]):
            print(f"{key} > {uri} - evangelisti_metadata_extraction.py:724")
        if len(global_metadata_type_map) > 3:
            extra_types = len(global_metadata_type_map) - 3
            print(f"... e altri {extra_types} - evangelisti_metadata_extraction.py:727")

    
    if software_stack_cache:
        print(f"\nEsempi software stacks: - evangelisti_metadata_extraction.py:731")
        for i, (key, uri) in enumerate(list(software_stack_cache.items())[:3]):
            components = key.split('|')[:2]
            print(f"{' + '.join(components)}{'...' if len(key.split('|')) > 2 else ''} > {uri} - evangelisti_metadata_extraction.py:734")
        if len(software_stack_cache) > 3:
            extra_stacks = len(software_stack_cache) - 3
            print(f"... e altri {extra_stacks} - evangelisti_metadata_extraction.py:737")       
    print(f"=============================\n - evangelisti_metadata_extraction.py:738")

# === REGEX PRE-COMPILATE PER PARSING OTTIMIZZATO ===
instantiation_pattern = re.compile(r'^<([^>]+)>\s+<[^>]*rdf-syntax-ns#type>\s+<[^>]*Instantiation[^>]*>')
location_pattern = re.compile(r'^<([^>]+)>\s+<[^>]*prov#atLocation>\s+<([^>]+)>')
label_pattern = re.compile(r'^<([^>]+)>\s+<[^>]*rdfs#label>\s+"([^"]+)"')

# === FUNZIONI HELPER PER URI STRUTTURATI ===

def extract_instantiation_identifier(instantiation_uri_str):
    """Estrae identificatore univoco dall'URI instantiation"""
    patterns = [
        r'/([^/]+)$',
        r'#([^#]+)$', 
        r'instantiation/([^/]+)',
        r'Instantiation/([^/]+)',
        r'/([A-Z0-9_]+)$',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, instantiation_uri_str)
        if match:
            identifier = match.group(1)
            return identifier.replace(" ", "_").replace(":", "_").replace("/", "_")


def determine_software_source(context_info):
    """Determina la fonte software dal contesto"""
    if context_info and 'tool' in context_info:
        tool = context_info['tool'].lower()
        if 'exif' in tool:
            return 'exiftool'
        elif 'droid' in tool:  # 🆕 NUOVO
            return 'droid'
        elif 'os' in tool or 'python' in tool:
            return 'os'
    return 'tika'

def get_main_software_from_context(context_info, dataset, graph_uri):
    """Ottiene l'URI del Software principale basato sul context_info"""
    if context_info and 'tool' in context_info:
        tool = context_info['tool'].lower()
        if 'exif' in tool:
            return get_or_create_software_entity("ExifTool", dataset, graph_uri)
        elif 'droid' in tool:  # 🆕 NUOVO
            return get_or_create_software_entity("DROID", dataset, graph_uri)
        elif 'os' in tool or 'python' in tool:
            # Per Python ottieni il Python interpreter specifico
            python_implementation = platform.python_implementation()
            python_version = platform.python_version()
            python_software_name = f"{python_implementation} {python_version}"
            return get_or_create_software_entity(python_software_name, dataset, graph_uri)
    
    # Default: Apache Tika
    return get_or_create_software_entity("Apache Tika", dataset, graph_uri)

def get_tool_abbreviation(tool_name):
    """Ottiene abbreviazione tool per URI"""
    tool_lower = tool_name.lower()
    if 'apache tika' in tool_lower or tool_lower == 'apache tika':
        return 'apachetika'
    elif 'exiftool' in tool_lower:
        return 'exiftool'
    elif 'droid' in tool_lower:  # 🆕 NUOVO
        return 'droid'
    elif 'python os' in tool_lower or 'os' in tool_lower:
        return 'os'
    else:
        return tool_name.lower().replace(' ', '').replace('-', '').replace('.', '')

def normalize_field_name(field):
    """Normalizza nome campo per URI"""
    return field.replace(" ", "_").replace(":", "_").replace("/", "_").replace("-", "_")

# === FUNZIONI OTTIMIZZATE PER ESTRARRE INSTANTIATIONS ===
@timing_decorator
def extract_instantiations_with_paths(nquads_file):
    """Estrae tutte le instantiation con i loro path dal file .nq - VERSIONE OTTIMIZZATA"""
    if not os.path.exists(nquads_file):
        print(f"[ERRORE] File {nquads_file} non trovato! - evangelisti_metadata_extraction.py:817")
        print(f"[DEBUG] Directory corrente: {os.getcwd()} - evangelisti_metadata_extraction.py:818")
        print(f"[DEBUG] File nella directory corrente: {os.listdir('.')[:10]} - evangelisti_metadata_extraction.py:819")
        return []
    
    file_size = os.path.getsize(nquads_file)
    print(f"[INFO] File trovato: {nquads_file} (dimensione: {file_size} bytes) - evangelisti_metadata_extraction.py:823")
    
    # Approccio 1: Parsing manuale ottimizzato
    print("\n[INFO] Tentativo 1: Parsing manuale ottimizzato... - evangelisti_metadata_extraction.py:826")
    instantiations = try_manual_parsing_optimized(nquads_file)
    if instantiations:
        print(f"[INFO] Parsing manuale riuscito: {len(instantiations)} instantiation - evangelisti_metadata_extraction.py:829")
        return instantiations
    
    # Approccio 2: Dataset
    print("\n[INFO] Tentativo 2: Parsing con Dataset... - evangelisti_metadata_extraction.py:833")
    instantiations = try_dataset_parsing(nquads_file)
    if instantiations:
        print(f"[INFO] Parsing Dataset riuscito: {len(instantiations)} instantiation - evangelisti_metadata_extraction.py:836")
        return instantiations
    
    # Approccio 3: Graph fallback
    print("\n[INFO] Tentativo 3: Parsing con Graph... - evangelisti_metadata_extraction.py:840")
    return try_graph_parsing(nquads_file)

def try_manual_parsing_optimized(nquads_file):
    """Parsing manuale ottimizzato con regex pre-compilate"""
    try:
        instantiation_uris = {}
        location_labels = {}
        
        with open(nquads_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                match = instantiation_pattern.match(line)
                if match:
                    instantiation_uris[match.group(1)] = True
                    continue
                    
                match = location_pattern.match(line)
                if match and match.group(1) in instantiation_uris:
                    instantiation_uris[match.group(1)] = match.group(2)
                    continue
                    
                match = label_pattern.match(line)
                if match:
                    location_labels[match.group(1)] = match.group(2)
                
                if line_num % 10000 == 0:
                    print(f"[DEBUG] Processate {line_num} righe... - evangelisti_metadata_extraction.py:870")
        
        results = []
        for inst_uri, location_uri in instantiation_uris.items():
            if isinstance(location_uri, str) and location_uri in location_labels:
                results.append({
                    'instantiation': URIRef(inst_uri),
                    'path': location_labels[location_uri]
                })
        
        return results
        
    except Exception as e:
        print(f"[ERRORE] Errore nel parsing manuale ottimizzato: {e} - evangelisti_metadata_extraction.py:883")
        return []

def try_dataset_parsing(nquads_file):
    """Parsing con Dataset RDF"""
    try:
        ds = Dataset()
        ds.parse(nquads_file, format='nquads')
        print(f"[INFO] Dataset caricato con {len(ds)} quad - evangelisti_metadata_extraction.py:891")
        
        instantiations = []
        for graph in ds.graphs():
            for s, p, o in graph:
                if p == RDF.type and (str(o).endswith("Instantiation") or "Instantiation" in str(o)):
                    for location in graph.objects(s, prov.atLocation):
                        for label in graph.objects(location, RDFS.label):
                            instantiations.append({
                                'instantiation': s,
                                'path': str(label)
                            })
                            break
                        break
        return instantiations
    except Exception as e:
        print(f"[ERRORE] Errore con Dataset: {e} - evangelisti_metadata_extraction.py:907")
        return []

def try_graph_parsing(nquads_file):
    """Parsing con Graph RDF"""
    try:
        g = Graph()
        for format_name in ['nquads', 'nt', 'n3', 'turtle']:
            try:
                g.parse(nquads_file, format=format_name)
                print(f"[INFO] Parsing riuscito con formato: {format_name} - evangelisti_metadata_extraction.py:917")
                break
            except:
                continue
                
        instantiations = []
        for s, p, o in g:
            if p == RDF.type and (str(o).endswith("Instantiation") or "Instantiation" in str(o)):
                for location in g.objects(s, prov.atLocation):
                    for label in g.objects(location, RDFS.label):
                        instantiations.append({
                            'instantiation': s,
                            'path': str(label)
                        })
                        break
                    break
        return instantiations
    except Exception as e:
        print(f"[ERRORE] Errore con Graph: {e} - evangelisti_metadata_extraction.py:935")
        return []

# === TIMESTAMP PER L'ESTRAZIONE ===
extraction_timestamp = datetime.now()
locale.setlocale(locale.LC_TIME, 'en_US.UTF-8') 
extraction_date_str = extraction_timestamp.isoformat()
extraction_date_expr = extraction_timestamp.strftime("%d %B %Y at %H:%M")

# === ENTITÀ CONDIVISE ===
lucia_person = URIRef(f"{BASE_URIS['person']}LuciaGiagnolini")
extraction_date = URIRef(f"{BASE_URIS['date']}{extraction_timestamp.strftime('%Y%m%d_%H%M%S')}")

EXCEPTION_WARNING_FIELDS = {
    "X-TIKA:EXCEPTION:warn", "X-TIKA:EXCEPTION:container_exception",
    "X-TIKA:EXCEPTION:embedded_exception", "X-TIKA:EXCEPTION:embedded_parser",
    "Warning"
}

TIKA_EXCLUDED_FIELDS = {
    "X-TIKA:Parsed-By", "X-TIKA:Parsed-By-Full-Set", "X-TIKA:parse_time_millis",
    "X-TIKA:embedded_depth", "embeddedRelationshipId", "X-TIKA:embedded_resource_path", 
    "X-TIKA:origResourceName", "X-TIKA:content_handler"
}

EXIFTOOL_VERSION_FIELDS = {
    "ExifToolVersion", "ExifTool Version", "Tool Version", "Version",
    "ExifTool:Version", "System:ExifToolVersion",
    "Data", "Versione"  # Campi problematici in italiano
}

# 🆕 NUOVO: Campi DROID da escludere/gestire separatamente
DROID_EXCLUDED_FIELDS = {
    "ID", "PARENT_ID", "URI", "NAME", "METHOD", "STATUS", "SIZE", "TYPE",
    "EXT", "LAST_MODIFIED", "EXTENSION_MISMATCH", "HASH", "FORMAT_COUNT", 
    "PUID", "MIME_TYPE", "FORMAT_NAME", "FORMAT_VERSION", 
    "PUID1", "MIME_TYPE1", "FORMAT_NAME1", "FORMAT_VERSION1", "FILE_PATH"
}

DROID_VERSION_FIELDS = {
    "DROID_VERSION", "SIGNATURE_VERSION", "CONTAINER_SIGNATURE_VERSION"
}

# === DOCUMENTAZIONE SOFTWARE ===
SOFTWARE_DOCUMENTATION_URLS = {
    'apache tika': 'https://tika.apache.org/',
    'exiftool': 'https://exiftool.org/',
    'droid': 'https://www.nationalarchives.gov.uk/information-management/manage-information/preserving-digital-records/droid/',
    'python os - miscellaneous operating system interfaces module': 'https://docs.python.org/3/library/os.html'
}

def get_documentation_url(software_name):
    """Ottiene URL documentazione per il software specificato"""
    software_key = software_name.lower().strip()
    
    # Mapping diretto
    if software_key in SOFTWARE_DOCUMENTATION_URLS:
        return SOFTWARE_DOCUMENTATION_URLS[software_key]
    
    # Pattern matching per varianti
    if 'apache tika' in software_key:
        return SOFTWARE_DOCUMENTATION_URLS['apache tika']
    elif 'exiftool' in software_key:
        return SOFTWARE_DOCUMENTATION_URLS['exiftool']
    elif 'droid' in software_key:
        return SOFTWARE_DOCUMENTATION_URLS['droid']
    elif 'python os' in software_key or software_key == 'python os - miscellaneous operating system interfaces module':
        return SOFTWARE_DOCUMENTATION_URLS['python os - miscellaneous operating system interfaces module']
    
    return None

# === FUNZIONI UTILITY ===
def get_file_permissions(mode):
    return oct(stat.S_IMODE(mode))

def normalize_software_name(name):
    return name.replace(" ", "").replace(".", "").replace("-", "").replace("/", "_").replace(":", "_")
    
def add_instantiation_label_safe(instantiation_uri, relative_path, dataset, graph_uri):
    """
    Aggiunge label all'instantiation con validazione robusta - VERSIONE CORRETTA
    """
    try:
        # 🔧 VALIDAZIONE RIGOROSA
        if not relative_path or not isinstance(relative_path, str):
            print(f"[LABELERROR] Invalid path for {instantiation_uri}: '{relative_path}' - evangelisti_metadata_extraction.py:1018")
            return False
            
        relative_path = str(relative_path).strip()
        if not relative_path:
            print(f"[LABELERROR] Empty path for {instantiation_uri} - evangelisti_metadata_extraction.py:1023")
            return False
        
        # 🔧 ESTRAZIONE NOME FILE CON FALLBACK
        file_name = os.path.basename(relative_path)
        
        # Se basename è vuoto, usa l'ultimo segmento valido
        if not file_name or file_name in ['', '.', '..']:
            path_segments = [seg for seg in relative_path.split('/') if seg.strip()]
            if path_segments:
                file_name = path_segments[-1]
            else:
                print(f"[LABELERROR] Cannot extract filename from '{relative_path}' - evangelisti_metadata_extraction.py:1035")
                return False
        
        # 🔧 VALIDAZIONE FINALE
        final_label = file_name.strip()
        if len(final_label) == 0:
            print(f"[LABELERROR] Empty filename from '{relative_path}' - evangelisti_metadata_extraction.py:1041")
            return False
            
        # 🔧 AGGIUNGI LABEL
        dataset.add((instantiation_uri, RDFS.label, Literal(final_label), graph_uri))
        print(f"[LABELOK] '{final_label}' added to {graph_uri} - evangelisti_metadata_extraction.py:1046")
        return True
        
    except Exception as e:
        print(f"[LABELEXCEPTION] Error for {relative_path}: {e} - evangelisti_metadata_extraction.py:1050")
        return False

def validate_output_filenames():
    """Valida che i nomi dei file di output siano tutti lowercase"""
    global OUTPUT_NQUADS_FS, OUTPUT_NQUADS_TIKA, OUTPUT_NQUADS_EXIF, OUTPUT_NQUADS_DROID  # 🆕 DROID
    
    files_to_check = [
        ("FileSystem", OUTPUT_NQUADS_FS),
        ("Tika", OUTPUT_NQUADS_TIKA), 
        ("ExifTool", OUTPUT_NQUADS_EXIF),
        ("DROID", OUTPUT_NQUADS_DROID)  # 🆕 NUOVO
    ]
    
    print(f"\n[VALIDATION] Verifica nomi file output: - evangelisti_metadata_extraction.py:1064")
    all_valid = True
    
    for file_type, filename in files_to_check:
        print(f"[VALIDATION] {file_type}: {filename} - evangelisti_metadata_extraction.py:1068")
        
        # Controlla che non contenga caratteri maiuscoli nel suffix
        if any(c.isupper() for c in filename.split('_')[-1].split('.')[0]):
            print(f"[ERROR] File {file_type} contiene caratteri maiuscoli nel suffix! - evangelisti_metadata_extraction.py:1072")
            all_valid = False
        else:
            print(f"[VALIDATION] ✅ {file_type} OK (lowercase) - evangelisti_metadata_extraction.py:1075")
    
    if not all_valid:
        print(f"[ERROR] Alcuni file hanno nomi non conformi. Interrompo. - evangelisti_metadata_extraction.py:1078")
        sys.exit(1)
    
    print(f"[VALIDATION] ✅ Tutti i nomi file sono conformi - evangelisti_metadata_extraction.py:1081")

# 🆕 AGGIUNGI QUI LE NUOVE FUNZIONI
def cleanup_tika_temp_files():
    """Pulisci file temporanei Tika - VERSIONE CORRETTA"""
    import tempfile
    import glob
    import shutil
    
    temp_dir = tempfile.gettempdir()
    
    # Pattern più specifici per evitare cancellazioni accidentali
    tika_temp_patterns = [
        os.path.join(temp_dir, "apache-tika-*"),
        os.path.join(temp_dir, "tika-*"),
        os.path.join(temp_dir, "*tika*.tmp"),
        os.path.join(temp_dir, "*cxf*.tmp"),  # AGGIUNTO: file CXF temporanei
        "/tmp/apache-tika-*",
        "/tmp/tika-*",
        "/tmp/*tika*.tmp",
        "/tmp/*cxf*.tmp"  # AGGIUNTO: file CXF temporanei
    ]
    
    cleaned = 0
    total_size = 0
    
    for pattern in tika_temp_patterns:
        for temp_file in glob.glob(pattern):
            try:
                if os.path.exists(temp_file):
                    # Ottieni dimensione prima della cancellazione
                    if os.path.isfile(temp_file):
                        size = os.path.getsize(temp_file)
                        total_size += size
                        os.unlink(temp_file)
                        cleaned += 1
                    elif os.path.isdir(temp_file):
                        size = sum(os.path.getsize(os.path.join(dirpath, filename))
                                 for dirpath, dirnames, filenames in os.walk(temp_file)
                                 for filename in filenames)
                        total_size += size
                        shutil.rmtree(temp_file)
                        cleaned += 1
            except Exception as e:
                print(f"[CLEANUP] Errore rimozione {temp_file}: {e}")
    
    if cleaned > 0:
        size_mb = total_size / (1024 * 1024)
        print(f"[CLEANUP] Rimossi {cleaned} file temporanei ({size_mb:.1f} MB liberati)")
    
    return cleaned

def force_restart_tika_server():
    """Riavvia effettivamente Tika Server per liberare memoria interna"""
    import signal
    import psutil
    
    print(f"[TIKA_RESTART] 🔄 Iniziando riavvio forzato Tika Server...")
    
    try:
        tika_processes = []
        
        # Trova tutti i processi Tika
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if (proc.info['name'] == 'java' and 
                    proc.info['cmdline'] and
                    any('tika' in str(cmd).lower() for cmd in proc.info['cmdline'])):
                    tika_processes.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        if not tika_processes:
            print(f"[TIKA_RESTART] ⚠️ Nessun processo Tika trovato")
            return start_tika_server()
        
        # Termina i processi
        for proc in tika_processes:
            try:
                print(f"[TIKA_RESTART] Terminando processo Tika PID {proc.pid}")
                proc.terminate()
                proc.wait(timeout=30)
                print(f"[TIKA_RESTART] ✅ Processo {proc.pid} terminato")
            except psutil.TimeoutExpired:
                print(f"[TIKA_RESTART] ⚡ Force killing processo {proc.pid}")
                proc.kill()
            except Exception as e:
                print(f"[TIKA_RESTART] Errore terminazione {proc.pid}: {e}")
        
        # Cleanup aggressivo dopo terminazione
        print(f"[TIKA_RESTART] 🧹 Cleanup post-terminazione...")
        cleanup_tika_temp_files()
        
        # Pausa per cleanup completo
        time.sleep(3)
        
        # Riavvia server
        print(f"[TIKA_RESTART] 🚀 Riavviando Tika Server...")
        return start_tika_server()
        
    except Exception as e:
        print(f"[TIKA_RESTART] ❌ Errore nel riavvio: {e}")
        return False


# 🆕 NUOVO: Funzione per riavvio Tika Server
def restart_tika_server_if_needed(processed_files_count, restart_interval=1000):  # AUMENTATO da 500 a 1000
    """Riavvia Tika Server meno frequentemente ma pulisci più spesso"""
    
    # Cleanup più frequente ogni 100 file invece che al restart
    if processed_files_count % 100 == 0 and processed_files_count > 0:
        print(f"[TIKA] Cleanup periodico dopo {processed_files_count} file...")
        cleanup_tika_temp_files()
        
    # Restart meno frequente
    if processed_files_count % restart_interval == 0 and processed_files_count > 0:
        print(f"[TIKA] Considerazione riavvio server dopo {processed_files_count} file processati...")
        cleanup_tika_temp_files()
        print("[TIKA] Cleanup intensivo completato invece del riavvio")
        return True
    
    return False

def monitor_temp_space_enhanced():
    """Monitora spazio temporaneo con controllo dimensioni file CXF"""
    import shutil
    import tempfile
    import glob
    
    temp_dir = tempfile.gettempdir()
    total, used, free = shutil.disk_usage(temp_dir)
    free_gb = free / (1024**3)
    
    # Controlla file CXF specifici
    cxf_pattern = os.path.join(temp_dir, "*cxf*")
    cxf_files = glob.glob(cxf_pattern)
    cxf_total_size = 0
    
    for cxf_file in cxf_files:
        try:
            if os.path.isfile(cxf_file):
                size = os.path.getsize(cxf_file)
                cxf_total_size += size
                
                # Se un singolo file CXF è > 1GB, rimuovilo immediatamente
                if size > 1024**3:  # 1 GB
                    print(f"[CRITICAL] File CXF gigante rilevato: {cxf_file} ({size/(1024**3):.1f} GB)")
                    try:
                        os.unlink(cxf_file)
                        print(f"[CRITICAL] File CXF gigante rimosso")
                    except Exception as e:
                        print(f"[ERROR] Impossibile rimuovere file CXF gigante: {e}")
        except:
            pass
    
    if cxf_total_size > 0:
        cxf_gb = cxf_total_size / (1024**3)
        print(f"[TEMPSPACE] File CXF totali: {cxf_gb:.1f}GB")
        
        # Se i file CXF occupano più di 10GB, cleanup aggressivo
        if cxf_gb > 10:
            print(f"[CRITICAL] File CXF eccessivi ({cxf_gb:.1f}GB) - cleanup aggressivo")
            cleanup_tika_temp_files()
    
    print(f"[TEMPSPACE] Spazio libero: {free_gb:.1f}GB")
    
    if free_gb < 10:  # AUMENTATO da 5GB a 10GB la soglia
        print(f"[WARNING] Spazio temporaneo molto basso: {free_gb:.1f}GB liberi")
        cleaned = cleanup_tika_temp_files()
        if cleaned > 0:
            print(f"[CLEANUP] Cleanup completato per liberare spazio")
            return True
    
    return False

@timing_decorator
def extract_droid_metadata_batch(file_paths):
    """Estrazione batch ottimizzata per DROID - CON SUPPORTO PIPELINE"""
    if not file_paths:
        print("[DEBUG DROID] Nessun file da processare")
        return {}
    
    # Rileva se chiamato dalla pipeline
    is_pipeline = os.environ.get('EVANGELISTI_PIPELINE_MODE') == '1'
    
    if is_pipeline:
        print(f"[DROID PIPELINE] Modalità pipeline attiva per {len(file_paths)} file")
        return _extract_droid_pipeline_mode(file_paths)
    else:
        print(f"[DROID STANDALONE] Modalità standalone per {len(file_paths)} file")
        return _extract_droid_standalone_mode(file_paths)

def _extract_droid_pipeline_mode(file_paths):
    """Modalità ottimizzata per esecuzione in pipeline"""
    
    # Rileva se è pipeline veloce
    fast_pipeline = os.environ.get('EVANGELISTI_FAST_PIPELINE_MODE') == '1'
    
    if fast_pipeline:
        print(f"[DROID FAST PIPELINE] Modalità veloce attivata per {len(file_paths)} file")
        # Usa parametri simili allo standalone
        MAX_BATCH_SIZE = 200  # Invece di 25
        INTER_BATCH_SLEEP = 0  # Nessuna pausa
        total_batches = 1 if len(file_paths) <= MAX_BATCH_SIZE else (len(file_paths) + MAX_BATCH_SIZE - 1) // MAX_BATCH_SIZE
        
        print(f"[DROID FAST PIPELINE] Batch ridotti da ~40 a {total_batches}")
    else:
        # Parametri conservativi originali
        MAX_BATCH_SIZE = 25
        INTER_BATCH_SLEEP = 1.0
    
    # Resto della logica rimane uguale
    results = {}
    total_batches = (len(file_paths) + MAX_BATCH_SIZE - 1) // MAX_BATCH_SIZE
    
    print(f"[DROID PIPELINE] Processamento in {total_batches} batch di max {MAX_BATCH_SIZE} file")
    
    for i in range(0, len(file_paths), MAX_BATCH_SIZE):
        batch_num = i // MAX_BATCH_SIZE + 1
        batch = file_paths[i:i + MAX_BATCH_SIZE]
        
        print(f"[DROID PIPELINE] Batch {batch_num}/{total_batches}: {len(batch)} file")
        
        # Processa batch con timeout ridotto
        batch_results = _process_single_droid_batch_optimized(batch, timeout=60)
        results.update(batch_results)
        
        # Pausa tra batch per evitare sovraccarico sistema
        if batch_num < total_batches:
            print(f"[DROID PIPELINE] Pausa {INTER_BATCH_SLEEP}s tra batch...")
            time.sleep(INTER_BATCH_SLEEP)
        
        # Cleanup memoria ogni 5 batch
        if batch_num % 5 == 0:
            import gc
            gc.collect()
            print(f"[DROID PIPELINE] Cleanup memoria dopo batch {batch_num}")
    
    print(f"[DROID PIPELINE] Completati {total_batches} batch, {len(results)} risultati")
    return results

def _extract_droid_standalone_mode(file_paths):
    """Modalità originale per esecuzione standalone"""
    return _process_single_droid_batch_optimized(file_paths, timeout=120)

def _process_single_droid_batch_optimized(file_paths, timeout=120):
    """Processo singolo batch DROID con opzioni ottimizzate"""
    if not file_paths:
        return {}
    
    # === CONTROLLI PRELIMINARI (semplificati) ===
    if not os.path.exists(DROID_JAR_PATH):
        print(f"[ERRORE DROID] DROID jar non trovato: {DROID_JAR_PATH}")
        return {path: ({}, 0) for path in file_paths}
    
    # Verifica rapida file validi
    valid_files = []
    for file_path in file_paths:
        if os.path.exists(file_path) and os.path.isfile(file_path) and not os.path.islink(file_path):
            valid_files.append(file_path)
    
    if not valid_files:
        print("[WARNING DROID] Nessun file valido da processare")
        return {path: ({}, 0) for path in file_paths}
    
    print(f"[DEBUG DROID] File validi: {len(valid_files)}/{len(file_paths)}")
    
    # === ESECUZIONE DROID OTTIMIZZATA ===
    try:
        start_time = time.time()
        
        # File temporanei
        temp_profile = tempfile.mktemp(suffix='.droid')
        temp_csv = tempfile.mktemp(suffix='.csv')
        
        try:
            # Comando DROID ottimizzato per performance
            scan_command = [
                'java', 
                '-Xmx1G',  # Limita memoria per pipeline
                '-XX:+UseG1GC',  # GC ottimizzato
                '-jar', DROID_JAR_PATH,
                '-p', temp_profile,
                '-q'  # Modalità quiet per ridurre output
            ]
            
            # Aggiungi file (mantieni logica originale ma con limiti)
            files_to_process = valid_files[:25] if os.environ.get('EVANGELISTI_PIPELINE_MODE') == '1' else valid_files
            
            for file_path in files_to_process:
                scan_command.extend(['-a', file_path])
            
            print(f"[DEBUG DROID] Esecuzione scansione per {len(files_to_process)} file (timeout: {timeout}s)")
            
            # Esegui scansione con timeout configurabile
            scan_result = subprocess.run(
                scan_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False
            )
            
            if scan_result.returncode != 0:
                print(f"[ERRORE DROID] Scansione fallita: {scan_result.returncode}")
                stderr_text = scan_result.stderr.decode('utf-8', errors='ignore')[:200]
                print(f"[DROID STDERR] {stderr_text}")
                return {path: ({}, 0) for path in file_paths}
            
            # Export CSV con timeout ridotto
            export_command = [
                'java', '-Xmx1G', '-jar', DROID_JAR_PATH,
                '-p', temp_profile,
                '-e', temp_csv,
                '-co', 'PUID,FORMAT_NAME,FORMAT_VERSION,FILE_PATH'
            ]
            
            export_result = subprocess.run(
                export_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,  # Timeout fisso ridotto per export
                check=False
            )
            
            if export_result.returncode != 0:
                print(f"[ERRORE DROID] Export fallito: {export_result.returncode}")
                return {path: ({}, 0) for path in file_paths}
            
            end_time = time.time()
            total_extraction_time_ms = (end_time - start_time) * 1000
            
        finally:
            # Cleanup immediato file temporanei
            if os.path.exists(temp_profile):
                try:
                    os.unlink(temp_profile)
                except:
                    pass
    
        # === PARSING CSV SEMPLIFICATO ===
        if not os.path.exists(temp_csv):
            print(f"[ERRORE DROID] File CSV non creato")
            return {path: ({}, 0) for path in file_paths}
        
        droid_by_path = {}
        
        try:
            with open(temp_csv, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    file_path = row.get('FILE_PATH', '').strip()
                    if not file_path:
                        continue
                    
                    normalized_path = os.path.abspath(file_path)
                    
                    # Prendi metadati escludendo FILE_PATH
                    filtered_metadata = {}
                    for field, value in row.items():
                        if field != 'FILE_PATH':
                            filtered_metadata[field] = value.strip() if value else ""
                    
                    droid_by_path[normalized_path] = filtered_metadata
            
            # Cleanup CSV
            try:
                os.unlink(temp_csv)
            except:
                pass
                
        except Exception as e:
            print(f"[ERRORE DROID] Parsing CSV: {e}")
            return {path: ({}, 0) for path in file_paths}
        
        # === COSTRUZIONE RISULTATI ===
        results = {}
        per_file_time = total_extraction_time_ms / len(files_to_process) if files_to_process else 0
        
        for original_path in file_paths:
            normalized_path = os.path.abspath(original_path)
            
            if normalized_path in droid_by_path:
                metadata_obj = droid_by_path[normalized_path]
                results[original_path] = (metadata_obj, per_file_time)
            else:
                results[original_path] = ({}, 0)
        
        files_with_metadata = sum(1 for path, (metadata, _) in results.items() 
                                 if any(value for value in metadata.values()))
        
        print(f"[DEBUG DROID] Risultati: {files_with_metadata}/{len(file_paths)} file con metadati")
        print(f"[DEBUG DROID] Tempo totale: {total_extraction_time_ms:.2f}ms")
        
        return results
        
    except subprocess.TimeoutExpired:
        print(f"[ERRORE DROID] Timeout dopo {timeout}s")
        return {path: ({}, 0) for path in file_paths}
    except Exception as e:
        print(f"[ERRORE DROID] Errore generico: {e}")
        return {path: ({}, 0) for path in file_paths}


def process_in_chunks_with_incremental_persistence(instantiations_data, chunk_size=CHUNK_SIZE):
    """Processa instantiation in chunk con serializzazione incrementale - CON LABEL E DROID E MONITORING DISCO"""
    global total_instantiations_processed, fs_ds, tika_ds, exif_ds, droid_ds
    global current_exiftool_version
    
    print(f"[INFO] Processando {len(instantiations_data)} instantiation in chunk di {chunk_size} - evangelisti_metadata_extraction.py:1371")
    print(f"[INFO] Serializzazione incrementale ogni {PERSISTENCE_INTERVAL} instantiation - evangelisti_metadata_extraction.py:1372")
    print(f"[INFO] URI strutturati abilitati con BASE_URIS - evangelisti_metadata_extraction.py:1373")
    print(f"[INFO] ✅ AGGIUNTA AUTOMATICA LABEL INSTANTIATION DAL NOME FILE - evangelisti_metadata_extraction.py:1374")
    print(f"[INFO] 🆕 DROID METADATA EXTRACTION: {'ABILITATO' if ENABLE_DROID else 'DISABILITATO'} - evangelisti_metadata_extraction.py:1375")
    print(f"[INFO] 🔧 MONITORING DISCO TEMPORANEO ATTIVO - evangelisti_metadata_extraction.py:1376")
    
    total_exif_files_found = 0
    total_exif_metadata_extracted = 0
    total_exif_activities_created = 0
    total_droid_files_found = 0
    total_droid_metadata_extracted = 0
    total_droid_activities_created = 0
    total_labels_added = 0
    total_tika_processed = 0  # 🆕 COUNTER PER TIKA
    
    # 🆕 MONITORING INIZIALE SPAZIO DISCO
    print(f"[INIT] Verifica iniziale spazio disco...")
    monitor_temp_space_enhanced()
    
    for i in range(0, len(instantiations_data), chunk_size):
        chunk = instantiations_data[i:i + chunk_size]
        print(f"\n[DEBUG CHUNK] Processando chunk {i//chunk_size + 1}: items {i} → {i + len(chunk)} - evangelisti_metadata_extraction.py:1387")
        
        # Prepara file paths per batch ExifTool e DROID
        file_paths = []
        chunk_items = []
        
        for item in chunk:
            inst_uri = item['instantiation']
            relative_path = item['path']
            # Rimuovi il / iniziale se presente per evitare path assoluti errati
            clean_relative_path = relative_path.lstrip('/')
            full_path = os.path.join(ROOT_PATH, clean_relative_path)
            
            if not os.path.exists(full_path):
                print(f"[AVVISO] Path non trovato: {full_path} - evangelisti_metadata_extraction.py:1401")
                continue
                
            chunk_items.append((inst_uri, relative_path, full_path))
            if os.path.isfile(full_path) and not os.path.islink(full_path):
                file_paths.append(full_path)
                total_exif_files_found += 1
                if ENABLE_DROID:
                    total_droid_files_found += 1
        
        print(f"[DEBUG CHUNK] File regolari in questo chunk: {len(file_paths)} - evangelisti_metadata_extraction.py:1411")
        if len(file_paths) > 0:
            print(f"[DEBUG CHUNK] Primi 3 file: {[os.path.basename(f) for f in file_paths[:3]]} - evangelisti_metadata_extraction.py:1413")
        
        # Batch processing ExifTool
        exif_results = extract_exif_metadata_batch(file_paths) if file_paths else {}
        
        # 🆕 NUOVO: Batch processing DROID
        droid_results = {}
        if ENABLE_DROID and file_paths:
            print(f"[DEBUG DROID] Iniziando estrazione DROID per {len(file_paths)} file... - evangelisti_metadata_extraction.py:1421")
            droid_results = extract_droid_metadata_batch(file_paths)
            print(f"[DEBUG DROID] Estrazione DROID completata: {len(droid_results)} risultati - evangelisti_metadata_extraction.py:1423")
        
        # Debug ExifTool results
        if file_paths:
            print(f"[DEBUG ExifTool] Batch risultati: {len(exif_results)} file processati - evangelisti_metadata_extraction.py:1427")
            exif_files_with_data = [path for path, (metadata, _) in exif_results.items() if metadata]
            print(f"[DEBUG ExifTool] File con metadati ExifTool: {len(exif_files_with_data)} - evangelisti_metadata_extraction.py:1429")
            if exif_files_with_data:
                print(f"[DEBUG ExifTool] Esempi file con metadati: {[os.path.basename(f) for f in exif_files_with_data[:3]]} - evangelisti_metadata_extraction.py:1431")
                
            # 🆕 NUOVO: Debug DROID results
            if ENABLE_DROID and droid_results:
                droid_files_with_data = [path for path, (metadata, _) in droid_results.items() if metadata]
                print(f"[DEBUG DROID] File con metadati DROID: {len(droid_files_with_data)} - evangelisti_metadata_extraction.py:1436")
                if droid_files_with_data:
                    print(f"[DEBUG DROID] Esempi file con metadati: {[os.path.basename(f) for f in droid_files_with_data[:3]]} - evangelisti_metadata_extraction.py:1438")
        else:
            print("[DEBUG ExifTool] Nessun file regular nel chunk corrente - evangelisti_metadata_extraction.py:1440")
            if ENABLE_DROID:
                print("[DEBUG DROID] Nessun file regular nel chunk corrente - evangelisti_metadata_extraction.py:1442")
        
        # Processa ogni item nel chunk
        for j, (inst_uri, relative_path, full_path) in enumerate(chunk_items):
            print(f"\n[DEBUG ITEM {j+1}/{len(chunk_items)}] Processing: {os.path.basename(full_path)} - evangelisti_metadata_extraction.py:1446")
            
            # 🆕 MONITORING SPAZIO DISCO OGNI 50 FILE
            current_total = total_instantiations_processed + j + 1
            if current_total % 200 == 0:
                print(f"[MONITORING] Verifica spazio disco (file #{current_total})...")
                if monitor_temp_space_enhanced():
                    print(f"[MONITORING] Spazio liberato, continuando...")
            
            if add_instantiation_label_safe(inst_uri, relative_path, fs_ds, fs_graph_uri):
                total_labels_added += 1
            else:
                print(f"[WARNING] Impossibile aggiungere label per {relative_path} - evangelisti_metadata_extraction.py:1451")
            
            # 1. Estrazione filesystem
            try:
                start_time = time.time()
                stat_result = os.stat(full_path)
                fs_metadata = {
                    "st_size": stat_result.st_size,
                    "st_mtime": stat_result.st_mtime,
                    "st_ctime": stat_result.st_ctime,
                    "st_atime": stat_result.st_atime,
                    "st_mode": get_file_permissions(stat_result.st_mode),
                    "st_uid": stat_result.st_uid,
                    "st_gid": stat_result.st_gid,
                    "file_type": "directory" if os.path.isdir(full_path) else ("symlink" if os.path.islink(full_path) else "file"),
                    "st_nlink": stat_result.st_nlink,
                    "st_ino": stat_result.st_ino,
                    "st_dev": stat_result.st_dev,
                    "st_blksize": stat_result.st_blksize,
                    "st_blocks": stat_result.st_blocks,
                }
                end_time = time.time()
                extraction_time_ms = (end_time - start_time) * 1000
                
                filtered_fs_metadata = {k: v for k, v in fs_metadata.items() if v not in (None, "", [])}
                
                if filtered_fs_metadata:
                    fs_activity = create_extraction_activity(
                        "Python os - Miscellaneous operating system interfaces module", 
                        inst_uri, fs_ds, fs_graph_uri, extraction_time_ms=extraction_time_ms
                    )
                    process_metadata_with_unique_instances(
                        filtered_fs_metadata, fs_ds, fs_graph_uri, fs_activity, inst_uri, inst_uri
                    )
                    print(f"[DEBUG ITEM] FS metadati: {len(filtered_fs_metadata)} campi processati - evangelisti_metadata_extraction.py:1485")
            except Exception as e:
                print(f"[ERRORE] Errore metadati filesystem per {full_path}: {e} - evangelisti_metadata_extraction.py:1487")

            # 2. Estrazione Tika
            if os.path.isfile(full_path) and not os.path.islink(full_path):
                # 🆕 RIAVVIO PERIODICO TIKA SERVER E CLEANUP
                total_tika_processed += 1
                
                # Cleanup Tika ottimizzato per modalità pipeline
                fast_pipeline = os.environ.get('EVANGELISTI_FAST_PIPELINE_MODE') == '1'
                cleanup_interval = 500 if fast_pipeline else 100
                restart_interval = 2000 if fast_pipeline else 500

                if total_tika_processed % cleanup_interval == 0:
                    print(f"[TIKA] Cleanup periodico file temporanei (file #{total_tika_processed})...")
                    cleanup_tika_temp_files()

                if total_tika_processed % restart_interval == 0:
                    restart_tika_server_if_needed(total_tika_processed, restart_interval)
                                
                tika_metadata = extract_tika_metadata(full_path, str(inst_uri), relative_path)
                if tika_metadata:
                    tika_parsed_by = None
                    if "X-TIKA:Parsed-By" in tika_metadata:
                        parsed_by_value = tika_metadata["X-TIKA:Parsed-By"]
                        tika_parsed_by = parsed_by_value if isinstance(parsed_by_value, list) else [parsed_by_value]
                    
                    tika_activity = create_extraction_activity("Apache Tika", inst_uri, tika_ds, tika_graph_uri, tika_parsed_by, tika_metadata)
                    process_metadata_with_unique_instances(tika_metadata, tika_ds, tika_graph_uri, tika_activity, inst_uri, inst_uri)
                    print(f"[DEBUG ITEM] Tika metadati: {len(tika_metadata)} campi processati - evangelisti_metadata_extraction.py:1500")

                # 3. Estrazione ExifTool (usa risultati batch)
                print(f"[DEBUG ITEM] Checking ExifTool results for: {full_path} - evangelisti_metadata_extraction.py:1503")
                print(f"[DEBUG ITEM] File path in exif_results? {full_path in exif_results} - evangelisti_metadata_extraction.py:1504")
                
                if full_path in exif_results:
                    exif_metadata, exif_extraction_time = exif_results[full_path]
                    print(f"[DEBUG ExifTool Processing] File: {os.path.basename(full_path)} - evangelisti_metadata_extraction.py:1508")
                    print(f"[DEBUG ExifTool Processing]   Metadati: {len(exif_metadata)} campi - evangelisti_metadata_extraction.py:1509")
                    print(f"[DEBUG ExifTool Processing]   Tempo: {exif_extraction_time:.2f}ms - evangelisti_metadata_extraction.py:1510")
                    
                    if exif_metadata:
                        print(f"[DEBUG ExifTool Processing]   Primi 5 campi: {list(exif_metadata.keys())[:5]} - evangelisti_metadata_extraction.py:1513")
                        print(f"[DEBUG ExifTool Processing]   Creando activity per ExifTool... - evangelisti_metadata_extraction.py:1514")
                        exiftool_version = exif_metadata.get("ExifTool Version")
                        if exiftool_version and current_exiftool_version != exiftool_version:
                            current_exiftool_version = exiftool_version
                            print(f"[EXIFTOOLVERSION] Impostata versione globale: {exiftool_version} - evangelisti_metadata_extraction.py:1518")
                        
                        try:
                            exif_activity = create_extraction_activity(
                                "ExifTool", inst_uri, exif_ds, exif_graph_uri, extraction_time_ms=exif_extraction_time
                            )
                            print(f"[DEBUG ExifTool Processing]   Activity creata: {exif_activity} - evangelisti_metadata_extraction.py:1524")
                            total_exif_activities_created += 1
                            
                            metadata_instances = process_metadata_with_unique_instances(
                                exif_metadata, exif_ds, exif_graph_uri, exif_activity, inst_uri, inst_uri
                            )
                            print(f"[DEBUG ExifTool Processing]   Istanze metadati create: {len(metadata_instances)} - evangelisti_metadata_extraction.py:1530")
                            total_exif_metadata_extracted += len(exif_metadata)
                            
                            # Verifica che siano state aggiunte triple al dataset
                            current_exif_ds_size = len(exif_ds)
                            print(f"[DEBUG ExifTool Processing]   Dimensione corrente exif_ds: {current_exif_ds_size} triple - evangelisti_metadata_extraction.py:1535")
                            
                        except Exception as activity_error:
                            print(f"[ERRORE ExifTool Processing] Errore nella creazione activity/processing: {activity_error} - evangelisti_metadata_extraction.py:1538")
                            import traceback
                            traceback.print_exc()
                    else:
                        print(f"[DEBUG ExifTool Processing]   Nessun metadato per questo file - evangelisti_metadata_extraction.py:1542")
                else:
                    print(f"[DEBUG ExifTool Processing] File non trovato nei risultati batch: {os.path.basename(full_path)} - evangelisti_metadata_extraction.py:1544")
                    print(f"[DEBUG ExifTool Processing] Chiavi disponibili in exif_results: {list(exif_results.keys())[:3] if exif_results else 'NESSUNA'} - evangelisti_metadata_extraction.py:1545")

                # 🆕 NUOVO: 4. Estrazione DROID (usa risultati batch)
                if ENABLE_DROID:
                    print(f"[DEBUG ITEM] Checking DROID results for: {full_path} - evangelisti_metadata_extraction.py:1549")
                    print(f"[DEBUG ITEM] File path in droid_results? {full_path in droid_results} - evangelisti_metadata_extraction.py:1550")
                    
                    if full_path in droid_results:
                        droid_metadata, droid_extraction_time = droid_results[full_path]
                        print(f"[DEBUG DROID Processing] File: {os.path.basename(full_path)} - evangelisti_metadata_extraction.py:1554")
                        print(f"[DEBUG DROID Processing]   Metadati: {len(droid_metadata)} campi - evangelisti_metadata_extraction.py:1555")
                        print(f"[DEBUG DROID Processing]   Tempo: {droid_extraction_time:.2f}ms - evangelisti_metadata_extraction.py:1556")
                        
                        if droid_metadata:
                            print(f"[DEBUG DROID Processing]   Campi: {list(droid_metadata.keys())} - evangelisti_metadata_extraction.py:1559")
                            print(f"[DEBUG DROID Processing]   Creando activity per DROID... - evangelisti_metadata_extraction.py:1560")
                            
                            try:
                                droid_activity = create_extraction_activity(
                                    "DROID", inst_uri, droid_ds, droid_graph_uri, extraction_time_ms=droid_extraction_time
                                )
                                print(f"[DEBUG DROID Processing]   Activity creata: {droid_activity} - evangelisti_metadata_extraction.py:1566")
                                total_droid_activities_created += 1
                                
                                metadata_instances = process_metadata_with_unique_instances(
                                    droid_metadata, droid_ds, droid_graph_uri, droid_activity, inst_uri, inst_uri
                                )
                                print(f"[DEBUG DROID Processing]   Istanze metadati create: {len(metadata_instances)} - evangelisti_metadata_extraction.py:1572")
                                total_droid_metadata_extracted += len(droid_metadata)
                                
                                # Verifica che siano state aggiunte triple al dataset
                                current_droid_ds_size = len(droid_ds)
                                print(f"[DEBUG DROID Processing]   Dimensione corrente droid_ds: {current_droid_ds_size} triple - evangelisti_metadata_extraction.py:1577")
                                
                            except Exception as activity_error:
                                print(f"[ERRORE DROID Processing] Errore nella creazione activity/processing: {activity_error} - evangelisti_metadata_extraction.py:1580")
                                import traceback
                                traceback.print_exc()
                        else:
                            print(f"[DEBUG DROID Processing]   Nessun metadato per questo file - evangelisti_metadata_extraction.py:1584")
                    else:
                        print(f"[DEBUG DROID Processing] File non trovato nei risultati batch: {os.path.basename(full_path)} - evangelisti_metadata_extraction.py:1586")
                        print(f"[DEBUG DROID Processing] Chiavi disponibili in droid_results: {list(droid_results.keys())[:3] if droid_results else 'NESSUNA'} - evangelisti_metadata_extraction.py:1587")

            # Incrementa contatore e controlla se è il momento di persistere
            total_instantiations_processed += 1
            persist_datasets_if_needed()

        print(f"[INFO] Processati {i + len(chunk)} / {len(instantiations_data)} instantiation - evangelisti_metadata_extraction.py:1593")
    
    # 🆕 CLEANUP FINALE FILE TEMPORANEI
    print(f"[CLEANUP] Pulizia finale file temporanei Tika...")
    final_cleaned = cleanup_tika_temp_files()
    print(f"[CLEANUP] File temporanei rimossi al termine: {final_cleaned}")
    
    print(f"\n[DEBUG SUMMARY ExifTool] - evangelisti_metadata_extraction.py:1595")
    print(f"File regolari trovati: {total_exif_files_found} - evangelisti_metadata_extraction.py:1596")
    print(f"Campi metadati estratti totali: {total_exif_metadata_extracted} - evangelisti_metadata_extraction.py:1597")
    print(f"Activity ExifTool create: {total_exif_activities_created} - evangelisti_metadata_extraction.py:1598")
    print(f"Dimensione finale exif_ds: {len(exif_ds)} triple - evangelisti_metadata_extraction.py:1599")
    
    # 🆕 NUOVO: Summary DROID
    if ENABLE_DROID:
        print(f"\n[DEBUG SUMMARY DROID] - evangelisti_metadata_extraction.py:1603")
        print(f"File regolari trovati: {total_droid_files_found} - evangelisti_metadata_extraction.py:1604")
        print(f"Campi metadati estratti totali: {total_droid_metadata_extracted} - evangelisti_metadata_extraction.py:1605")
        print(f"Activity DROID create: {total_droid_activities_created} - evangelisti_metadata_extraction.py:1606")
        print(f"Dimensione finale droid_ds: {len(droid_ds)} triple - evangelisti_metadata_extraction.py:1607")
    
    # 🆕 SUMMARY TIKA CON MONITORING
    print(f"\n[DEBUG SUMMARY TIKA] - evangelisti_metadata_extraction.py:1608")
    print(f"File Tika processati: {total_tika_processed} - evangelisti_metadata_extraction.py:1609")
    print(f"File temporanei puliti al termine: {final_cleaned} - evangelisti_metadata_extraction.py:1610")
    
    print(f"🆕 LABEL INSTANTIATION AGGIUNTE: {total_labels_added} - evangelisti_metadata_extraction.py:1609")

    # Serializzazione finale per i dati rimanenti
    datasets_to_serialize = [
        (fs_ds, OUTPUT_NQUADS_FS, "FileSystem"),
        (tika_ds, OUTPUT_NQUADS_TIKA, "Tika"),
        (exif_ds, OUTPUT_NQUADS_EXIF, "ExifTool")
    ]
    
    if ENABLE_DROID:
        datasets_to_serialize.append((droid_ds, OUTPUT_NQUADS_DROID, "DROID"))
    
    if any(len(ds) > 0 for ds, _, _ in datasets_to_serialize):
        print(f"\n[PERSISTENCE] === SERIALIZZAZIONE FINALE === - evangelisti_metadata_extraction.py:1622")
        datasets_info = ", ".join([f"{name}: {len(ds)}" for ds, _, name in datasets_to_serialize])
        print(f"Dati rimanenti → {datasets_info} triple - evangelisti_metadata_extraction.py:1624")
        
        fs_ds = serialize_dataset_incremental(fs_ds, OUTPUT_NQUADS_FS, "FileSystem")
        tika_ds = serialize_dataset_incremental(tika_ds, OUTPUT_NQUADS_TIKA, "Tika")
        exif_ds = serialize_dataset_incremental(exif_ds, OUTPUT_NQUADS_EXIF, "ExifTool")
        
        if ENABLE_DROID:
            droid_ds = serialize_dataset_incremental(droid_ds, OUTPUT_NQUADS_DROID, "DROID")
    
    # 🆕 MONITORAGGIO FINALE SPAZIO DISCO
    print(f"\n[FINAL] Verifica finale spazio disco...")
    monitor_temp_space_enhanced()
    print(f"[FINAL] Elaborazione completata con monitoring disco attivo.")
# === FUNZIONI URI STRUTTURATI CON PERSISTENZA JSON ===

def get_or_create_metadata_type_uri(field, context_info=None):
    """Genera URI strutturati per metadata types con persistenza JSON"""
    global software_counters
    
    field_normalized = normalize_field_name(field)
    software_source = determine_software_source(context_info)
    cache_key = (field_normalized, software_source)
    
    if cache_key in global_metadata_type_map:
        return global_metadata_type_map[cache_key]
    
    # Incrementa contatore specifico per software
    software_counters[software_source] += 1
    counter = software_counters[software_source]
    counter_str = f"{counter:04d}"  # Zero-padding
    
    # Usa BASE_URIS strutturato
    base_uri = BASE_URIS[software_source]
    type_uri = URIRef(f"{base_uri}{counter_str}")
    
    global_metadata_type_map[cache_key] = type_uri
    
    print(f"[TYPENEW] {field} ({software_source}) > {counter_str} - evangelisti_metadata_extraction.py:1657")
    
    # Salva periodicamente ogni 50 nuovi metadata types
    if sum(software_counters.values()) % 50 == 0:
        save_counters_to_json()
    
    return type_uri

def get_or_create_metadata_instance_uri(field, value, instantiation_uri):
    """Genera URI specifici per instantiation"""
    global instantiation_counters, instantiation_maps
    
    inst_uri_str = str(instantiation_uri)
    inst_identifier = extract_instantiation_identifier(inst_uri_str)
    
    # Inizializza strutture se necessario
    if inst_identifier not in instantiation_maps:
        instantiation_maps[inst_identifier] = {}
        instantiation_counters[inst_identifier] = 0
    
    field_normalized = normalize_field_name(field)
    inst_key = (field_normalized, str(value))
    
    # Controlla se già esiste per questa instantiation
    if inst_key in instantiation_maps[inst_identifier]:
        return instantiation_maps[inst_identifier][inst_key]
    
    # Crea nuovo URI
    instantiation_counters[inst_identifier] += 1
    counter = instantiation_counters[inst_identifier]
    counter_str = f"{counter:04d}"
    
    # URI specifico per instantiation
    new_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/{inst_identifier}_tm{counter_str}")
    
    # Memorizza in entrambe le strutture
    instantiation_maps[inst_identifier][inst_key] = new_uri
    
    return new_uri

def extract_instantiation_and_tool_from_activity_uri(activity_uri):
    """Estrae instantiation identifier e tool abbreviation dall'URI activity"""
    uri_str = str(activity_uri)
    
    # Pattern per estrarre componenti dall'activity URI
    pattern = r'ArchivioEvangelisti/(.+)_([^_]+)_metaextr'
    match = re.search(pattern, uri_str)
    
    if match:
        inst_identifier = match.group(1)
        tool_abbrev = match.group(2)
        return inst_identifier, tool_abbrev
    else:
        print(f"[WARNING] Cannot parse activity URI: {uri_str} - evangelisti_metadata_extraction.py:1710")
        return "unknown_inst", "unknown_tool"

def create_exception(field_name, exception_value, activity_uri, file_uri, dataset, graph_uri, tool_name="Unknown"):
    """Create exception entity with instantiation-specific URI e persistenza JSON"""
    global exception_counters
    
    # Estrai instantiation e tool dall'activity URI
    inst_identifier, tool_abbrev = extract_instantiation_and_tool_from_activity_uri(activity_uri)
    
    # Chiave per contatore specifico instantiation+tool
    counter_key = (inst_identifier, tool_abbrev)
    
    # Inizializza contatore se necessario
    if counter_key not in exception_counters:
        exception_counters[counter_key] = 0
    
    # Incrementa contatore
    exception_counters[counter_key] += 1
    counter = exception_counters[counter_key]
    
    # Crea URI specifico per instantiation+tool
    counter_str = f"{counter:04d}"
    exception_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/{inst_identifier}_{tool_abbrev}_exc{counter_str}")
    
    print(f"[EXCNEW] {inst_identifier} {tool_abbrev}: {field_name} > exc{counter_str} - evangelisti_metadata_extraction.py:1735")
    
    dataset.add((exception_uri, RDF.type, bodi.Exception, graph_uri))
    
    if isinstance(exception_value, list):
        for msg in exception_value:
            dataset.add((exception_uri, bodi.exceptionMessage, Literal(str(msg)), graph_uri))
    else:
        dataset.add((exception_uri, bodi.exceptionMessage, Literal(str(exception_value)), graph_uri))
    
    dataset.add((exception_uri, bodi.generatedBy, activity_uri, graph_uri))
    dataset.add((activity_uri, bodi.hasGenerated, exception_uri, graph_uri))
    
    # Salva periodicamente ogni 25 nuove eccezioni
    total_exceptions = sum(exception_counters.values())
    if total_exceptions % 25 == 0:
        save_counters_to_json()
    
    return exception_uri

def create_software_stack(tool_name, tika_parsed_by=None, dataset=None, graph_uri=None):
    """Software stack con URI strutturato e cache completa - VERSIONE CON CACHE SEPARATA"""
    global software_stack_counter, software_stack_cache
    
    components = [tool_name]
    if tika_parsed_by:
        normalized_parsers = []
        for item in tika_parsed_by:
            if isinstance(item, list):
                normalized_parsers.extend([str(subitem) for subitem in item])
            else:
                normalized_parsers.append(str(item))
        components.extend(sorted(set(normalized_parsers)))
    
    sorted_components = sorted(set(components))
    cache_key = "|".join(sorted_components)
    
    # Controlla cache SoftwareStack
    if cache_key in software_stack_cache:
        stack_uri = software_stack_cache[cache_key]
        print(f"[STACKCACHE] Using cached: {cache_key} > {stack_uri} - evangelisti_metadata_extraction.py:1775")
        
        if dataset and graph_uri:
            # Triple basilari SoftwareStack
            dataset.add((stack_uri, RDF.type, bodi.SoftwareStack, graph_uri))
            dataset.add((stack_uri, RDFS.label, Literal(f"Software Stack: {', '.join(sorted_components)}"), graph_uri))
            
            # 🆕 CREA SEMPRE Software e Components (usa le loro cache)
            _create_software_and_components_with_cache(stack_uri, sorted_components, dataset, graph_uri)
            
        return stack_uri
    
    # Nuovo SoftwareStack
    software_stack_counter += 1
    counter_str = f"{software_stack_counter:04d}"
    
    base_uri = BASE_URIS['software_stack']
    stack_uri = URIRef(f"{base_uri}{counter_str}")
    software_stack_cache[cache_key] = stack_uri
    
    print(f"[STACKNEW] {', '.join(sorted_components)} > {counter_str} - evangelisti_metadata_extraction.py:1795")
    
    if dataset and graph_uri:
        # Triple basilari
        dataset.add((stack_uri, RDF.type, bodi.SoftwareStack, graph_uri))
        dataset.add((stack_uri, RDFS.label, Literal(f"Software Stack: {', '.join(sorted_components)}"), graph_uri))
        
        # Software e Components
        _create_software_and_components_with_cache(stack_uri, sorted_components, dataset, graph_uri)
        
        if software_stack_counter % 10 == 0:
            save_counters_to_json()
    
    return stack_uri

def get_or_create_software_entity(software_name, dataset=None, graph_uri=None, exiftool_version=None):
    """Crea o recupera Software entity con URI strutturato e cache - CON VERSIONE EXIFTOOL E DROID GARANTITA E DOCUMENTAZIONE"""
    global software_counter, software_entities_cache, current_exiftool_version, current_droid_version
    
    # Normalizza nome per cache key
    cache_key = normalize_software_name(software_name)
    
    # 🆕 DEBUG: Log della richiesta di creazione/recupero software
    print(f"[SOFTWAREREQUEST] Software richiesto: '{software_name}' (cache_key: '{cache_key}') - evangelisti_metadata_extraction.py:1818")
    if "exiftool" in software_name.lower():
        print(f"[SOFTWAREREQUEST] ExifTool detected → current_exiftool_version: {current_exiftool_version} - evangelisti_metadata_extraction.py:1820")
        print(f"[SOFTWAREREQUEST] Graph URI: {graph_uri} - evangelisti_metadata_extraction.py:1821")
    elif "droid" in software_name.lower():
        print(f"[SOFTWAREREQUEST] DROID detected → current_droid_version: {current_droid_version} - evangelisti_metadata_extraction.py:1823")
        print(f"[SOFTWAREREQUEST] Graph URI: {graph_uri} - evangelisti_metadata_extraction.py:1824")
    
    # Controlla cache
    if cache_key in software_entities_cache:
        software_uri = software_entities_cache[cache_key]
        print(f"[SOFTWARECACHE] Using cached: {software_name} > {software_uri} - evangelisti_metadata_extraction.py:1829")
        
        # 🔧 IMPORTANTE: Aggiungi sempre le triple basilari (per serializzazione incrementale)
        if dataset and graph_uri:
            dataset.add((software_uri, RDF.type, bodi.Software, graph_uri))
            dataset.add((software_uri, RDFS.label, Literal(software_name), graph_uri))
            
            # 🆕 AGGIUNTA DOCUMENTAZIONE SOFTWARE (ANCHE PER CACHE)
            documentation_url = get_documentation_url(software_name)
            if documentation_url:
                # Controlla se documentazione già presente (evita duplicati)
                if documentation_url:
                    dataset.add((software_uri, bodi.hasDocumentation, URIRef(documentation_url), graph_uri))
                    print(f"[SOFTWARECACHE] 📚 Documentazione aggiunta: {documentation_url}")
                            
            # 🆕 GESTIONE SPECIALE VERSIONE EXIFTOOL
            if "exiftool" in software_name.lower():
                print(f"[EXIFTOOLCACHE] Gestendo ExifTool cached entity... - evangelisti_metadata_extraction.py:1847")
                
                # Usa la versione passata come parametro o quella globale
                version_to_use = exiftool_version or current_exiftool_version
                
                if version_to_use:
                    # Controlla se versione già presente (evita duplicati)
                    existing_versions = list(dataset.objects(software_uri, premis.version))
                    version_already_exists = any(str(v) == str(version_to_use) for v in existing_versions)
                    
                    if not version_already_exists:
                        dataset.add((software_uri, premis.version, 
                                   Literal(str(version_to_use)), graph_uri))
                        print(f"[EXIFTOOLCACHE] ✅ Versione {version_to_use} aggiunta a cached entity {software_uri} - evangelisti_metadata_extraction.py:1860")
                    else:
                        print(f"[EXIFTOOLCACHE] ℹ️ Versione {version_to_use} già presente per {software_uri} - evangelisti_metadata_extraction.py:1862")
                    
                    # Aggiorna versione globale se necessario
                    if not current_exiftool_version and version_to_use:
                        current_exiftool_version = str(version_to_use)
                        print(f"[EXIFTOOLCACHE] Aggiornata versione globale: {current_exiftool_version} - evangelisti_metadata_extraction.py:1867")
                        
    
                else:
                    print(f"[EXIFTOOLCACHE] ⚠️ Nessuna versione disponibile per ExifTool cached entity - evangelisti_metadata_extraction.py:1871")
            
            # 🆕 GESTIONE VERSIONE DROID
            elif "droid" in software_name.lower():
                print(f"[DROIDCACHE] Gestendo DROID cached entity... - evangelisti_metadata_extraction.py:1875")
                
                if current_droid_version:
                    existing_versions = list(dataset.objects(software_uri, premis.version))
                    version_already_exists = any(str(v) == str(current_droid_version) for v in existing_versions)
                    
                    if not version_already_exists:
                        dataset.add((software_uri, premis.version, 
                                   Literal(str(current_droid_version)), graph_uri))
                        print(f"[DROIDCACHE] ✅ Versione {current_droid_version} aggiunta a cached entity {software_uri} - evangelisti_metadata_extraction.py:1884")
                    else:
                        print(f"[DROIDCACHE] ℹ️ Versione {current_droid_version} già presente per {software_uri} - evangelisti_metadata_extraction.py:1886")
                else:
                    print(f"[DROIDCACHE] ⚠️ Nessuna versione disponibile per DROID cached entity - evangelisti_metadata_extraction.py:1888")
            
            # 🆕 GESTIONE ALTRE VERSIONI SOFTWARE
            elif "apache tika" in software_name.lower() and current_tika_version:
                existing_versions = list(dataset.objects(software_uri, premis.version))
                if not any(str(v) == str(current_tika_version) for v in existing_versions):
                    dataset.add((software_uri, premis.version, 
                               Literal(str(current_tika_version)), graph_uri))
                    print(f"[SOFTWARECACHE] ✅ Versione Tika {current_tika_version} aggiunta a cached entity - evangelisti_metadata_extraction.py:1896")
                    
            elif "python" in software_name.lower():
                python_version = platform.python_version()
                existing_versions = list(dataset.objects(software_uri, premis.version))
                if not any(str(v) == python_version for v in existing_versions):
                    dataset.add((software_uri, premis.version, 
                               Literal(python_version), graph_uri))
                    print(f"[SOFTWARECACHE] ✅ Versione Python {python_version} aggiunta a cached entity - evangelisti_metadata_extraction.py:1904")
        
        return software_uri
    
    # 🆕 CREA NUOVO SOFTWARE ENTITY
    software_counter += 1
    counter_str = f"{software_counter:04d}"
    
    software_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/software_{counter_str}")
    software_entities_cache[cache_key] = software_uri
    
    print(f"[SOFTWARENEW] {software_name} > software_{counter_str} - evangelisti_metadata_extraction.py:1915")
    
    # Crea triple basilari
    if dataset and graph_uri:
        dataset.add((software_uri, RDF.type, bodi.Software, graph_uri))
        dataset.add((software_uri, RDFS.label, Literal(software_name), graph_uri))
        
        # 🆕 AGGIUNTA DOCUMENTAZIONE SOFTWARE
        documentation_url = get_documentation_url(software_name)
        if documentation_url:
            dataset.add((software_uri, bodi.hasDocumentation, URIRef(documentation_url), graph_uri))
            print(f"[SOFTWARENEW] 📚 Documentazione aggiunta: {documentation_url} - evangelisti_metadata_extraction.py:1926")
        
        # 🆕 GESTIONE SPECIALE NUOVA ENTITÀ EXIFTOOL
        if "exiftool" in software_name.lower():
            print(f"[EXIFTOOLNEW] Creando nuova entity ExifTool... - evangelisti_metadata_extraction.py:1930")
            
            # Usa la versione passata come parametro o quella globale
            version_to_use = exiftool_version or current_exiftool_version
            
            if version_to_use:
                dataset.add((software_uri, premis.version, 
                           Literal(str(version_to_use)), graph_uri))
                print(f"[EXIFTOOLNEW] ✅ Versione {version_to_use} aggiunta a nuova entity {software_uri} - evangelisti_metadata_extraction.py:1938")
                
                # Aggiorna versione globale se necessario
                if not current_exiftool_version:
                    current_exiftool_version = str(version_to_use)
                    print(f"[EXIFTOOLNEW] Aggiornata versione globale: {current_exiftool_version} - evangelisti_metadata_extraction.py:1943")
            else:
                print(f"[EXIFTOOLNEW] ⚠️ ATTENZIONE: Creata entity ExifTool senza versione! - evangelisti_metadata_extraction.py:1945")
                print(f"[EXIFTOOLNEW]   current_exiftool_version: {current_exiftool_version} - evangelisti_metadata_extraction.py:1946")
                print(f"[EXIFTOOLNEW]   exiftool_version param: {exiftool_version} - evangelisti_metadata_extraction.py:1947")
            
        # 🆕 GESTIONE NUOVA ENTITÀ DROID
        elif "droid" in software_name.lower():
            print(f"[DROIDNEW] Creando nuova entity DROID... - evangelisti_metadata_extraction.py:1951")
            
            if current_droid_version:
                dataset.add((software_uri, premis.version, 
                           Literal(str(current_droid_version)), graph_uri))
                print(f"[DROIDNEW] ✅ Versione {current_droid_version} aggiunta a nuova entity {software_uri} - evangelisti_metadata_extraction.py:1956")
            else:
                print(f"[DROIDNEW] ⚠️ ATTENZIONE: Creata entity DROID senza versione! - evangelisti_metadata_extraction.py:1958")
                print(f"[DROIDNEW]   current_droid_version: {current_droid_version} - evangelisti_metadata_extraction.py:1959")

        
        # 🆕 GESTIONE ALTRE NUOVE ENTITÀ SOFTWARE  
        elif "apache tika" in software_name.lower():
            if current_tika_version:
                dataset.add((software_uri, premis.version, 
                           Literal(str(current_tika_version)), graph_uri))
                print(f"[SOFTWARENEW] ✅ Versione Tika {current_tika_version} aggiunta a nuova entity - evangelisti_metadata_extraction.py:1967")
                
        elif "python" in software_name.lower():
            python_version = platform.python_version()
            dataset.add((software_uri, premis.version, 
                       Literal(python_version), graph_uri))
            print(f"[SOFTWARENEW] ✅ Versione Python {python_version} aggiunta a nuova entity - evangelisti_metadata_extraction.py:1973")
        
        # Salva periodicamente (meno frequente per evitare overhead)
        if software_counter % 5 == 0:
            save_counters_to_json()
            print(f"[SOFTWARENEW] Checkpoint salvataggio contatori: {software_counter} software creati - evangelisti_metadata_extraction.py:1978")
    
    return software_uri

def extract_exif_metadata(file_path, instantiation_uri, relative_path):
    """Wrapper per compatibilità con codice esistente"""
    try:
        start_time = time.time()
        
        result = subprocess.run(
            ['exiftool', '-json', file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
        end_time = time.time()
        extraction_time_ms = (end_time - start_time) * 1000
        
        metadata_list = json.loads(result.stdout)
        if not metadata_list:
            return {}, extraction_time_ms
        metadata = metadata_list[0]
        filtered_metadata = {k: v for k, v in metadata.items() if k != "SourceFile" and v not in ("", [], None)}
        return filtered_metadata, extraction_time_ms
    except Exception as e:
        print(f"[ERRORE] Errore nell'estrazione dei metadati ExifTool per {file_path}: {e} - evangelisti_metadata_extraction.py:2003")
        return {}, 0

# === FUNZIONI PER SERIALIZZAZIONE INCREMENTALE ===

def initialize_output_files():
    """Inizializza i file di output puliti e scrive gli header se necessario"""
    print("[INFO] Inizializzazione file di output... - evangelisti_metadata_extraction.py:2010")
    
    # 🆕 AGGIUNTO DROID
    output_files = [OUTPUT_NQUADS_FS, OUTPUT_NQUADS_TIKA, OUTPUT_NQUADS_EXIF]
    if ENABLE_DROID:
        output_files.append(OUTPUT_NQUADS_DROID)
    
    # Pulisci i file esistenti
    for file_path in output_files:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"[INFO] File esistente rimosso: {file_path} - evangelisti_metadata_extraction.py:2021")
        
        # Crea file vuoto
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"# RDF N-Quads file generato il {datetime.now().isoformat()}\n")
            f.write(f"# File: {file_path}\n")
            f.write(f"# Serializzazione incrementale ogni {PERSISTENCE_INTERVAL} instantiation\n\n")

def validate_no_orphan_types_in_datasets():
    """Valida che non ci siano TechnicalMetadataType orfani nei dataset correnti (in memoria)"""
    global fs_ds, tika_ds, exif_ds, droid_ds  # 🆕 DROID
    
    print(f"\n[VALIDATIONMEMORY] Controllo TechnicalMetadataType orfani nei dataset in memoria... - evangelisti_metadata_extraction.py:2033")
    
    total_orphans = 0
    
    dataset_configs = [(fs_ds, "FS", fs_graph_uri), (tika_ds, "Tika", tika_graph_uri), (exif_ds, "ExifTool", exif_graph_uri)]
    if ENABLE_DROID and droid_ds is not None:  # 🆕 NUOVO
        dataset_configs.append((droid_ds, "DROID", droid_graph_uri))
    
    for ds, name, graph_uri in dataset_configs:
        if ds is None or len(ds) == 0:
            print(f"[VALIDATIONMEMORY] Dataset {name}: vuoto o None - evangelisti_metadata_extraction.py:2043")
            continue
        
        types_found = set()
        types_with_instances = set()
        
        for s, p, o in ds:
            if p == RDF.type and o == bodi.TechnicalMetadataType:
                types_found.add(s)
            elif p == bodi.hasTechnicalMetadataType:
                types_with_instances.add(o)
        
        orphans_in_dataset = types_found - types_with_instances
        
        if orphans_in_dataset:
            print(f"[VALIDATIONMEMORY] ❌ Dataset {name}: {len(orphans_in_dataset)} tipi orfani - evangelisti_metadata_extraction.py:2058")
            total_orphans += len(orphans_in_dataset)
            
            # Mostra alcuni esempi
            for i, orphan in enumerate(list(orphans_in_dataset)[:3]):
                label = "No label"
                for _, p, o in ds:
                    if _ == orphan and p == RDFS.label:
                        label = str(o)
                        break
                print(f"[VALIDATIONMEMORY]   Orfano {i+1}: {label} - evangelisti_metadata_extraction.py:2068")
        else:
            print(f"[VALIDATIONMEMORY] ✅ Dataset {name}: nessun tipo orfano ({len(types_found)} tipi totali) - evangelisti_metadata_extraction.py:2070")
    
    if total_orphans == 0:
        print(f"[VALIDATIONMEMORY] ✅ SUCCESSO: Nessun TechnicalMetadataType orfano nei dataset in memoria - evangelisti_metadata_extraction.py:2073")
    else:
        print(f"[VALIDATIONMEMORY] ❌ TROVATI {total_orphans} TechnicalMetadataType orfani nei dataset in memoria - evangelisti_metadata_extraction.py:2075")
    
    return total_orphans


def get_or_create_software_component(component_name, dataset=None, graph_uri=None):
    """Crea o recupera SoftwareComponent con URI strutturato e cache - CON DOCUMENTAZIONE SOLO PER OS"""
    global software_component_counter, software_components_cache
    
    # Normalizza nome per cache key
    cache_key = normalize_software_name(component_name)
    
    # Controlla cache
    if cache_key in software_components_cache:
        component_uri = software_components_cache[cache_key]
        print(f"[COMPONENTCACHE] Using cached: {component_name} > {component_uri} - evangelisti_metadata_extraction.py:2090")
        
        # 🔧 IMPORTANTE: Aggiungi sempre le triple basilari
        if dataset and graph_uri:
            dataset.add((component_uri, RDF.type, bodi.SoftwareComponent, graph_uri))
            dataset.add((component_uri, RDFS.label, Literal(component_name), graph_uri))
            
            # 🆕 AGGIUNTA DOCUMENTAZIONE SOLO PER LIBRERIA OS PYTHON
            if "python os" in component_name.lower() or component_name.lower() == "python os - miscellaneous operating system interfaces module":
                documentation_url = get_documentation_url(component_name)
                if documentation_url:
                    # Controlla se documentazione già presente (evita duplicati)
                    existing_docs = list(dataset.objects(component_uri, bodi.hasDocumentation))
                    if not any(str(doc) == documentation_url for doc in existing_docs):
                        dataset.add((component_uri, bodi.hasDocumentation, URIRef(documentation_url), graph_uri))
                        print(f"[COMPONENTCACHE] 📚 Documentazione OS aggiunta a cached entity: {documentation_url} - evangelisti_metadata_extraction.py:2105")
        
        return component_uri
    
    # Crea nuovo SoftwareComponent
    software_component_counter += 1
    counter_str = f"{software_component_counter:04d}"
    
    component_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/software_comp_{counter_str}")
    software_components_cache[cache_key] = component_uri
    
    print(f"[COMPONENTNEW] {component_name} > software_comp_{counter_str} - evangelisti_metadata_extraction.py:2116")
    
    # Crea triple
    if dataset and graph_uri:
        dataset.add((component_uri, RDF.type, bodi.SoftwareComponent, graph_uri))
        dataset.add((component_uri, RDFS.label, Literal(component_name), graph_uri))
        
        # 🆕 AGGIUNTA DOCUMENTAZIONE SOLO PER LIBRERIA OS PYTHON
        if "python os" in component_name.lower() or component_name.lower() == "python os - miscellaneous operating system interfaces module":
            documentation_url = get_documentation_url(component_name)
            if documentation_url:
                dataset.add((component_uri, bodi.hasDocumentation, URIRef(documentation_url), graph_uri))
                print(f"[COMPONENTNEW] 📚 Documentazione OS aggiunta: {documentation_url} - evangelisti_metadata_extraction.py:2128")
        
        # Salva periodicamente
        if software_component_counter % 10 == 0:
            save_counters_to_json()
    
    return component_uri

def _create_software_and_components_with_cache(stack_uri, sorted_components, dataset, graph_uri):
    """Crea Software e Components usando le cache separate - VERSIONE OTTIMIZZATA"""
    
    main_software_uri = None
    
    for component_name in sorted_components:
        if component_name.lower() == "python os - miscellaneous operating system interfaces module":
            # Questo è un SoftwareComponent
            component_uri = get_or_create_software_component(component_name, dataset, graph_uri)
            
            # Python interpreter come Software separato
            python_implementation = platform.python_implementation()
            python_version = platform.python_version()
            python_software_name = f"{python_implementation} {python_version}"
            
            python_interpreter_uri = get_or_create_software_entity(python_software_name, dataset, graph_uri)
            
            # Relazioni Python interpreter <-> OS component (solo se non esistono già)
            dataset.add((python_interpreter_uri, bodi.hasSoftwareComponent, component_uri, graph_uri))
            dataset.add((component_uri, bodi.isSoftwareComponentOf, python_interpreter_uri, graph_uri))
            
            # Relazioni Stack <-> Python interpreter
            dataset.add((stack_uri, rico.hasOrHadPart, python_interpreter_uri, graph_uri))
            dataset.add((python_interpreter_uri, rico.isOrWasPartOf, stack_uri, graph_uri))
            
        elif component_name.lower() in ["apache tika", "exiftool", "droid"]:  # 🆕 AGGIUNTO DROID
            # Software principale
            software_uri = get_or_create_software_entity(component_name, dataset, graph_uri)
            
            if component_name.lower() == "apache tika":
                main_software_uri = software_uri
            
            # Relazioni Stack <-> Software principale
            dataset.add((stack_uri, rico.hasOrHadPart, software_uri, graph_uri))
            dataset.add((software_uri, rico.isOrWasPartOf, stack_uri, graph_uri))
            
        else:
            # SoftwareComponent (parser, ecc.)
            component_uri = get_or_create_software_component(component_name, dataset, graph_uri)
            
            # Relazioni Stack <-> Component
            dataset.add((stack_uri, rico.hasOrHadPart, component_uri, graph_uri))
            dataset.add((component_uri, rico.isOrWasPartOf, stack_uri, graph_uri))
    
    # Collega i parser al software principale
    if main_software_uri:
        for component_name in sorted_components:
            if component_name.lower() not in ["apache tika", "exiftool", "droid", "python os - miscellaneous operating system interfaces module"]:  # 🆕 AGGIUNTO DROID
                parser_uri = get_or_create_software_component(component_name, dataset, graph_uri)
                
                # Relazioni Software <-> Parser (solo se non esistono già)
                dataset.add((main_software_uri, bodi.hasSoftwareComponent, parser_uri, graph_uri))
                dataset.add((parser_uri, bodi.isSoftwareComponentOf, main_software_uri, graph_uri))

@timing_decorator
def extract_tika_metadata(file_path, inst_uri=None, relative_path=None):
    """VERSIONE CORRETTA - Senza memory mapping che causa problemi con Tika"""
    metadata = {}
    
    # Salva timestamp originali prima della lettura
    try:
        original_stat = os.stat(file_path)
        original_atime = original_stat.st_atime
        original_mtime = original_stat.st_mtime
    except:
        original_atime = original_mtime = None
    
    try:
        # USA LA VERSIONE ORIGINALE SENZA MMAP - più stabile
        with open(file_path, 'rb') as f:
            response = requests.put(
                'http://localhost:9998/meta',
                data=f,  # File object diretto, NO mmap
                headers={'Accept': 'application/json'},
                timeout=30  # RIDOTTO da 1000 a 30 secondi
            )
        
        if response.status_code == 200:
            metadata = response.json()
        else:
            print(f"[TIKA ERROR] ({response.status_code}) {file_path}")
            
    except Exception as e:
        print(f"[TIKA EXCEPTION] {file_path} ({inst_uri}): {e}")
    
    finally:
        # Ripristina timestamp solo se necessario
        if original_atime is not None and original_mtime is not None:
            try:
                # Verifica se timestamp è cambiato prima di ripristinare
                current_stat = os.stat(file_path)
                if abs(current_stat.st_mtime - original_mtime) > 1:  # Soglia di 1 secondo
                    os.utime(file_path, (original_atime, original_mtime))
            except:
                pass  # Silenzioso se non riesce
    
    return metadata

    
def extract_exif_metadata_batch(file_paths):
    """Estrazione batch ottimizzata per ExifTool - CON PRESERVAZIONE TIMESTAMP"""
    if not file_paths:
        print("[DEBUG ExifTool] Nessun file da processare - evangelisti_metadata_extraction.py:2248")
        return {}
    
    print(f"[DEBUG ExifTool] Processando {len(file_paths)} file in batch con preservazione timestamp - evangelisti_metadata_extraction.py:2251")
    print(f"[DEBUG ExifTool] Primi 3 file: {[os.path.basename(f) for f in file_paths[:3]]} - evangelisti_metadata_extraction.py:2252")
    
    # === CONTROLLI PRELIMINARI ===
    
    # 1. Verifica che ExifTool sia installato
    try:
        test_result = subprocess.run(['exiftool', '-ver'], 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE,
                                   timeout=10)
        if test_result.returncode != 0:
            print("[ERRORE ExifTool] ExifTool non trovato! Installare con: brew install exiftool - evangelisti_metadata_extraction.py:2263")
            return {path: ({}, 0) for path in file_paths}
        else:
            exiftool_version = test_result.stdout.decode().strip()
            print(f"[DEBUG ExifTool] ExifTool versione: {exiftool_version} - evangelisti_metadata_extraction.py:2267")
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"[ERRORE ExifTool] Impossibile verificare ExifTool: {e} - evangelisti_metadata_extraction.py:2269")
        return {path: ({}, 0) for path in file_paths}
    
    # 2. Verifica esistenza e leggibilità file
    valid_files = []
    invalid_files = []
    
    for file_path in file_paths:
        if not os.path.exists(file_path):
            invalid_files.append(f"File non esistente: {file_path}")
            continue
            
        if not os.path.isfile(file_path):
            invalid_files.append(f"Non è un file regolare: {file_path}")
            continue
            
        if os.path.islink(file_path):
            invalid_files.append(f"È un link simbolico: {file_path}")
            continue
            
        try:
            # Test di lettura rapido
            with open(file_path, 'rb') as f:
                f.read(1)
            valid_files.append(file_path)
        except (PermissionError, OSError) as e:
            invalid_files.append(f"File non leggibile {file_path}: {e}")
    
    if invalid_files:
        print(f"[WARNING ExifTool] File non validi: {len(invalid_files)} - evangelisti_metadata_extraction.py:2298")
        for invalid in invalid_files[:5]:  # Mostra solo i primi 5
            print(f"{invalid} - evangelisti_metadata_extraction.py:2300")
        if len(invalid_files) > 5:
            extra_files = len(invalid_files) - 5
            print(f"... e altri {extra_files} - evangelisti_metadata_extraction.py:2303")

    
    if not valid_files:
        print("[WARNING ExifTool] Nessun file valido da processare - evangelisti_metadata_extraction.py:2307")
        return {path: ({}, 0) for path in file_paths}
    
    print(f"[DEBUG ExifTool] File validi da processare: {len(valid_files)}/{len(file_paths)} - evangelisti_metadata_extraction.py:2310")
    
    # === ESECUZIONE EXIFTOOL CON PRESERVAZIONE TIMESTAMP ===
    
    # 1. BACKUP TIMESTAMP ORIGINALI
    print(f"[TIMESTAMPBACKUP] Salvando timestamp originali per {len(valid_files)} file... - evangelisti_metadata_extraction.py:2315")
    original_timestamps = {}
    
    for file_path in valid_files:
        try:
            stat_info = os.stat(file_path)
            original_timestamps[file_path] = {
                'atime': stat_info.st_atime,
                'mtime': stat_info.st_mtime,
                'ctime': stat_info.st_ctime
            }
        except Exception as e:
            print(f"[WARNING] Impossibile salvare timestamp per {file_path}: {e} - evangelisti_metadata_extraction.py:2327")
    
    try:
        start_time = time.time()
        
        # Normalizza i path per evitare problemi di encoding/spazi
        normalized_paths = [os.path.abspath(path) for path in valid_files]
        
        # 2. COMANDO CON FLAG PRESERVE (MODIFICA PRINCIPALE)
        command = [
            'exiftool', 
            '-json', 
            '-fast', 
            '-quiet',
            '-preserve'  # ← FLAG CHIAVE per preservare timestamp
        ] + normalized_paths
        
        # Debug comando (limitato per non spammare)
        if len(normalized_paths) <= 3:
            print(f"[DEBUG ExifTool] Comando con preserve: {' '.join(command)} - evangelisti_metadata_extraction.py:2346")
        else:
            sample_cmd = ['exiftool', '-json', '-fast', '-quiet', '-preserve'] + normalized_paths[:2] + ['...']
            print(f"[DEBUG ExifTool] Comando con preserve (esempio): {' '.join(sample_cmd)} - evangelisti_metadata_extraction.py:2349")
        
        # Esegui comando con timeout appropriato
        timeout_seconds = max(30, len(valid_files) * 2)  # 2 sec per file, minimo 30
        print(f"[DEBUG ExifTool] Timeout impostato: {timeout_seconds}s - evangelisti_metadata_extraction.py:2353")
        
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
            check=False  # Non lanciare eccezione per return code != 0
        )
        
        end_time = time.time()
        total_extraction_time_ms = (end_time - start_time) * 1000
        
        print(f"[DEBUG ExifTool] Tempo totale estrazione: {total_extraction_time_ms:.2f}ms - evangelisti_metadata_extraction.py:2366")
        print(f"[DEBUG ExifTool] Return code: {result.returncode} - evangelisti_metadata_extraction.py:2367")
        
        # 3. VERIFICA E RIPRISTINO TIMESTAMP (SAFETY NET)
        altered_files = []
        restored_files = []
        
        for file_path in valid_files:
            if file_path in original_timestamps:
                try:
                    current_stat = os.stat(file_path)
                    original = original_timestamps[file_path]
                    
                    # Verifica se qualche timestamp è cambiato (tolleranza 1 secondo)
                    atime_changed = abs(current_stat.st_atime - original['atime']) > 1
                    mtime_changed = abs(current_stat.st_mtime - original['mtime']) > 1
                    
                    if atime_changed or mtime_changed:
                        altered_files.append(file_path)
                        
                        # Ripristina timestamp originali
                        os.utime(file_path, (original['atime'], original['mtime']))
                        restored_files.append(file_path)
                        
                        print(f"[TIMESTAMPRESTORE] Ripristinato: {os.path.basename(file_path)} - evangelisti_metadata_extraction.py:2390")
                        
                except Exception as e:
                    print(f"[ERROR] Errore verifica/ripristino timestamp per {file_path}: {e} - evangelisti_metadata_extraction.py:2393")
        
        # 4. REPORT TIMESTAMP
        if altered_files:
            print(f"[TIMESTAMPSAFETY] ⚠️ {len(altered_files)} file avevano timestamp alterati - evangelisti_metadata_extraction.py:2397")
            print(f"[TIMESTAMPSAFETY] ✅ {len(restored_files)} file ripristinati - evangelisti_metadata_extraction.py:2398")
        else:
            print(f"[TIMESTAMPSAFETY] ✅ Tutti i timestamp preservati correttamente - evangelisti_metadata_extraction.py:2400")
        
        # Gestisci stderr
        if result.stderr:
            stderr_text = result.stderr.decode('utf-8', errors='ignore')
            if stderr_text.strip():
                print(f"[DEBUG ExifTool] STDERR: {stderr_text[:300]}... - evangelisti_metadata_extraction.py:2406")
        
        # Verifica output
        if not result.stdout:
            print("[ERRORE ExifTool] Nessun output da ExifTool - evangelisti_metadata_extraction.py:2410")
            return {path: ({}, 0) for path in file_paths}
        
        stdout_text = result.stdout.decode('utf-8', errors='ignore')
        print(f"[DEBUG ExifTool] Output length: {len(stdout_text)} caratteri - evangelisti_metadata_extraction.py:2414")
        
        if len(stdout_text) < 10:
            print(f"[ERRORE ExifTool] Output troppo breve: '{stdout_text}' - evangelisti_metadata_extraction.py:2417")
            return {path: ({}, 0) for path in file_paths}
        
        # Debug dei primi caratteri per verificare formato JSON
        print(f"[DEBUG ExifTool] Primi 100 caratteri: {stdout_text[:100]}... - evangelisti_metadata_extraction.py:2421")
        
    except subprocess.TimeoutExpired:
        print(f"[ERRORE ExifTool] Timeout dopo {timeout_seconds}s - evangelisti_metadata_extraction.py:2424")
        return {path: ({}, 0) for path in file_paths}
    except Exception as e:
        print(f"[ERRORE ExifTool] Errore nell'esecuzione: {e} - evangelisti_metadata_extraction.py:2427")
        return {path: ({}, 0) for path in file_paths}
    
    # === PARSING JSON ===
    
    try:
        metadata_list = json.loads(stdout_text)
        if not isinstance(metadata_list, list):
            print(f"[ERRORE ExifTool] Output non è una lista JSON: {type(metadata_list)} - evangelisti_metadata_extraction.py:2435")
            return {path: ({}, 0) for path in file_paths}
            
        print(f"[DEBUG ExifTool] Parsed {len(metadata_list)} oggetti metadata - evangelisti_metadata_extraction.py:2438")
        
    except json.JSONDecodeError as je:
        print(f"[ERRORE ExifTool] JSON parsing error: {je} - evangelisti_metadata_extraction.py:2441")
        print(f"[DEBUG ExifTool] Output grezzo (primi 500 char): {stdout_text[:500]}... - evangelisti_metadata_extraction.py:2442")
        # Tenta di salvare l'output per debug
        try:
            with open('exiftool_debug_output.txt', 'w') as f:
                f.write(stdout_text)
            print("[DEBUG ExifTool] Output salvato in exiftool_debug_output.txt - evangelisti_metadata_extraction.py:2447")
        except:
            pass
        return {path: ({}, 0) for path in file_paths}
    
    # === MAPPATURA RISULTATI ===
    
    # Crea mapping da SourceFile ai metadati
    exif_by_source = {}
    processed_files = set()
    
    for i, metadata_obj in enumerate(metadata_list):
        if not isinstance(metadata_obj, dict):
            print(f"[WARNING ExifTool] Oggetto {i} non è un dict: {type(metadata_obj)} - evangelisti_metadata_extraction.py:2460")
            continue
            
        source_file = metadata_obj.get('SourceFile')
        if not source_file:
            print(f"[WARNING ExifTool] Oggetto {i} senza SourceFile: {list(metadata_obj.keys())[:5]} - evangelisti_metadata_extraction.py:2465")
            continue
        
        # Normalizza il path per il matching
        normalized_source = os.path.abspath(source_file)
        exif_by_source[normalized_source] = metadata_obj
        processed_files.add(normalized_source)
        
        # Debug per i primi file
        if i < 3:
            print(f"[DEBUG ExifTool] Oggetto {i}: {os.path.basename(source_file)} - evangelisti_metadata_extraction.py:2475")
            print(f"[DEBUG ExifTool]   Campi totali: {len(metadata_obj)} - evangelisti_metadata_extraction.py:2476")
            filtered_preview = {k: v for k, v in metadata_obj.items() 
                               if k != "SourceFile" and v not in ("", [], None)}
            print(f"[DEBUG ExifTool]   Campi filtrati: {len(filtered_preview)} - evangelisti_metadata_extraction.py:2479")
            if filtered_preview:
                sample_fields = list(filtered_preview.keys())[:5]
                print(f"[DEBUG ExifTool]   Campi esempio: {sample_fields} - evangelisti_metadata_extraction.py:2482")
    
    print(f"[DEBUG ExifTool] File con metadati ExifTool: {len(processed_files)} - evangelisti_metadata_extraction.py:2484")
    
    # === ESTRAZIONE VERSIONE EXIFTOOL ===
    
    global current_exiftool_version
    if not current_exiftool_version and processed_files:
        # Prendi il primo file con metadati per estrarre la versione
        first_file_with_metadata = next(iter(exif_by_source.values()))
        
        # Cerca il campo versione (ExifTool può usare diversi nomi)
        version_candidates = [
            "ExifTool Version",     # Nome più comune
            "ExifToolVersion",      # Variante senza spazio
            "Tool Version",         # Variante generica
            "Version",              # Fallback
            "ExifTool:Version",     # Con namespace
            "System:ExifToolVersion" # Altra variante possibile
        ]
        
        print(f"[EXIFTOOLVERSION] Cercando versione ExifTool... - evangelisti_metadata_extraction.py:2503")
        print(f"[EXIFTOOLVERSION] Candidati: {version_candidates} - evangelisti_metadata_extraction.py:2504")
        print(f"[EXIFTOOLVERSION] Campi disponibili: {list(first_file_with_metadata.keys())[:10]}... - evangelisti_metadata_extraction.py:2505")
        
        for field_name in version_candidates:
            if field_name in first_file_with_metadata:
                extracted_version = first_file_with_metadata[field_name]
                current_exiftool_version = str(extracted_version).strip()
                print(f"[EXIFTOOLVERSION] ✅ Versione trovata: '{field_name}' = '{current_exiftool_version}' - evangelisti_metadata_extraction.py:2511")
                break
        else:
            # Cerca pattern con versioni nei valori
            for k, v in first_file_with_metadata.items():
                if isinstance(v, str) and any(pattern in k.lower() for pattern in ['version', 'exif']):
                    if re.match(r'^\d+\.\d+', str(v)):  # Pattern versione numerica
                        current_exiftool_version = str(v).strip()
                        print(f"[EXIFTOOLVERSION] ✅ Versione estratta da pattern: '{k}' = '{current_exiftool_version}' - evangelisti_metadata_extraction.py:2519")
                        break
            else:
                print(f"[EXIFTOOLVERSION] ⚠️ Versione non trovata nei metadati - evangelisti_metadata_extraction.py:2522")
                # Usa la versione dal comando -ver se disponibile
                if 'exiftool_version' in locals():
                    current_exiftool_version = exiftool_version
                    print(f"[EXIFTOOLVERSION] ✅ Usata versione da comando: '{current_exiftool_version}' - evangelisti_metadata_extraction.py:2526")
    
    # === COSTRUZIONE RISULTATI FINALI ===
    
    results = {}
    per_file_time = total_extraction_time_ms / len(valid_files) if valid_files else 0
    
    files_with_metadata = 0
    total_metadata_fields = 0
    
    # CAMPI VERSIONE DA ESCLUDERE (gestiti separatamente)
    version_field_names = {
        "ExifTool Version", "ExifToolVersion", "Tool Version", "Version", 
        "ExifTool:Version", "System:ExifToolVersion"
    }
    
    for original_path in file_paths:
        normalized_path = os.path.abspath(original_path)
        
        if normalized_path in exif_by_source:
            metadata_obj = exif_by_source[normalized_path]
            
            # FILTRA METADATI ESCLUDENDO CAMPI VERSIONE
            filtered_metadata = {}
            version_fields_found = []
            
            for k, v in metadata_obj.items():
                # Escludi SourceFile
                if k == "SourceFile":
                    continue
                    
                # ESCLUDI CAMPI VERSIONE (gestiti separatamente)
                if k in version_field_names:
                    version_fields_found.append(f"{k}={v}")
                    continue
                    
                # Escludi valori vuoti
                if v in ("", [], None):
                    continue
                    
                # Gestisci stringhe vuote o solo spazi
                if isinstance(v, str) and not v.strip():
                    continue
                    
                filtered_metadata[k] = v
            
            # Debug per versioni trovate (solo per primi file)
            if version_fields_found and len(results) < 3:
                print(f"[DEBUGVERSION] {os.path.basename(original_path)}: esclusi {version_fields_found} - evangelisti_metadata_extraction.py:2574")
            
            results[original_path] = (filtered_metadata, per_file_time)
            
            if filtered_metadata:
                files_with_metadata += 1
                total_metadata_fields += len(filtered_metadata)
        else:
            # File non processato da ExifTool (potrebbe essere tipo non supportato)
            results[original_path] = ({}, 0)
    
    # === STATISTICHE FINALI ===
    
    print(f"[DEBUG ExifTool] === STATISTICHE BATCH CON PRESERVAZIONE TIMESTAMP === - evangelisti_metadata_extraction.py:2587")
    print(f"File input: {len(file_paths)} - evangelisti_metadata_extraction.py:2588")
    print(f"File validi: {len(valid_files)} - evangelisti_metadata_extraction.py:2589")
    print(f"File processati da ExifTool: {len(processed_files)} - evangelisti_metadata_extraction.py:2590")
    print(f"File con metadati non vuoti: {files_with_metadata} - evangelisti_metadata_extraction.py:2591")
    print(f"Totale campi metadati: {total_metadata_fields} - evangelisti_metadata_extraction.py:2592")
    print(f"Tempo medio per file: {per_file_time:.2f}ms - evangelisti_metadata_extraction.py:2593")
    print(f"Versione ExifTool globale: {current_exiftool_version} - evangelisti_metadata_extraction.py:2594")
    print(f"✅ TIMESTAMP PRESERVATI per tutti i file - evangelisti_metadata_extraction.py:2595")
    
    # Mostra esempi di file senza metadati (potrebbero essere normali)
    files_without_metadata = [path for path, (metadata, _) in results.items() if not metadata]
    if files_without_metadata:
        print(f"File senza metadati: {len(files_without_metadata)} - evangelisti_metadata_extraction.py:2600")
        # Mostra solo alcuni esempi
        for path in files_without_metadata[:3]:
            file_ext = os.path.splitext(path)[1].lower()
            print(f"{os.path.basename(path)} ({file_ext}) - evangelisti_metadata_extraction.py:2604")
        if len(files_without_metadata) > 3:
            extra_without = len(files_without_metadata) - 3
            print(f"... e altri {extra_without} - evangelisti_metadata_extraction.py:2607")
    
    return results

def create_extraction_activity(tool_name, instantiation_uri, dataset, graph_uri, tika_parsed_by=None, tika_metadata=None, extraction_time_ms=None, exiftool_version=None):
    """Create extraction activity with instantiation-specific URI"""
    
    # Estrai instantiation identifier
    inst_identifier = extract_instantiation_identifier(str(instantiation_uri))
    
    # Determina tool abbreviation
    tool_abbrev = get_tool_abbreviation(tool_name)
    
    # Crea URI specifico per instantiation
    activity_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/{inst_identifier}_{tool_abbrev}_metaextr")

    dataset.add((activity_uri, RDF.type, rico.Activity, graph_uri))
    dataset.add((activity_uri, RDFS.label, Literal(f"Metadata extraction using {tool_name}"), graph_uri))

    dataset.add((activity_uri, bodi.hasOrHadSupervisor, lucia_person, graph_uri))
    dataset.add((lucia_person, bodi.isOrWasSupervisorOf, activity_uri, graph_uri))

    dataset.add((activity_uri, rico.occurredAtDate, extraction_date, graph_uri))
    dataset.add((extraction_date, rico.isDateOfOccurrenceOf, activity_uri, graph_uri))

    software_stack = create_software_stack(tool_name, tika_parsed_by, dataset, graph_uri)

    dataset.add((activity_uri, rico.isOrWasPerformedBy, software_stack, graph_uri))
    dataset.add((software_stack, rico.performsOrPerformed, activity_uri, graph_uri))
    
    # Gestione extent con URI specifici per instantiation
    if tool_name.lower() in ["apache tika"] and tika_metadata and "X-TIKA:parse_time_millis" in tika_metadata:
        parse_time_value = tika_metadata["X-TIKA:parse_time_millis"]
        
        if isinstance(parse_time_value, list):
            parse_time = parse_time_value[0] if parse_time_value else "0"
        else:
            parse_time = str(parse_time_value)
        
        # URI specifico per instantiation (senza tempo nell'URI)
        extent_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/{inst_identifier}_{tool_abbrev}_pars_ext")
        extent_type_uri = URIRef(f"{BASE_URIS['extent_type']}ApacheTikaParsingTime")
        
        # Relazioni bidirezionali con extent
        dataset.add((activity_uri, rico.hasExtent, extent_uri, graph_uri))
        dataset.add((extent_uri, rico.isExtentOf, activity_uri, graph_uri))
        dataset.add((extent_uri, RDF.type, rico.Extent, graph_uri))
        dataset.add((extent_uri, RDF.value, Literal(f"{parse_time} milliseconds"), graph_uri))
        dataset.add((extent_uri, rico.hasExtentType, extent_type_uri, graph_uri))
        dataset.add((extent_type_uri, rico.isExtentTypeOf, extent_uri, graph_uri))
        
        # Usa cache per ExtentType
        if extent_type_uri not in created_extent_types:
            dataset.add((extent_type_uri, RDF.type, rico.ExtentType, graph_uri))
            dataset.add((extent_type_uri, RDFS.label, Literal("Apache Tika parsing time"), graph_uri))
            created_extent_types.add(extent_type_uri)
        
        print(f"[EXTNEW] {inst_identifier} {tool_abbrev}: parsing time > pars_ext ({parse_time}ms) - evangelisti_metadata_extraction.py:2664")

    if extraction_time_ms is not None:
        extraction_time_rounded = round(extraction_time_ms, 2)
        
        # URI specifico per instantiation (senza tempo nell'URI)
        extent_uri = URIRef(f"http://ficlit.unibo.it/ArchivioEvangelisti/{inst_identifier}_{tool_abbrev}_pars_ext")
        
        if tool_name == "ExifTool":
            extent_type_uri = URIRef(f"{BASE_URIS['extent_type']}ExifToolParsingTime")
            extent_label = "ExifTool parsing time"
        elif tool_name == "DROID":  # 🆕 NUOVO
            extent_type_uri = URIRef(f"{BASE_URIS['extent_type']}DROIDParsingTime")
            extent_label = "DROID parsing time"
        elif "Python os" in tool_name:
            extent_type_uri = URIRef(f"{BASE_URIS['extent_type']}FilesystemParsingTime") 
            extent_label = "Filesystem parsing time"
        else:
            extent_type_uri = URIRef(f"{BASE_URIS['extent_type']}ApacheTikaParsingTime")
            extent_label = f"{tool_name} parsing time"
        
        # Relazioni bidirezionali con extent
        dataset.add((activity_uri, rico.hasExtent, extent_uri, graph_uri))
        dataset.add((extent_uri, rico.isExtentOf, activity_uri, graph_uri))
        dataset.add((extent_uri, RDF.type, rico.Extent, graph_uri))
        dataset.add((extent_uri, RDF.value, Literal(f"{extraction_time_rounded} milliseconds"), graph_uri))
        dataset.add((extent_uri, rico.hasExtentType, extent_type_uri, graph_uri))
        dataset.add((extent_type_uri, rico.isExtentTypeOf, extent_uri, graph_uri))
        
        # Usa cache per ExtentType
        if extent_type_uri not in created_extent_types:
            dataset.add((extent_type_uri, RDF.type, rico.ExtentType, graph_uri))
            dataset.add((extent_type_uri, RDFS.label, Literal(extent_label), graph_uri))
            created_extent_types.add(extent_type_uri)
        
        print(f"[EXTNEW] {inst_identifier} {tool_abbrev}: extraction time > pars_ext ({extraction_time_rounded}ms) - evangelisti_metadata_extraction.py:2699")

    return activity_uri

def process_metadata_with_unique_instances(metadata_dict, dataset, graph_uri, activity_uri, file_uri, instantiation_uri):
    """Process metadata with structured URIs - VERSIONE CORRETTA che evita tipi orfani"""
    if not metadata_dict:
        return []

    # === FILTRI APPLICATI PRIMA DI CREARE QUALSIASI ENTITÀ ===
    
    # Processa eccezioni (unchanged)
    exceptions_created = []
    for field, value in metadata_dict.items():
        if field in EXCEPTION_WARNING_FIELDS:
            tool_name = "Apache Tika" if field.startswith("X-TIKA:") else ("ExifTool" if field == "Warning" else "DROID" if field.startswith("DROID") else "Unknown")  # 🆕 DROID
            exception_uri = create_exception(field, value, activity_uri, file_uri, dataset, graph_uri, tool_name)
            exceptions_created.append(exception_uri)
    
    # ⚠️ MODIFICA CRITICA: Aggiungi i campi versione ExifTool e DROID ai filtri
    exiftool_version_batch_fields = {
        "ExifTool Version", "ExifToolVersion", "Tool Version", "Version", 
        "ExifTool:Version", "System:ExifToolVersion"
    }
    all_excluded_fields = TIKA_EXCLUDED_FIELDS | EXCEPTION_WARNING_FIELDS | EXIFTOOL_VERSION_FIELDS | exiftool_version_batch_fields | DROID_EXCLUDED_FIELDS | DROID_VERSION_FIELDS  # 🆕 DROID
    
    # ⚠️ PRE-FILTRA i metadati per valore PRIMA di creare i tipi
    pre_filtered_metadata = {}
    
    for field, value in metadata_dict.items():
        # Salta campi esclusi
        if field in all_excluded_fields:
            continue
            
        # Gestisci liste
        if isinstance(value, list):
            # Filtra valori vuoti nella lista
            valid_values = [v for v in value if v not in ("", [], None) and 
                           (not isinstance(v, str) or v.strip())]
            
            if valid_values:
                pre_filtered_metadata[field] = valid_values
        else:
            # Valori singoli - controllo rigoroso
            if value in ("", [], None):
                continue
                
            if isinstance(value, str) and not value.strip():
                continue
                
            pre_filtered_metadata[field] = value
    
    # Se non ci sono metadati validi, esci
    if not pre_filtered_metadata:
        return []

    # === ORA PROCESSA SOLO I METADATI VALIDATI ===
    
    metadata_instances_used = []
    
    # Determina context info dal graph URI
    context_info = None
    if graph_uri == fs_graph_uri:
        context_info = {'tool': 'os'}
    elif graph_uri == exif_graph_uri:
        context_info = {'tool': 'exiftool'}
    elif graph_uri == droid_graph_uri:  # 🆕 NUOVO
        context_info = {'tool': 'droid'}
    else:
        context_info = {'tool': 'tika'}
    
    # 🆕 OTTIENI IL SOFTWARE PRINCIPALE PER I COLLEGAMENTI
    main_software_uri = get_main_software_from_context(context_info, dataset, graph_uri)
    
    # 🆕 SET PER TRACCIARE I METADATA TYPES USATI IN QUESTA SESSIONE
    metadata_types_in_this_session = set()
    
    # Processa SOLO i metadati pre-filtrati
    for field, value in pre_filtered_metadata.items():
        
        if isinstance(value, list):
            for val in value:
                type_uri = get_or_create_metadata_type_uri(field, context_info)
                
                # Usa cache locale invece di query costose
                if type_uri not in created_metadata_types:
                    dataset.add((type_uri, RDF.type, bodi.TechnicalMetadataType, graph_uri))
                    dataset.add((type_uri, RDFS.label, Literal(field), graph_uri))
                    created_metadata_types.add(type_uri)
                
                # 🆕 TRACCIA IL METADATA TYPE PER COLLEGAMENTO SOFTWARE
                metadata_types_in_this_session.add(type_uri)
                
                instance_uri = get_or_create_metadata_instance_uri(field, val, instantiation_uri)
                
                # Crea instance
                if instance_uri not in created_metadata_instances:
                    dataset.add((instance_uri, RDF.type, bodi.TechnicalMetadata, graph_uri))
                    dataset.add((instance_uri, RDF.value, Literal(val), graph_uri))
                    
                    combined_value = f"{field}: {val}"
                    dataset.add((instance_uri, RDFS.label, Literal(combined_value), graph_uri))
                   
                    dataset.add((instance_uri, bodi.hasTechnicalMetadataType, type_uri, graph_uri))
                    dataset.add((type_uri, bodi.isTechnicalMetadataTypeOf, instance_uri, graph_uri))
                    dataset.add((instance_uri, bodi.generatedBy, activity_uri, graph_uri))
                    dataset.add((activity_uri, bodi.hasGenerated, instance_uri, graph_uri))
                    created_metadata_instances.add(instance_uri)
                
                dataset.add((instantiation_uri, bodi.hasTechnicalMetadata, instance_uri, graph_uri))
                dataset.add((instance_uri, bodi.isTechnicalMetadataOf, instantiation_uri, graph_uri))
                
                metadata_instances_used.append(instance_uri)
        else:
            type_uri = get_or_create_metadata_type_uri(field, context_info)
            
            # Usa cache locale
            if type_uri not in created_metadata_types:
                dataset.add((type_uri, RDF.type, bodi.TechnicalMetadataType, graph_uri))
                dataset.add((type_uri, RDFS.label, Literal(field), graph_uri))
                created_metadata_types.add(type_uri)
            
            # 🆕 TRACCIA IL METADATA TYPE PER COLLEGAMENTO SOFTWARE
            metadata_types_in_this_session.add(type_uri)
            
            instance_uri = get_or_create_metadata_instance_uri(field, value, instantiation_uri)
            
            # Crea instance
            if instance_uri not in created_metadata_instances:
                dataset.add((instance_uri, RDF.type, bodi.TechnicalMetadata, graph_uri))
                dataset.add((instance_uri, RDF.value, Literal(value), graph_uri))
                
                combined_value = f"{field}: {value}"
                dataset.add((instance_uri, RDFS.label, Literal(combined_value), graph_uri))
                
                dataset.add((instance_uri, bodi.hasTechnicalMetadataType, type_uri, graph_uri))
                dataset.add((type_uri, bodi.isTechnicalMetadataTypeOf, instance_uri, graph_uri))
                dataset.add((instance_uri, bodi.generatedBy, activity_uri, graph_uri))
                dataset.add((activity_uri, bodi.hasGenerated, instance_uri, graph_uri))
                created_metadata_instances.add(instance_uri)
            
            dataset.add((instantiation_uri, bodi.hasTechnicalMetadata, instance_uri, graph_uri))
            dataset.add((instance_uri, bodi.isTechnicalMetadataOf, instantiation_uri, graph_uri))
            
            metadata_instances_used.append(instance_uri)
    
    # 🆕 AGGIUNGI COLLEGAMENTI DIRETTI TECHNICAL METADATA TYPE ↔ SOFTWARE
    if metadata_types_in_this_session:
        for type_uri in metadata_types_in_this_session:
            # TechnicalMetadataType generatedBy Software
            dataset.add((type_uri, bodi.generatedBy, main_software_uri, graph_uri))
            
            # Software hasGenerated TechnicalMetadataType (inversa)
            dataset.add((main_software_uri, bodi.hasGenerated, type_uri, graph_uri))
    
    return metadata_instances_used

# === FUNZIONI PER SERIALIZZAZIONE INCREMENTALE ===

def append_to_file(file_path, content):
    """Appende contenuto a un file"""
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write(content)

def serialize_dataset_incremental(dataset, output_file, dataset_type):
    """Serializza un dataset appendendo al file e poi lo pulisce"""
    global persistence_counter
    
    if len(dataset) == 0:
        print(f"[DEBUG] Dataset {dataset_type} vuoto, salto serializzazione - evangelisti_metadata_extraction.py:2868")
        # CORREZIONE: Restituisci un dataset vuoto invece di None
        empty_dataset = Dataset()
        # Mantieni i namespace binding del dataset originale
        for prefix, namespace in dataset.namespaces():
            empty_dataset.bind(prefix, namespace)
        return empty_dataset
    
    try:
        # Serializza in una stringa temporanea
        temp_content = dataset.serialize(format='nquads')
        
        # Appendi al file
        append_to_file(output_file, temp_content)
        print(f"[PERSISTENCE] {dataset_type}: {len(dataset)} triple appendte a {output_file} - evangelisti_metadata_extraction.py:2882")
        
        # Pulisci il dataset ma mantieni i namespace binding
        namespaces = dict(dataset.namespaces())
        dataset.store.close()
        new_dataset = Dataset()
        for prefix, namespace in namespaces.items():
            new_dataset.bind(prefix, namespace)
        
        return new_dataset
        
    except Exception as e:
        print(f"[ERRORE] Errore nella serializzazione incrementale di {dataset_type}: {e} - evangelisti_metadata_extraction.py:2894")
        # CORREZIONE: Assicurati che il dataset abbia i namespace binding
        for prefix, namespace in [("rico", rico), ("bodi", bodi)]:
            if prefix not in dict(dataset.namespaces()):
                dataset.bind(prefix, namespace)
        return dataset

def write_shared_entities_once(dataset, graph_uri, dataset_type):
    """Scrive le entità condivise solo una volta per dataset - CORRETTO"""
    global shared_entities_written
    
    if shared_entities_written[dataset_type]:
        return dataset
    
    print(f"[PERSISTENCE] Scrivendo entità condivise per {dataset_type}... - evangelisti_metadata_extraction.py:2908")
    
    dataset.add((lucia_person, RDF.type, rico.Person, graph_uri))
    dataset.add((lucia_person, RDFS.label, Literal("Lucia Giagnolini"), graph_uri))
    
    dataset.add((extraction_date, RDF.type, rico.Date, graph_uri))
    dataset.add((extraction_date, rico.normalizedDateValue, Literal(extraction_date_str), graph_uri))
    dataset.add((extraction_date, rico.expressedDate, Literal(extraction_date_expr), graph_uri))
    
    shared_entities_written[dataset_type] = True
    return dataset

def remove_orphan_types_from_dataset(dataset, dataset_name, graph_uri):
    """Rimuove tipi orfani dal dataset prima della serializzazione - CON GRAFO SPECIFICO"""
    types_found = set()
    types_with_instances = set()
    
    print(f"[CLEANUP] Analizzando dataset {dataset_name} per tipi orfani nel grafo {graph_uri}... - evangelisti_metadata_extraction.py:2925")
    
    try:
        # 🔧 FIX: Filtra per il grafo specifico
        for s, p, o, g in dataset.quads((None, None, None, graph_uri)):
            if p == RDF.type and o == bodi.TechnicalMetadataType:
                types_found.add(s)
            elif p == bodi.hasTechnicalMetadataType:
                types_with_instances.add(o)
        
        print(f"[CLEANUP] Dataset {dataset_name}: {len(types_found)} tipi totali, {len(types_with_instances)} con istanze - evangelisti_metadata_extraction.py:2935")
        
    except Exception as e:
        print(f"[CLEANUP] ERRORE durante l'analisi del dataset {dataset_name}: {e} - evangelisti_metadata_extraction.py:2938")
        print(f"[CLEANUP] Provo metodo fallback senza filtro grafo... - evangelisti_metadata_extraction.py:2939")
        
        # Fallback: usa triples senza filtro grafo
        try:
            for s, p, o in dataset.triples((None, None, None)):
                if p == RDF.type and o == bodi.TechnicalMetadataType:
                    types_found.add(s)
                elif p == bodi.hasTechnicalMetadataType:
                    types_with_instances.add(o)
        except Exception as e2:
            print(f"[CLEANUP] ERRORE anche con fallback: {e2} - evangelisti_metadata_extraction.py:2949")
            return 0
    
    # Resto della funzione uguale...
    orphan_types = types_found - types_with_instances
    
    if orphan_types:
        print(f"[CLEANUP] Rimuovendo {len(orphan_types)} tipi orfani da {dataset_name} - evangelisti_metadata_extraction.py:2956")
        
        triples_removed = 0
        for orphan_type in orphan_types:
            # Rimuovi triple con grafo specifico
            try:
                for s, p, o, g in list(dataset.quads((orphan_type, None, None, graph_uri))):
                    dataset.remove((s, p, o, g))
                    triples_removed += 1
            except:
                # Fallback senza grafo specifico
                for s, p, o in list(dataset.triples((orphan_type, None, None))):
                    dataset.remove((s, p, o))
                    triples_removed += 1
            
            try:
                for s, p, o, g in list(dataset.quads((None, None, orphan_type, graph_uri))):
                    dataset.remove((s, p, o, g))
                    triples_removed += 1
            except:
                # Fallback senza grafo specifico
                for s, p, o in list(dataset.triples((None, None, orphan_type))):
                    dataset.remove((s, p, o))
                    triples_removed += 1
        
        print(f"[CLEANUP] Rimosse {triples_removed} triple relative a {len(orphan_types)} tipi orfani - evangelisti_metadata_extraction.py:2981")
    
    return len(orphan_types)

def persist_datasets_if_needed():
    """Versione sicura che non serializza tipi orfani - INCLUDE TUTTA LA LOGICA ORIGINALE CON DROID"""
    global fs_ds, tika_ds, exif_ds, droid_ds, total_instantiations_processed, persistence_counter  # 🆕 DROID
    
    if total_instantiations_processed % PERSISTENCE_INTERVAL == 0:
        persistence_counter += 1
        print(f"\n[PERSISTENCE] === CHECKPOINT SICURO {persistence_counter} → {total_instantiations_processed} instantiation processate === - evangelisti_metadata_extraction.py:2991")
        
        # Scrivi entità condivise se è il primo checkpoint
        if persistence_counter == 1:
            fs_ds = write_shared_entities_once(fs_ds, fs_graph_uri, "fs")
            tika_ds = write_shared_entities_once(tika_ds, tika_graph_uri, "tika") 
            exif_ds = write_shared_entities_once(exif_ds, exif_graph_uri, "exif")
            if ENABLE_DROID:  # 🆕 NUOVO
                droid_ds = write_shared_entities_once(droid_ds, droid_graph_uri, "droid")
        

        # Serializza e pulisci i dataset (logica originale)
        fs_ds = serialize_dataset_incremental(fs_ds, OUTPUT_NQUADS_FS, "FileSystem")
        tika_ds = serialize_dataset_incremental(tika_ds, OUTPUT_NQUADS_TIKA, "Tika")
        exif_ds = serialize_dataset_incremental(exif_ds, OUTPUT_NQUADS_EXIF, "ExifTool")
        if ENABLE_DROID:  # 🆕 NUOVO
            droid_ds = serialize_dataset_incremental(droid_ds, OUTPUT_NQUADS_DROID, "DROID")
        
        # CORREZIONE: Controllo di sicurezza per valori None (logica originale)
        if fs_ds is None:
            fs_ds = Dataset()
            fs_ds.bind("rico", rico)
            fs_ds.bind("bodi", bodi)
            
        if tika_ds is None:
            tika_ds = Dataset()
            tika_ds.bind("rico", rico)
            tika_ds.bind("bodi", bodi)
            
        if exif_ds is None:
            exif_ds = Dataset()
            exif_ds.bind("rico", rico)
            exif_ds.bind("bodi", bodi)
            
        if ENABLE_DROID and droid_ds is None:  # 🆕 NUOVO
            droid_ds = Dataset()
            droid_ds.bind("rico", rico)
            droid_ds.bind("bodi", bodi)

        # Aggiungi date tipizzate ai dataset puliti (logica originale)
        dataset_list = [(fs_ds, fs_graph_uri, "FS"), (tika_ds, tika_graph_uri, "Tika"), (exif_ds, exif_graph_uri, "ExifTool")]
        if ENABLE_DROID:  # 🆕 NUOVO
            dataset_list.append((droid_ds, droid_graph_uri, "DROID"))
            
        for ds, graph_uri, name in dataset_list:
            ds.add((extraction_date, RDF.type, rico.Date, graph_uri))
            ds.add((extraction_date, rico.normalizedDateValue, Literal(extraction_date_str), graph_uri))
            ds.add((extraction_date, rico.expressedDate, Literal(extraction_date_expr), graph_uri))
        
        # Salva contatori a ogni checkpoint importante (logica originale)
        print(f"[PERSISTENCE] Salvataggio contatori URI checkpoint {persistence_counter}... - evangelisti_metadata_extraction.py:3065")
        save_counters_to_json()
        
        print(f"[PERSISTENCE] Checkpoint sicuro {persistence_counter} completato. Memoria liberata. - evangelisti_metadata_extraction.py:3068")
        
        # Re-bind namespaces dopo la pulizia (logica originale con controllo di sicurezza)
        all_datasets = [fs_ds, tika_ds, exif_ds]
        if ENABLE_DROID:  # 🆕 NUOVO
            all_datasets.append(droid_ds)
            
        for ds in all_datasets:
            if ds is not None:  # Controllo aggiuntivo di sicurezza
                ds.bind("rico", rico)
                ds.bind("bodi", bodi)

# === MAIN EXECUTION ===
if __name__ == "__main__":
    # CONTROLLO EMERGENZA FILE CXF
    print("[STARTUP] Controllo file temporanei CXF...")
    initial_cleanup = cleanup_tika_temp_files()
    if initial_cleanup > 0:
        print(f"[STARTUP] Cleanup iniziale: {initial_cleanup} file rimossi")
    args = parse_arguments()
    
    # Aggiorna configurazioni con argomenti da linea di comando
    CHUNK_SIZE = args.chunk_size
    PERSISTENCE_INTERVAL = args.persistence_interval
    ENABLE_DROID = not args.disable_droid 
    
    # Inizializza configurazione basata sulla directory selezionata
    initialize_config(args.directory)

    print(f"[INIT] Verifica e avvio Tika Server... - evangelisti_metadata_extraction.py:3092")
    if not setup_tika_server():
        print("[FALLITO] Tika server non disponibile e impossibile avviarlo automaticamente. - evangelisti_metadata_extraction.py:3094")
        print("[SUGGERIMENTO] Verifica che il file tikaserverstandard3.2.1.jar sia presente - evangelisti_metadata_extraction.py:3095")
        print("[SUGGERIMENTO] Oppure avvia manualmente: java server jar tikaserverstandard3.2.1.jar - evangelisti_metadata_extraction.py:3096")
        sys.exit(1)

    # 🆕 NUOVO: Setup DROID se abilitato
    if ENABLE_DROID:
        if not setup_droid():
            print("[WARNING] DROID non disponibile. Continuo senza DROID. - evangelisti_metadata_extraction.py:3102")
            ENABLE_DROID = False

    validate_output_filenames()

    
    # Inizializza RDF dataset dopo la configurazione
    fs_ds = Dataset()
    fs_ds.bind("rico", rico)
    fs_ds.bind("bodi", bodi)

    tika_ds = Dataset()
    tika_ds.bind("rico", rico)
    tika_ds.bind("bodi", bodi)

    exif_ds = Dataset()
    exif_ds.bind("rico", rico)
    exif_ds.bind("bodi", bodi)
    
    # 🆕 NUOVO: Dataset DROID
    if ENABLE_DROID:
        droid_ds = Dataset()
        droid_ds.bind("rico", rico)
        droid_ds.bind("bodi", bodi)
    else:
        droid_ds = None
    
    print(f"[INFO] Avvio estrazione metadati con URI strutturati, persistenza JSON e DROID → {datetime.now().isoformat()} - evangelisti_metadata_extraction.py:3129")
    print(f"[INFO] Directory selezionata: {args.directory} - evangelisti_metadata_extraction.py:3130")
    print(f"[INFO] Configurazione: chunk_size={CHUNK_SIZE}, persistence_interval={PERSISTENCE_INTERVAL} - evangelisti_metadata_extraction.py:3131")
    print(f"[INFO] DROID abilitato: {ENABLE_DROID} - evangelisti_metadata_extraction.py:3132")
    print(f"[INFO] File contatori JSON: {COUNTERS_JSON_FILE} - evangelisti_metadata_extraction.py:3133")
    print(f"[INFO] BASE_URIS configurati: - evangelisti_metadata_extraction.py:3134")
    for tool, uri in BASE_URIS.items():
        print(f"{tool}: {uri} - evangelisti_metadata_extraction.py:3136")
    
    # Carica contatori da JSON
    print(f"\n[INIT] Caricamento contatori esistenti... - evangelisti_metadata_extraction.py:3139")
    load_counters_from_json()
    print_counters_summary()
    
    # Inizializza i file di output
    initialize_output_files()
    
    # Estrai le instantiation dal file .nq
    instantiations_data = extract_instantiations_with_paths(INPUT_NQUADS)
    
    if not instantiations_data:
        print("[ERRORE] Nessuna instantiation trovata nel file .nq - evangelisti_metadata_extraction.py:3150")
        sys.exit(1)
    
    # Processa con serializzazione incrementale
    process_in_chunks_with_incremental_persistence(instantiations_data, CHUNK_SIZE)
    
    # Salva contatori finali
    print(f"\n[FINAL] Salvataggio finale contatori... - evangelisti_metadata_extraction.py:3157")
    save_counters_to_json()
    
    # Statistiche finali con dettaglio URI
    print(f"\n=== STATISTICHE FINALI URI STRUTTURATI CON PERSISTENZA E DROID === - evangelisti_metadata_extraction.py:3161")
    print(f"[INFO] Estrazione metadati completata il {datetime.now().isoformat()} - evangelisti_metadata_extraction.py:3162")
    print(f"[INFO] Directory processata: {args.directory} - evangelisti_metadata_extraction.py:3163")
    print(f"[INFO] Totale instantiation processate: {total_instantiations_processed} - evangelisti_metadata_extraction.py:3164")
    print(f"[INFO] Checkpoint di persistenza: {persistence_counter} - evangelisti_metadata_extraction.py:3165")
    print(f"🆕 Software Documentation: URLs documentazione ufficiale collegati - evangelisti_metadata_extraction.py:3166")
    print(f"🆕 DROID: Supporto completo per metadati DROID {'ATTIVATO' if ENABLE_DROID else 'DISATTIVATO'} - evangelisti_metadata_extraction.py:3167")
    
    # Verifica file contatori
    if os.path.exists(COUNTERS_JSON_FILE):
        counter_file_size = os.path.getsize(COUNTERS_JSON_FILE)
        print(f"[INFO] Dimensione file contatori: {counter_file_size} bytes - evangelisti_metadata_extraction.py:3172")
    
    print(f"[INFO] Software stack creati: {software_stack_counter} (cache: {len(software_stack_cache)}) - evangelisti_metadata_extraction.py:3174")
    print(f"[INFO] TechnicalMetadataType creati per software: - evangelisti_metadata_extraction.py:3175")
    for software, count in software_counters.items():
        print(f"{software}: {count} tipi - evangelisti_metadata_extraction.py:3177")
    print(f"[INFO] Totale instantiation con metadati: {len(instantiation_maps)} - evangelisti_metadata_extraction.py:3178")
    total_instances = sum(len(m) for m in instantiation_maps.values())
    print(f"[INFO] Totale TechnicalMetadata instances: {total_instances} - evangelisti_metadata_extraction.py:3180")
    print(f"[INFO] Exception contexts: {len(exception_counters)} - evangelisti_metadata_extraction.py:3181")
    if exception_counters:
        total_exceptions = sum(exception_counters.values())
        print(f"[INFO] Totale exceptions: {total_exceptions} - evangelisti_metadata_extraction.py:3184")
    
    # Verifica dimensioni file finali
    print(f"\n=== FILE OUTPUT FINALI === - evangelisti_metadata_extraction.py:3187")
    output_files = [OUTPUT_NQUADS_FS, OUTPUT_NQUADS_TIKA, OUTPUT_NQUADS_EXIF]
    if ENABLE_DROID:  # 🆕 NUOVO
        output_files.append(OUTPUT_NQUADS_DROID)
        
    for file_path in output_files:
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            with open(file_path, 'r') as f:
                line_count = sum(1 for line in f if line.strip() and not line.startswith('#'))
            print(f"[INFO] {file_path}: {file_size} bytes, ~{line_count} righe - evangelisti_metadata_extraction.py:3197")
        else:
            print(f"[WARNING] {file_path}: File non creato - evangelisti_metadata_extraction.py:3199")
            
    print(f"\n=== URI STRUTTURATI: RIEPILOGO CON PERSISTENZA E DROID === - evangelisti_metadata_extraction.py:3201")
    print(f"✅ TechnicalMetadataType: BASE_URIS + contatori zeropadded [PERSISTENTI] - evangelisti_metadata_extraction.py:3202")
    print(f"✅ TechnicalMetadata: URI specifici per instantiation - evangelisti_metadata_extraction.py:3203")
    print(f"✅ Activity: URI specifici per instantiation + tool - evangelisti_metadata_extraction.py:3204")
    print(f"✅ Extent: URI specifici per instantiation + tool - evangelisti_metadata_extraction.py:3205")
    print(f"✅ Exception: URI specifici per instantiation + tool + contatore [PERSISTENTI] - evangelisti_metadata_extraction.py:3206")
    print(f"✅ SoftwareStack: URI strutturati con contatori [PERSISTENTI] - evangelisti_metadata_extraction.py:3207")
    print(f"🆕 DROID: Supporto completo per metadati DROID {'ATTIVATO' if ENABLE_DROID else 'DISATTIVATO'} - evangelisti_metadata_extraction.py:3208")
    print(f"✅ Contatori salvati in: {COUNTERS_JSON_FILE} - evangelisti_metadata_extraction.py:3209")
    print(f"✅ Consistenza URI garantita tra esecuzioni diverse! - evangelisti_metadata_extraction.py:3210")
    
    print_counters_summary()

# fmt: off