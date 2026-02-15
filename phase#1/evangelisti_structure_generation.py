import os
import sys
import hashlib
import logging
import urllib.parse
import time
import argparse
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
from pathlib import Path
import re
import traceback
import time
from rdflib import Namespace, URIRef, Literal, RDF, RDFS, XSD, Dataset
from rdflib.namespace import PROV
import locale


# === IMPORT CONFIGURAZIONE CENTRALIZZATA ===
try:
    from config_loader import load_config, ConfigError
    USE_CENTRALIZED_CONFIG = True
    print("✅ Configurazione centralizzata caricata - evangelisti_structure_generation.py:22")
except ImportError:
    USE_CENTRALIZED_CONFIG = False
    ConfigError = Exception  # Fallback
    load_config = None  # Fallback
    print("⚠️ Configurazione centralizzata non disponibile, uso configurazione locale - evangelisti_structure_generation.py:27")

def get_directory_configs():
    """Ottiene le configurazioni delle directory"""
    if USE_CENTRALIZED_CONFIG and load_config is not None:
        try:
            config = load_config()
            return config.to_legacy_format("evangelisti_structure_generation")
        except ConfigError:
            print("⚠️ Errore configurazione centralizzata, uso fallback locale - evangelisti_structure_generation.py:36")
            pass
    
    # Configurazione locale fallback aggiornata
    return {
        "floppy": {
            "path": "", #provide desided file system path here
            "root_id": "RS1_RS3",
            "output_suffix": "floppy",
            "log_suffix": "floppy"
        },
        "hd": {
            "path": "", #provide desided file system path here
            "root_id": "RS1_RS1", 
            "output_suffix": "hd",
            "log_suffix": "hd"
        },
        "hdesterno": {
            "path": "", #provide desided file system path here
            "root_id": "RS1_RS2",
            "output_suffix": "hdesterno", 
            "log_suffix": "hdesterno"
        }
    }

# Inizializza configurazioni globalmente
DIRECTORY_CONFIGS = get_directory_configs()

# Verifica che le configurazioni siano state caricate
if not DIRECTORY_CONFIGS:
    print("❌ ERRORE: Impossibile caricare le configurazioni delle directory - evangelisti_structure_generation.py:66")
    sys.exit(1)
else:
    print(f"✅ Configurazioni directory caricate: {', '.join(DIRECTORY_CONFIGS.keys())} - evangelisti_structure_generation.py:69") 

# === CONFIGURAZIONE GLOBALE ===
@dataclass
class Config:
    """Configurazione centralizzata del sistema"""
    # Percorsi (da configurare dinamicamente)
    ROOT_PATH: str
    OUTPUT_FILE: str
    LOG_FILE: str
    ROOT_ID: str
    
    # Parametri di performance
    BATCH_SIZE: int = 2000
    HASH_CHUNK_SIZE: int = 8192
    PROGRESS_INTERVAL: int = 100
    
    # Namespace
    BASE_URL: str = "http://ficlit.unibo.it/ArchivioEvangelisti/"
    
    # Opzioni di debug
    DEBUG_MODE: bool = True
    VERBOSE_LOGGING: bool = True
    VALIDATE_OUTPUT: bool = True
    
    @classmethod
    def from_directory_type(cls, directory_type: str) -> 'Config':
        """Crea configurazione basata sul tipo di directory"""
        if directory_type not in DIRECTORY_CONFIGS:
            raise ValueError(f"Tipo directory non supportato: {directory_type}. "
                           f"Tipi disponibili: {list(DIRECTORY_CONFIGS.keys())}")
        
        config_data = DIRECTORY_CONFIGS[directory_type]
        
        # Genera nomi file di output e log
        output_file = f"structure_{config_data['output_suffix']}.nq" if config_data['output_suffix'] else "structure.nq"
        log_file = f"structure_generation_{config_data['log_suffix']}.log" if config_data['log_suffix'] else "structure_generation.log"
        
        return cls(
            ROOT_PATH=config_data['path'],
            ROOT_ID=config_data['root_id'],
            OUTPUT_FILE=output_file,
            LOG_FILE=log_file
        )
    
    @classmethod
    def from_custom_path(cls, root_path: str, root_id: str = "RS1_CUSTOM", 
                        output_suffix: str = "custom") -> 'Config':
        """Crea configurazione per percorso personalizzato"""
        return cls(
            ROOT_PATH=root_path,
            ROOT_ID=root_id,
            OUTPUT_FILE=f"structure_{output_suffix}.nq",
            LOG_FILE=f"structure_generation_{output_suffix}.log"
        )


# === FUNZIONI HELPER PER DETERMINAZIONE TIPOLOGIA A PARTIRE DALL'ID ===

def determine_rico_type_from_id(entry_id: str) -> str:
    """
    Determina il tipo RiC basandosi sul pattern dell'ID
    
    Regole:
    - ID che finisce con _R{numero} → rico:Record (file)  
    - ID che finisce con _RS{numero} → rico:RecordSet (cartella)
    - ID che è solo RS{numero} → rico:RecordSet (cartella root)
    """
    # Pattern per file: termina con _R seguito da numeri
    file_pattern = r'_R\d+$'
    
    # Pattern per cartelle: termina con _RS seguito da numeri, o è solo RS + numeri
    folder_pattern = r'(_RS\d+$|^RS\d+$)'
    
    if re.search(file_pattern, entry_id):
        return 'record'  # File
    elif re.search(folder_pattern, entry_id):
        return 'recordset'  # Cartella
    else:
        # Fallback: se non corrisponde ai pattern, probabilmente è una cartella
        print(f"[WARNING] ID pattern non riconosciuto: {entry_id}, assumo RecordSet - evangelisti_structure_generation.py:149")
        return 'recordset'

def get_rico_type_uri(rico_type: str, namespace_manager) -> 'URIRef':
    """Converte stringa tipo in URI RiC"""
    if rico_type == 'record':
        return namespace_manager.rico.Record
    elif rico_type == 'recordset':
        return namespace_manager.rico.RecordSet
    else:
        raise ValueError(f"Tipo RiC non riconosciuto: {rico_type}")

# === SISTEMA DI LOGGING ===
class AdvancedLogger:
    """Sistema di logging avanzato con tracking dettagliato"""
    
    def __init__(self, config: Config):
        self.config = config
        self.setup_logging()
        self.stats = {
            'files_processed': 0,
            'directories_processed': 0,
            'instantiations_created': 0,
            'triples_generated': 0,
            'errors_encountered': 0
        }
        self.start_time = time.time()
        
        # Tracking dettagliato
        self.instantiation_log = []  # Lista di tutte le istanziazioni create
        self.error_paths = []        # Lista dei path che hanno causato errori
    
    def setup_logging(self):
        """Configura il sistema di logging"""
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            '[%(levelname)s] %(asctime)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        
        # File handler
        file_handler = logging.FileHandler(self.config.LOG_FILE, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '[%(levelname)s] %(asctime)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)
        
        # Root logger configuration
        self.logger = logging.getLogger('StructureGenerator')
        self.logger.setLevel(logging.DEBUG if self.config.VERBOSE_LOGGING else logging.INFO)
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
    
    def info(self, message: str):
        self.logger.info(message)
    
    def debug(self, message: str):
        self.logger.debug(message)
    
    def warning(self, message: str):
        self.logger.warning(message)
    
    def error(self, message: str):
        self.logger.error(message)
        self.stats['errors_encountered'] += 1
    
    def update_stats(self, stat_name: str, increment: int = 1):
        """Aggiorna le statistiche"""
        if stat_name in self.stats:
            self.stats[stat_name] += increment
    
    def log_instantiation_created(self, entry_id: str, entry_path: str, entry_type: str):
        """Log dettagliato delle istanziazioni create"""
        self.instantiation_log.append({
            'entry_id': entry_id,
            'path': entry_path,
            'type': entry_type,
            'timestamp': time.time()
        })
        self.update_stats('instantiations_created')
    
    def log_error_path(self, path: str, error: str):
        """Log dei path che causano errori"""
        self.error_paths.append({
            'path': path,
            'error': str(error),
            'timestamp': time.time()
        })
        self.update_stats('errors_encountered')
    
    def progress_report(self, current: int, total: int, operation: str = "Processing"):
        """Report di progresso con ETA"""
        if current % self.config.PROGRESS_INTERVAL == 0 or current == total:
            elapsed = time.time() - self.start_time
            if current > 0:
                eta = (elapsed / current) * (total - current)
                eta_str = f", ETA: {eta:.1f}s" if eta > 0 else ""
            else:
                eta_str = ""
            
            percentage = (current / total * 100) if total > 0 else 0
            self.info(f"{operation}: {current}/{total} ({percentage:.1f}%){eta_str}")
    
    def analyze_discrepancies(self):
        """Analisi dettagliata delle discrepanze"""
        self.info("\n=== ANALISI DISCREPANZE ===")
        
        # Conta istanziazioni per tipo
        type_counts = {}
        for inst in self.instantiation_log:
            t = inst['type']
            type_counts[t] = type_counts.get(t, 0) + 1
        
        self.info(f"Istanziazioni per tipo:")
        for t, count in type_counts.items():
            self.info(f"  {t}: {count}")
        
        # Verifica duplicati
        entry_ids = [inst['entry_id'] for inst in self.instantiation_log]
        duplicates = set([x for x in entry_ids if entry_ids.count(x) > 1])
        
        if duplicates:
            self.warning(f"ID duplicati trovati: {len(duplicates)}")
            for dup_id in list(duplicates)[:5]:  # Mostra primi 5
                paths = [inst['path'] for inst in self.instantiation_log if inst['entry_id'] == dup_id]
                self.warning(f"  {dup_id}: {paths}")
        else:
            self.info("Nessun ID duplicato trovato nelle istanziazioni")
        
        # Analisi errori
        self.info(f"Analisi errori ({len(self.error_paths)} totali):")
        error_summary = {}
        for error_info in self.error_paths:
            error_type = error_info['error'].split(':')[0] if ':' in error_info['error'] else error_info['error']
            error_summary[error_type] = error_summary.get(error_type, 0) + 1
        
        for error_type, count in sorted(error_summary.items()):
            self.info(f"  {error_type}: {count}")
        
        # Calcolo discrepanze
        expected_instantiations = (
            self.stats['files_processed'] + 
            self.stats['directories_processed'] +
            1  # Root sempre presente
        )
        actual_instantiations = len(self.instantiation_log)
        
        self.info(f"\nConteggio istanziazioni:")
        self.info(f"  File processati: {self.stats['files_processed']}")
        self.info(f"  Directory processate: {self.stats['directories_processed']}")
        self.info(f"  Root: 1")
        self.info(f"  Totale atteso: {expected_instantiations}")
        self.info(f"  Istanziazioni effettive: {actual_instantiations}")
        self.info(f"  Discrepanza: {actual_instantiations - expected_instantiations}")
        self.info(f"  Errori totali: {len(self.error_paths)}")
        
        if actual_instantiations != expected_instantiations:
            self.warning(f"⚠️  DISCREPANZA RILEVATA: {actual_instantiations - expected_instantiations}")
        else:
            self.info("✅ Conteggio istanziazioni corretto")
        
        self.info("===============================")
    
    def final_report(self):
        """Report finale con statistiche complete"""
        elapsed = time.time() - self.start_time
        self.info("\n" + "="*60)
        self.info("GENERAZIONE COMPLETATA - REPORT FINALE")
        self.info("="*60)
        self.info(f"Configurazione: {self.config.ROOT_ID} -> {os.path.basename(self.config.ROOT_PATH)}")
        self.info(f"Tempo totale di esecuzione: {elapsed:.2f} secondi")
        self.info(f"File processati: {self.stats['files_processed']}")
        self.info(f"Directory processate: {self.stats['directories_processed']}")
        self.info(f"Istanziazioni create: {len(self.instantiation_log)}")
        self.info(f"Triple RDF generate: {self.stats['triples_generated']}")
        self.info(f"Errori riscontrati: {len(self.error_paths)}")
        
        if self.stats['files_processed'] > 0:
            avg_time = elapsed / self.stats['files_processed']
            self.info(f"Tempo medio per file: {avg_time*1000:.2f}ms")
        
        self.info("="*60)


# === GESTIONE HASH E DUPLICATI ===
class HashManager:
    """Gestione ottimizzata degli hash e rilevamento duplicati"""
    
    def __init__(self, logger: AdvancedLogger):
        self.logger = logger
        self.hash_to_paths: Dict[str, List[str]] = {}
        self.hash_to_instances: Dict[str, List[URIRef]] = {}
        self.processed_hashes: Set[str] = set()
    
    def calculate_file_hash(self, file_path: str, chunk_size: int = 8192) -> Optional[str]:
        """Calcola l'hash SHA-256 di un file"""
        try:
            hash_sha256 = hashlib.sha256()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_sha256.update(chunk)
            return hash_sha256.hexdigest()
        except Exception as e:
            self.logger.log_error_path(file_path, f"Calcolo hash: {e}")
            return None
    
    def register_hash(self, file_hash: str, file_path: str, instance_uri: URIRef):
        """Registra un hash con il suo percorso e istanza"""
        if file_hash not in self.hash_to_paths:
            self.hash_to_paths[file_hash] = []
            self.hash_to_instances[file_hash] = []
        
        self.hash_to_paths[file_hash].append(file_path)
        self.hash_to_instances[file_hash].append(instance_uri)
        self.processed_hashes.add(file_hash)
    
    def get_duplicate_instances(self, file_hash: str) -> List[URIRef]:
        """Ottieni tutte le istanze con lo stesso hash"""
        return self.hash_to_instances.get(file_hash, [])
    
    def get_duplicate_count(self) -> int:
        """Conta i file duplicati"""
        return sum(1 for paths in self.hash_to_paths.values() if len(paths) > 1)
    
    def get_unique_hash_count(self) -> int:
        """Conta gli hash unici"""
        return len(self.processed_hashes)


# === NAMESPACE E URI MANAGER ===
class NamespaceManager:
    """Gestione centralizzata dei namespace e URI"""
    
    def __init__(self, config: Config):
        self.config = config
        self.setup_namespaces()
        self.setup_graph()
    
    def setup_namespaces(self):
        """Configura i namespace RDF"""
        self.rico = Namespace("https://www.ica.org/standards/RiC/ontology#")
        self.bodi = Namespace("http://w3id.org/bodi#")
        self.premis = Namespace("http://www.loc.gov/premis/rdf/v3/")
        self.lrmoo = Namespace("http://www.cidoc-crm.org/lrmoo/")
        
        # URI del grafo nominale
        encoded_root_id = urllib.parse.quote(self.config.ROOT_ID, safe='')
        self.named_graph_uri = URIRef(f"{self.config.BASE_URL}structure/{encoded_root_id}")
    
    def setup_graph(self):
        """Configura il dataset RDF principale"""
        self.dataset = Dataset()
        self.graph = self.dataset.get_context(self.named_graph_uri)
        
        # Bind dei prefissi
        self.dataset.bind("rico", self.rico)
        self.dataset.bind("bodi", self.bodi)
        self.dataset.bind("premis", self.premis)
        self.dataset.bind("prov", PROV)
        self.dataset.bind("rdfs", RDFS)
        self.dataset.bind("xsd", XSD)
    
    def create_uri(self, identifier: str) -> URIRef:
        """Crea un URI basato su un identificatore"""
        return URIRef(self.config.BASE_URL + identifier)
    
    def create_algorithm_uri(self) -> URIRef:
        """Crea URI per l'algoritmo SHA-256"""
        return URIRef(f"{self.config.BASE_URL}mechanism/sha256")


# === ENTITY BUILDER ===
class EntityBuilder:
    """Costruzione delle entità RDF"""
    
    def __init__(self, namespace_manager: NamespaceManager, logger: AdvancedLogger):
        self.ns = namespace_manager
        self.logger = logger
        self.triple_count = 0
    
    def add_triple(self, subject, predicate, obj):
        """Aggiunge una tripla al grafo con conteggio - SOLO NAMED GRAPH"""
        # CORREZIONE: Usa add con grafo esplicito per evitare duplicazione DEFAULT
        self.ns.dataset.add((subject, predicate, obj, self.ns.named_graph_uri))
        self.triple_count += 1
        self.logger.update_stats('triples_generated')
    
    def create_sha256_algorithm(self) -> URIRef:
        """Crea l'entità algoritmo SHA-256"""
        algorithm_uri = self.ns.create_algorithm_uri()
        self.add_triple(algorithm_uri, RDF.type, self.ns.bodi.Algorithm)
        # 🆕 AGGIUNGI LABEL
        self.add_triple(algorithm_uri, RDFS.label, Literal("SHA-256"))
        self.add_triple(algorithm_uri, self.ns.rico.hasTechnicalCharacteristic,
                       Literal("Secure Hash Algorithm 256-bit (SHA-256)", datatype=XSD.string))
        self.logger.debug("Creato algoritmo SHA-256")
        return algorithm_uri
    
    def create_record_entity(self, entry_id: str, entry_name: str, entry_type: str) -> URIRef:
        """
        Crea entità Record o RecordSet - VERSIONE CORRETTA CON ID PATTERN
        
        NOTA: Il parametro entry_type viene IGNORATO, il tipo viene determinato dall'ID
        """
        base_uri = self.ns.create_uri(entry_id)
        
        # 🆕 DETERMINA IL TIPO DALL'ID (non dal parametro entry_type)
        rico_type = determine_rico_type_from_id(entry_id)
        rico_type_uri = get_rico_type_uri(rico_type, self.ns)
        
        # Log della determinazione tipo
        expected_entry_type = 'folder' if rico_type == 'recordset' else 'file'
        if entry_type != expected_entry_type:
            self.logger.warning(f"[TYPE-MISMATCH] {entry_id}: parametro='{entry_type}', "
                            f"da_ID='{rico_type}' - uso il tipo da ID")
        
        # Verifica se l'entità esiste già 
        existing_types = list(self.ns.graph.objects(base_uri, RDF.type))
        existing_rico_types = [t for t in existing_types if 'RiC/ontology' in str(t)]
        
        if existing_rico_types:
            # Entità già esiste - verifica consistenza
            if rico_type_uri not in existing_types:
                self.logger.error(f"[CRITICAL-CONFLICT] {entry_id}: "
                                f"tipo esistente {[str(t) for t in existing_rico_types]}, "
                                f"tipo da ID {rico_type_uri}")
                
                raise ValueError(f"Conflitto critico di tipi per {entry_id}")
            
            # Gestisci label duplicata con priorità per label personalizzate
            custom_label = self.get_custom_record_label(entry_id, entry_name)
            existing_labels = list(self.ns.graph.objects(base_uri, RDFS.label))
            if not any(str(label) == custom_label for label in existing_labels):
                self.add_triple(base_uri, RDFS.label, Literal(custom_label))
                self.logger.debug(f"[LABEL-ADD] Aggiunta label personalizzata '{custom_label}' a {entry_id}")
            
            self.logger.debug(f"[EXISTING-ENTITY] {entry_id}: tipo corretto già presente")
            return base_uri
        
        # ENTITÀ NUOVA - usa tipo determinato dall'ID e label personalizzata
        self.add_triple(base_uri, RDF.type, rico_type_uri)
        
        # USA LABEL PERSONALIZZATA PER LE ROOT SPECIALI
        custom_label = self.get_custom_record_label(entry_id, entry_name)
        self.add_triple(base_uri, RDFS.label, Literal(custom_label))
        
        # AGGIUNGI RecordSetType SE È UN RECORDSET
        if rico_type == 'recordset':
            recordset_type_uri, recordset_type_label = self.get_recordset_type(entry_id)
            
            # Collega RecordSet al suo tipo
            self.add_triple(base_uri, self.ns.rico.hasRecordSetType, recordset_type_uri)
            self.add_triple(recordset_type_uri, self.ns.rico.isRecordSetTypeOf, base_uri)
            
            # Crea l'entità RecordSetType se non esiste già
            self.create_recordset_type_if_needed(recordset_type_uri, recordset_type_label)
            
            self.logger.debug(f"[RECORDSET-TYPE] {entry_id}: associato a {recordset_type_label}")
        
        self.logger.info(f"[NEW-ENTITY] {entry_id}: {rico_type} -> '{custom_label}'")
        
        # Identifier (resto del codice originale)
        identifier_uri = URIRef(f"{self.ns.config.BASE_URL}/{entry_id}_id")
        identifier_type_uri = URIRef(f"{self.ns.config.BASE_URL}idType/unique-id")
        
        self.add_triple(base_uri, self.ns.rico.hasOrHadIdentifier, identifier_uri)
        self.add_triple(identifier_uri, RDF.type, self.ns.rico.Identifier)
        self.add_triple(identifier_uri, RDFS.label, Literal(entry_id))
        self.add_triple(identifier_uri, self.ns.rico.hasIdentifierType, identifier_type_uri)
        
        # Identifier Type (solo una volta)
        if not hasattr(self, '_identifier_type_created'):
            self.add_triple(identifier_type_uri, RDF.type, self.ns.rico.IdentifierType)
            self.add_triple(identifier_type_uri, RDFS.label, Literal("Identificativo Univoco"))
            self._identifier_type_created = True
        
        return base_uri
    
    def get_custom_record_label(self, entry_id: str, default_name: str) -> str:
        """Ottiene label personalizzata per i RecordSet delle root speciali"""
        custom_labels = {
            "RS1_RS1": "Hard Disk computer",
            "RS1_RS2": "Hard Disk esterno", 
            "RS1_RS3": "Floppy Disks"
        }
        return custom_labels.get(entry_id, default_name)
    
    def get_recordset_type(self, entry_id: str) -> Tuple[URIRef, str]:
        """
        Determina il RecordSetType basato sull'ID
        
        Returns:
            Tuple[URIRef, str]: URI del tipo e label
        """
        root_containers = ["RS1_RS1", "RS1_RS2", "RS1_RS3"]
        
        if entry_id in root_containers:
            # Root Container
            type_uri = URIRef(f"{self.ns.config.BASE_URL}recordSetType/root-container")
            type_label = "Root Container"
        else:
            # Folder
            type_uri = URIRef(f"{self.ns.config.BASE_URL}recordSetType/folder") 
            type_label = "Folder"
        
        return type_uri, type_label
    
    def create_recordset_type_if_needed(self, recordset_type_uri: URIRef, recordset_type_label: str):
        """
        Crea l'entità RecordSetType se non esiste già
        """
        # Verifica se esiste già
        existing_types = list(self.ns.graph.objects(recordset_type_uri, RDF.type))
        
        if not any('RecordSetType' in str(t) for t in existing_types):
            # Crea l'entità RecordSetType
            self.add_triple(recordset_type_uri, RDF.type, self.ns.rico.RecordSetType)
            self.add_triple(recordset_type_uri, RDFS.label, Literal(recordset_type_label))
            
            self.logger.debug(f"[RECORDSET-TYPE-CREATED] Creato RecordSetType: {recordset_type_label}")
        
        return recordset_type_uri
    
    def create_instantiation(self, entry_id: str, entry_name: str, relative_path: str, 
                           depth: int, file_hash: Optional[str] = None, 
                           algorithm_uri: Optional[URIRef] = None) -> URIRef:
        """Crea istanziazione con verifica esplicita e label personalizzate per le root"""
        inst_uri = URIRef(f"{self.ns.config.BASE_URL}{entry_id}_inst")
        
        # CRUCIALE: Dichiarazione esplicita del tipo
        self.add_triple(inst_uri, RDF.type, self.ns.rico.Instantiation)
        
        # LABELS PERSONALIZZATE PER LE ROOT SPECIALI
        custom_label = self.get_custom_instantiation_label(entry_id, entry_name)
        self.add_triple(inst_uri, RDFS.label, Literal(custom_label))
        
        # Location
        location_uri = URIRef(f"{self.ns.config.BASE_URL}{entry_id}_inst_path")
        self.add_triple(inst_uri, PROV.atLocation, location_uri)
        self.add_triple(location_uri, RDF.type, PROV.Location)
        self.add_triple(location_uri, RDFS.label, Literal("/"+relative_path))
        
        # Depth
        self.add_triple(inst_uri, self.ns.bodi.hierarchyDepth, 
                       Literal(depth, datatype=XSD.integer))
        
        # Hash (solo per file)
        # Hash (solo per file)
        if file_hash and algorithm_uri:
            # Crea Activity di hash generation
            hash_activity_uri = self.create_hash_generation_activity(entry_id, algorithm_uri)
            
            hash_uri = URIRef(f"{self.ns.config.BASE_URL}/{entry_id}_inst_hash")
            self.add_triple(hash_uri, RDF.type, self.ns.premis.Fixity)
            self.add_triple(hash_uri, RDF.value, Literal(file_hash))
            self.add_triple(hash_uri, self.ns.bodi.isHashCodeOf, inst_uri)
            
            # Collegamenti Fixity-Activity (invece di Fixity-Algorithm)
            self.add_triple(hash_uri, self.ns.bodi.generatedBy, hash_activity_uri)
            self.add_triple(hash_activity_uri, self.ns.bodi.hasGenerated, hash_uri)
        
        if entry_id in ["RS1_RS1", "RS1_RS2", "RS1_RS3"]:
            self.create_storage_infrastructure(entry_id, inst_uri)
        
        self.logger.debug(f"Creata istanziazione: {inst_uri}")
        return inst_uri
    
    def get_custom_instantiation_label(self, entry_id: str, default_name: str) -> str:
        """Ottiene label personalizzata per le istanziazioni delle root speciali"""
        custom_labels = {
            "RS1_RS1": "Hard Disk computer",
            "RS1_RS2": "Hard Disk esterno", 
            "RS1_RS3": "Floppy Disks"
        }
        return custom_labels.get(entry_id, default_name)
    
    def create_storage_infrastructure(self, root_id: str, instantiation_uri: URIRef):
        """Crea l'infrastruttura di storage per le root speciali"""
        # Crea StorageLocation
        storage_location_uri = URIRef(f"{self.ns.config.BASE_URL}storage_location_{root_id}")
        self.add_triple(storage_location_uri, RDF.type, self.ns.premis.StorageLocation)
        self.add_triple(storage_location_uri, RDFS.label, 
                       Literal("Hard Disk conservato da ADLab - Laboratorio Analogico Digitale, Dipartimento di Filologia Classica e Italianistica, Università di Bologna, Via Zamboni 32, Bologna BO"))
        
        # Collega instantiation a StorageLocation
        self.add_triple(instantiation_uri, self.ns.premis.storedAt, storage_location_uri)
        
        # Crea StorageMedium
        storage_medium_uri = URIRef(f"{self.ns.config.BASE_URL}storage_medium_{root_id}")
        self.add_triple(storage_medium_uri, RDF.type, self.ns.premis.StorageMedium)
        self.add_triple(storage_medium_uri, RDFS.label, 
                       Literal("Samsung Portable SSD T7 1 TB USB tipo-C 3.2 Gen 2"))
        
        # Collega StorageLocation a StorageMedium
        self.add_triple(storage_location_uri, self.ns.premis.medium, storage_medium_uri)
        
        self.logger.info(f"[STORAGE-INFRA] Creata infrastruttura storage per {root_id}")
    
    def create_hierarchy_links(self, child_uri: URIRef, parent_uri: URIRef, 
                             child_inst_uri: Optional[URIRef] = None, 
                             parent_inst_uri: Optional[URIRef] = None):
        """Crea collegamenti gerarchici"""
        # Collegamenti principali
        self.add_triple(child_uri, self.ns.rico.isOrWasIncludedIn, parent_uri)
        self.add_triple(parent_uri, self.ns.rico.includesOrIncluded, child_uri)
        
        # Collegamenti istanziazioni
        if child_inst_uri and parent_inst_uri:
            self.add_triple(child_inst_uri, self.ns.rico.isOrWasPartOf, parent_inst_uri)
            self.add_triple(parent_inst_uri, self.ns.rico.hasOrHadPart, child_inst_uri)
    
    def create_instantiation_links(self, record_uri: URIRef, inst_uri: URIRef):
        """Crea collegamenti bidirezionali record-istanziazione"""
        self.add_triple(record_uri, self.ns.rico.hasOrHadInstantiation, inst_uri)
        self.add_triple(inst_uri, self.ns.rico.isOrWasInstantiationOf, record_uri)

    def create_hash_generation_activity(self, entry_id: str, algorithm_uri: URIRef) -> URIRef:
        """Crea attività di generazione hash"""
        # URI per l'activity
        activity_uri = URIRef(f"{self.ns.config.BASE_URL}{entry_id}_hash_gen_activity")
        
        # Crea date per l'operazione (timestamp corrente)
        from datetime import datetime
        import locale
        
        hash_date_timestamp = datetime.now()
        hash_date_uri = URIRef(f"{self.ns.config.BASE_URL}hash_date_{hash_date_timestamp.strftime('%Y%m%d_%H%M%S')}_{entry_id}")
        
        # Formatta le date come nel metadata extraction
        hash_date_str = hash_date_timestamp.isoformat()
        
        # Per expressedDate, usa il formato leggibile (assicurati che locale sia impostato)
        try:
            locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
            hash_date_expr = hash_date_timestamp.strftime("%d %B %Y at %H:%M")
        except:
            # Fallback se locale non disponibile
            hash_date_expr = hash_date_timestamp.strftime("%d %B %Y at %H:%M")
        
        # Activity
        self.add_triple(activity_uri, RDF.type, self.ns.rico.Activity)
        self.add_triple(activity_uri, RDFS.label, Literal("Hash generation using SHA-256"))
        
        # Date con entrambi i valori
        self.add_triple(hash_date_uri, RDF.type, self.ns.rico.Date)
        self.add_triple(hash_date_uri, self.ns.rico.normalizedDateValue, Literal(hash_date_str))
        self.add_triple(hash_date_uri, self.ns.rico.expressedDate, Literal(hash_date_expr))
        
        # Collegamenti Activity-Date
        self.add_triple(activity_uri, self.ns.rico.occurredAtDate, hash_date_uri)
        self.add_triple(hash_date_uri, self.ns.rico.isDateOfOccurrenceOf, activity_uri)
        
        # Collegamenti Activity-Algorithm
        self.add_triple(activity_uri, self.ns.rico.isOrWasPerformedBy, algorithm_uri)
        self.add_triple(algorithm_uri, self.ns.rico.performsOrPerformed, activity_uri)
        
        return activity_uri


# === FILE SYSTEM PROCESSOR ===
class FileSystemProcessor:
    """Processore del file system con gestione avanzata"""
    
    def __init__(self, config: Config, logger: AdvancedLogger, 
                 namespace_manager: NamespaceManager, entity_builder: EntityBuilder, 
                 hash_manager: HashManager):
        self.config = config
        self.logger = logger
        self.ns = namespace_manager
        self.builder = entity_builder
        self.hash_manager = hash_manager
        
        # Contatori
        self.folder_counters = {}  # parent_id -> counter
        self.file_counters = {}    # parent_id -> counter
        
        # Verifica percorso root
        if not os.path.exists(self.config.ROOT_PATH):
            raise FileNotFoundError(f"Directory root non trovata: {self.config.ROOT_PATH}")
    
    def get_next_folder_id(self, parent_id: str) -> str:
        """Ottieni il prossimo ID cartella per un parent"""
        if parent_id not in self.folder_counters:
            self.folder_counters[parent_id] = 0
        self.folder_counters[parent_id] += 1
        return f"{parent_id}_RS{self.folder_counters[parent_id]}"
    
    def get_next_file_id(self, parent_id: str) -> str:
        """Ottieni il prossimo ID file per un parent"""
        if parent_id not in self.file_counters:
            self.file_counters[parent_id] = 0
        self.file_counters[parent_id] += 1
        return f"{parent_id}_R{self.file_counters[parent_id]}"
    
    def process_entry(self, entry_path: str, entry_name: str, entry_id: str, 
                     parent_id: Optional[str], entry_type: str, 
                     relative_path: str, depth: int, algorithm_uri: URIRef) -> Tuple[Optional[URIRef], Optional[URIRef]]:
        """Processa una singola entry con tracking dettagliato"""
        
        try:
            # Crea l'entità record principale
            record_uri = self.builder.create_record_entity(entry_id, entry_name, entry_type)
            
            # MODIFICA: Crea istanziazione per TUTTE le root speciali, non solo quelle diverse da "RS1"
            inst_uri = None
            if entry_id != "RS1" or entry_id in ["RS1_RS1", "RS1_RS2", "RS1_RS3"]:  
                # Calcola hash per i file
                file_hash = None
                if entry_type == 'file':
                    file_hash = self.hash_manager.calculate_file_hash(entry_path)
                
                # Crea istanziazione
                inst_uri = self.builder.create_instantiation(
                    entry_id, entry_name, relative_path, depth, file_hash, algorithm_uri
                )
                
                # Collegamenti record-istanziazione
                self.builder.create_instantiation_links(record_uri, inst_uri)
                
                # Gestione duplicati hash
                if file_hash:
                    duplicate_instances = self.hash_manager.get_duplicate_instances(file_hash)
                    self.hash_manager.register_hash(file_hash, relative_path, inst_uri)
                
                # LOG DETTAGLIATO - Dopo la creazione dell'istanziazione
                self.logger.log_instantiation_created(entry_id, entry_path, entry_type)
            
            # Collegamenti gerarchici
            if parent_id:
                parent_uri = self.ns.create_uri(parent_id)
                parent_inst_uri = URIRef(f"{self.ns.config.BASE_URL}{parent_id}_inst") if parent_id != "RS1" else None
                self.builder.create_hierarchy_links(record_uri, parent_uri, inst_uri, parent_inst_uri)
            
            # Update statistics SOLO se tutto è andato bene
            if entry_type == 'file':
                self.logger.update_stats('files_processed')
            else:
                self.logger.update_stats('directories_processed')
            
            return record_uri, inst_uri
            
        except Exception as e:
            self.logger.log_error_path(entry_path, str(e))
            return None, None  # Indica fallimento
    
    def traverse_directory(self, path: str, current_id: str, depth: int, algorithm_uri: URIRef, 
                          total_entries: int = None, processed_count: int = 0) -> int:
        """Attraversamento ricorsivo con gestione errori migliorata"""
        
        try:
            entries = sorted(os.listdir(path), key=str.lower)
        except Exception as e:
            self.logger.log_error_path(path, f"Accesso directory: {e}")
            return processed_count
        
        for entry in entries:
            entry_path = os.path.join(path, entry)
            relative_path = os.path.relpath(entry_path, self.config.ROOT_PATH)
            entry_name = os.path.basename(entry_path)
            
            try:
                if os.path.isdir(entry_path):
                    entry_id = self.get_next_folder_id(current_id)
                    
                    # Processa directory - SE SUCCESSO, incrementa processed_count
                    result = self.process_entry(entry_path, entry_name, entry_id, current_id, 
                                             'folder', relative_path, depth, algorithm_uri)
                    
                    if result[0] is not None:  # Successo
                        processed_count += 1
                        
                        # Ricorsione
                        processed_count = self.traverse_directory(entry_path, entry_id, depth + 1, 
                                                               algorithm_uri, total_entries, processed_count)
                
                elif os.path.isfile(entry_path):
                    entry_id = self.get_next_file_id(current_id)
                    
                    # Processa file - SE SUCCESSO, incrementa processed_count
                    result = self.process_entry(entry_path, entry_name, entry_id, current_id, 
                                             'file', relative_path, depth, algorithm_uri)
                    
                    if result[0] is not None:  # Successo
                        processed_count += 1
                
                # Report progresso
                if total_entries and processed_count % self.config.PROGRESS_INTERVAL == 0:
                    self.logger.progress_report(processed_count, total_entries)
                    
            except Exception as e:
                # Log errore ma NON incrementare processed_count
                self.logger.log_error_path(entry_path, str(e))
                continue
        
        return processed_count
    
    def count_total_entries(self, path: str) -> int:
        """Conta il totale delle entry per il progress tracking"""
        total = 0
        try:
            for root, dirs, files in os.walk(path):
                total += len(dirs) + len(files)
        except Exception as e:
            self.logger.warning(f"Impossibile contare entry in {path}: {e}")
        return total
    
    def debug_instantiation_count(self):
        """Debug per verificare il conteggio delle istanziazioni"""
        self.logger.info("=== DEBUG CONTEGGIO ISTANZIAZIONI ===")
        self.logger.info(f"ROOT_ID configurato: '{self.config.ROOT_ID}'")
        self.logger.info(f"Condizione modificata: entry_id != 'RS1' OR entry_id in speciali")
        self.logger.info(f"Root creerebbe istanziazione? {self.config.ROOT_ID != 'RS1' or self.config.ROOT_ID in ['RS1_RS1', 'RS1_RS2', 'RS1_RS3']}")
        
        # Conteggio teorico
        theoretical_total = (
            self.logger.stats['files_processed'] + 
            self.logger.stats['directories_processed'] + 
            (1 if self.config.ROOT_ID != 'RS1' or self.config.ROOT_ID in ['RS1_RS1', 'RS1_RS2', 'RS1_RS3'] else 0)
        )
        
        actual_instantiations = len(self.logger.instantiation_log)
        
        self.logger.info(f"Totale teorico: {theoretical_total}")
        self.logger.info(f"Istanziazioni effettive: {actual_instantiations}")
        self.logger.info(f"Differenza: {actual_instantiations - theoretical_total}")
        self.logger.info(f"Errori riscontrati: {len(self.logger.error_paths)}")
        self.logger.info("=====================================")


# === OUTPUT VALIDATOR ===
class OutputValidator:
    """Validatore dell'output generato"""
    
    def __init__(self, config: Config, logger: AdvancedLogger):
        self.config = config
        self.logger = logger
    
    def validate_nquads_file(self, output_file: str) -> Dict[str, int]:
        """Valida il file N-Quads generato"""
        self.logger.info("Validazione dell'output in corso...")
        
        stats = {
            'total_lines': 0,
            'meaningful_lines': 0,
            'instantiation_types': 0,
            'inst_uris': 0,
            'location_links': 0,
            'hash_links': 0,
            'storage_locations': 0,
            'storage_mediums': 0,
            'stored_at_links': 0
        }
        
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    stats['total_lines'] += 1
                    line = line.strip()
                    
                    if not line or line.startswith('#'):
                        continue
                    
                    stats['meaningful_lines'] += 1
                    
                    # Conta istanziazioni
                    if 'rico/ontology#Instantiation' in line:
                        stats['instantiation_types'] += 1
                    
                    # Conta URI _inst
                    if '_inst>' in line:
                        stats['inst_uris'] += 1
                    
                    # Conta collegamenti location
                    if 'prov#atLocation' in line:
                        stats['location_links'] += 1
                    
                    #  CONTA ELEMENTI STORAGE
                    if 'premis/rdf/v3/StorageLocation' in line:
                        stats['storage_locations'] += 1
                    
                    if 'premis/rdf/v3/StorageMedium' in line:
                        stats['storage_mediums'] += 1
                    
                    if 'premis/rdf/v3/storedAt' in line:
                        stats['stored_at_links'] += 1
        
        except Exception as e:
            self.logger.error(f"Errore durante la validazione: {e}")
            return stats
        
        # Report validazione
        self.logger.info(f"Validazione completata:")
        self.logger.info(f"  - Righe totali: {stats['total_lines']}")
        self.logger.info(f"  - Righe significative: {stats['meaningful_lines']}")
        self.logger.info(f"  - Tipi istanziazione: {stats['instantiation_types']}")
        self.logger.info(f"  - URI _inst: {stats['inst_uris']}")
        self.logger.info(f"  - Collegamenti location: {stats['location_links']}")
        self.logger.info(f"  - Collegamenti hash: {stats['hash_links']}")
        self.logger.info(f"  - Storage locations: {stats['storage_locations']}")
        self.logger.info(f"  - Storage mediums: {stats['storage_mediums']}")
        self.logger.info(f"  - Collegamenti storedAt: {stats['stored_at_links']}")
        
        # Verifica consistenza
        if stats['instantiation_types'] == 0:
            self.logger.warning("ATTENZIONE: Nessun tipo istanziazione trovato!")
        
        if stats['instantiation_types'] != stats['location_links']:
            self.logger.warning(f"Inconsistenza: {stats['instantiation_types']} istanziazioni vs {stats['location_links']} location")
        
        # Verifica storage per root speciali
        expected_storage_entities = 3  # RS1_RS1, RS1_RS2, RS1_RS3
        if stats['storage_locations'] != expected_storage_entities:
            self.logger.warning(f"Storage locations: attese {expected_storage_entities}, trovate {stats['storage_locations']}")
        
        if stats['storage_mediums'] != expected_storage_entities:
            self.logger.warning(f"Storage mediums: attesi {expected_storage_entities}, trovati {stats['storage_mediums']}")
        
        return stats


# === MAIN ORCHESTRATOR ===
class StructureGenerator:
    """Orchestratore principale della generazione strutturale"""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = AdvancedLogger(config)
        self.namespace_manager = NamespaceManager(config)
        self.entity_builder = EntityBuilder(self.namespace_manager, self.logger)
        self.hash_manager = HashManager(self.logger)
        self.file_processor = FileSystemProcessor(
            config, self.logger, self.namespace_manager, 
            self.entity_builder, self.hash_manager
        )
        self.validator = OutputValidator(config, self.logger) if config.VALIDATE_OUTPUT else None
    
    def run(self):
        """Esecuzione principale"""
        self.logger.info("AVVIO GENERAZIONE STRUTTURALE RDF")
        self.logger.info(f"Directory root: {self.config.ROOT_PATH}")
        self.logger.info(f"Output file: {self.config.OUTPUT_FILE}")
        
        try:
            # 1. Validazione pattern ID (NUOVO)
            self.validate_id_patterns()
            
            # 2. Inizializzazione
            self._initialize()
            
            # 3. Processamento file system
            self._process_filesystem()
            
            # 4. Serializzazione
            self._serialize_output()
            
            # 5. Validazione (opzionale)
            if self.validator:
                self._validate_output()
            
            # 6. Report finale
            self._finalize()
            
        except Exception as e:
            self.logger.error(f"Errore fatale durante la generazione: {e}")
            raise
    
    def _initialize(self):
        """Inizializzazione componenti"""
        self.logger.info("Inizializzazione componenti...")
        
        # Crea algoritmo SHA-256
        self.algorithm_uri = self.entity_builder.create_sha256_algorithm()
        
         # Crea entry root
        root_folder_name = os.path.basename(self.config.ROOT_PATH)
        self.root_record_uri, _ = self.file_processor.process_entry(
            self.config.ROOT_PATH, root_folder_name, self.config.ROOT_ID, 
            None, 'folder', "", 0, self.algorithm_uri  # <-- Correzione: "" invece di root_folder_name
        )
        
        self.logger.info("Inizializzazione completata")
    
    def _process_filesystem(self):
        """Processamento del file system"""
        self.logger.info("Avvio processamento file system...")
        
        # Conta totale entry per progress tracking
        if self.config.DEBUG_MODE:
            total_entries = self.file_processor.count_total_entries(self.config.ROOT_PATH)
            self.logger.info(f"Entry totali da processare: {total_entries}")
        else:
            total_entries = None
        
        # Attraversamento ricorsivo
        processed = self.file_processor.traverse_directory(
            self.config.ROOT_PATH, self.config.ROOT_ID, 1, 
            self.algorithm_uri, total_entries
        )
        
        self.logger.info(f"Processamento completato: {processed} entry processate")
    
    def _serialize_output(self):
        """Serializzazione dell'output"""
        self.logger.info("Serializzazione output...")
        
        try:
            # Serializza dataset completo
            nquads_content = self.namespace_manager.dataset.serialize(format="nquads")
            
            # Scrivi file con encoding UTF-8
            with open(self.config.OUTPUT_FILE, 'w', encoding='utf-8') as f:
                f.write(nquads_content)
            
            file_size = os.path.getsize(self.config.OUTPUT_FILE)
            self.logger.info(f"Serializzazione completata: {file_size:,} bytes")
            
        except Exception as e:
            self.logger.error(f"Errore durante la serializzazione: {e}")
            raise
    
    def validate_id_patterns(self):
        """Valida che gli ID seguano i pattern corretti durante l'inizializzazione"""
        self.logger.info("Validazione pattern ID...")
        
        # Test con alcuni ID di esempio per verificare la logica
        test_ids = [
            ("RS1", "recordset", "root"),
            ("RS1_RS1", "recordset", "folder"),  
            ("RS1_RS1_R1", "record", "file"),
            ("RS1_RS1_RS2_R5", "record", "file"),
            ("RS1_RS1_RS2_RS3", "recordset", "folder")
        ]
        
        for test_id, expected, description in test_ids:
            result = determine_rico_type_from_id(test_id)
            status = "✅" if result == expected else "❌"
            self.logger.info(f"{status} {test_id} ({description}): {result}")
            
            if result != expected:
                self.logger.error(f"Pattern validation failed for {test_id}")
                raise ValueError(f"ID pattern logic error: {test_id}")
        
        self.logger.info("✅ Validazione pattern ID completata")
    
    def _validate_output(self):
        """Validazione dell'output"""
        validation_stats = self.validator.validate_nquads_file(self.config.OUTPUT_FILE)
        
        # Verifica consistenza con statistiche interne
        expected_instantiations = len(self.logger.instantiation_log)
        found_instantiations = validation_stats['instantiation_types']
        
        if expected_instantiations != found_instantiations:
            self.logger.warning(
                f"Inconsistenza istanziazioni: create {expected_instantiations}, "
                f"trovate {found_instantiations}"
            )
        else:
            self.logger.info("Validazione superata")
    
    def _finalize(self):
        """Finalizzazione e report"""
        self._add_rs1_container()
        duplicate_count = self.hash_manager.get_duplicate_count()
        unique_hashes = self.hash_manager.get_unique_hash_count()
        
        if duplicate_count > 0:
            self.logger.info(f"File duplicati rilevati: {duplicate_count}")
            self.logger.info(f"Hash unici: {unique_hashes}")
        
        # Debug dettagliato (se abilitato)
        if self.config.DEBUG_MODE:
            self.file_processor.debug_instantiation_count()
            self.logger.analyze_discrepancies()
        
        # Report finale
        self.logger.final_report()
        self.logger.info("GENERAZIONE STRUTTURALE COMPLETATA CON SUCCESSO")

    def _add_rs1_container(self):
        """Aggiunge RS1 come contenitore logico collegato alla root corrente"""
        self.logger.info("Aggiunta contenitore logico RS1...")
        
        # Crea RS1
        rs1_uri = self.namespace_manager.create_uri("RS1")
        self.entity_builder.add_triple(rs1_uri, RDF.type, self.namespace_manager.rico.RecordSet)
        self.entity_builder.add_triple(rs1_uri, RDFS.label, Literal("Archivio Valerio Evangelisti - Partizione Digitale"))
        
        # RecordSetType per RS1 (Archive Container)
        archive_type_uri = URIRef(f"{self.namespace_manager.config.BASE_URL}recordSetType/archive-container")
        self.entity_builder.add_triple(rs1_uri, self.namespace_manager.rico.hasRecordSetType, archive_type_uri)
        self.entity_builder.add_triple(archive_type_uri, self.namespace_manager.rico.isRecordSetTypeOf, rs1_uri)
        
        # Crea il tipo Archive Container se non esiste già
        existing_types = list(self.namespace_manager.graph.objects(archive_type_uri, RDF.type))
        if not any('RecordSetType' in str(t) for t in existing_types):
            self.entity_builder.add_triple(archive_type_uri, RDF.type, self.namespace_manager.rico.RecordSetType)
            self.entity_builder.add_triple(archive_type_uri, RDFS.label, Literal("Archive Container"))
        
        # Identifier per RS1
        identifier_uri = URIRef(f"{self.namespace_manager.config.BASE_URL}/RS1_id")
        identifier_type_uri = URIRef(f"{self.namespace_manager.config.BASE_URL}idType/unique-id")
        
        self.entity_builder.add_triple(rs1_uri, self.namespace_manager.rico.hasOrHadIdentifier, identifier_uri)
        self.entity_builder.add_triple(identifier_uri, RDF.type, self.namespace_manager.rico.Identifier)
        self.entity_builder.add_triple(identifier_uri, RDFS.label, Literal("RS1"))
        self.entity_builder.add_triple(identifier_uri, self.namespace_manager.rico.hasIdentifierType, identifier_type_uri)
        
        # Collegamenti gerarchici con la root corrente
        current_root_uri = self.namespace_manager.create_uri(self.config.ROOT_ID)
        self.entity_builder.add_triple(current_root_uri, self.namespace_manager.rico.isOrWasIncludedIn, rs1_uri)
        self.entity_builder.add_triple(rs1_uri, self.namespace_manager.rico.includesOrIncluded, current_root_uri)
        
        self.logger.info(f"RS1 collegato a {self.config.ROOT_ID}")

# === ARGUMENT PARSING ===
def parse_arguments():
    """Parser degli argomenti da riga di comando"""
    parser = argparse.ArgumentParser(
        description="Generatore strutturale RDF per l'Archivio Evangelisti",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Tipi di directory disponibili:
{chr(10).join([f"  {k}: {v['path']}" for k, v in DIRECTORY_CONFIGS.items()])}

Esempi di utilizzo:
  python {sys.argv[0]} --type floppy
  python {sys.argv[0]} --type hd_esterno
  python {sys.argv[0]} --custom-path /path/to/custom/dir --root-id RS1_CUSTOM
"""
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--type', 
        choices=list(DIRECTORY_CONFIGS.keys()),
        help='Tipo di directory predefinita da analizzare'
    )
    group.add_argument(
        '--custom-path',
        help='Percorso personalizzato da analizzare'
    )
    
    parser.add_argument(
        '--root-id',
        default='RS1_CUSTOM',
        help='ID root per percorso personalizzato (default: RS1_CUSTOM)'
    )
    
    parser.add_argument(
        '--output-suffix',
        default='custom',
        help='Suffisso per file di output personalizzato (default: custom)'
    )
    
    return parser.parse_args()


# === MAIN FUNCTION ===
def main():
    """Funzione principale"""
    args = parse_arguments()
    
    # Crea configurazione
    if args.type:
        config = Config.from_directory_type(args.type)
        print(f"Configurazione per tipo: {args.type} - evangelisti_structure_generation.py:1091")
    else:
        config = Config.from_custom_path(args.custom_path, args.root_id, args.output_suffix)
        print(f"Configurazione personalizzata per: {args.custom_path} - evangelisti_structure_generation.py:1094")
    
    # Verifica prerequisiti
    if not os.path.exists(config.ROOT_PATH):
        print(f"ERRORE: Directory root non trovata: {config.ROOT_PATH} - evangelisti_structure_generation.py:1098")
        sys.exit(1)
    
    print(f"Directory root: {config.ROOT_PATH} - evangelisti_structure_generation.py:1101")
    print(f"Root ID: {config.ROOT_ID} - evangelisti_structure_generation.py:1102")
    print(f"Output file: {config.OUTPUT_FILE} - evangelisti_structure_generation.py:1103")
    print(f"Log file: {config.LOG_FILE} - evangelisti_structure_generation.py:1104")
    
    try:
        # Crea e esegui il generatore
        generator = StructureGenerator(config)
        generator.run()
        
    except KeyboardInterrupt:
        print("\nOperazione interrotta dall'utente - evangelisti_structure_generation.py:1112")
        sys.exit(1)
    except Exception as e:
        print(f"ERRORE FATALE: {e} - evangelisti_structure_generation.py:1115")
        sys.exit(1)


if __name__ == "__main__":
    main()
