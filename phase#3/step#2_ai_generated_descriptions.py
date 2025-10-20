#!/usr/bin/env python3
"""
AI Technical Descriptions Generator - Generatore Descrizioni AI per Metadati Tecnici

🔍 Query Blazegraph with pagination + filter for non-anonymized Records

🛡️ Automatic exclusion of Instantiations of Records that are redactedInformation

🤖 Send metadata to Ollama for description generation (only visible Records)

📝 Create TechnicalDescription entity with generated description

🎭 Create "Text generation" Activity for each description

💻 Create/reuse Software entity for Ollama model

🔗 Complete bidirectional linking between all entities

✅ Add hasHumanValidation = false

💾 Insert into Blazegraph and/or export as N-Quads

RELATIONSHIPS CREATED:

Instantiation →(bodi:hasTechnicalDescription)→ TechnicalDescription

TechnicalDescription →(bodi:isTechnicalDescriptionOf)→ Instantiation

TechnicalDescription →(bodi:generatedBy)→ Activity

Activity →(bodi:hasGenerated)→ TechnicalDescription

Activity →(rico:isOrWasPerformedBy)→ Software

Software →(rico:performsOrPerformed)→ Activity

TechnicalDescription →(bodi:hasHumanValidation)→ "false"

"""

import argparse
import json
import logging
import requests
import sys
import time
import re
from datetime import datetime
from typing import Dict, List, Set, Tuple, Any, Optional
from dataclasses import dataclass
from urllib.parse import quote
import ollama
from pathlib import Path
import locale


# === ENHANCED BASE URIs SEGUENDO PATTERN METADATA EXTRACTION ===
BASE_URIS = {
    'ai_generated_desc': "http://ficlit.unibo.it/ArchivioEvangelisti/ai_generated_desc_",
    'ai_text_generation_activity': "http://ficlit.unibo.it/ArchivioEvangelisti/ai_textgen_activity_",
    'software': "http://ficlit.unibo.it/ArchivioEvangelisti/software_",
    'date': "http://ficlit.unibo.it/ArchivioEvangelisti/date_",
}

# === ENHANCED CONFIGURATION FOR PERSISTENCE ===
AI_COUNTERS_JSON_FILE = "ai_descriptions_uri_counters.json"

@dataclass
class InstantiationMetadata:
    """Struttura dati per metadati di un'Instantiation"""
    instantiation_uri: str
    file_path: str
    metadata_dict: Dict[str, List[str]]  # metadata_type -> [values]
    mime_type: Optional[str] = None
    file_size: Optional[str] = None
    hash_code: Optional[str] = None
    # 🆕 CAMPO PER TRACCIARE IL RECORD COLLEGATO
    related_record_uri: Optional[str] = None

@dataclass
class AIGeneratedDescription:
    """Struttura dati per descrizione AI generata - ENHANCED"""
    instantiation_uri: str
    ai_text_uri: str
    description: str
    model_used: str
    generation_timestamp: str
    metadata_count: int
    file_path: str
    # 🆕 ENHANCED FIELDS
    activity_uri: str = None
    software_uri: str = None
    has_human_validation: bool = False

@dataclass
class ProcessingResult:
    """Risultato del processo di generazione descrizioni AI - ENHANCED"""
    total_instantiations_processed: int = 0
    total_descriptions_generated: int = 0
    total_ai_text_entities_created: int = 0
    total_relationships_created: int = 0
    total_ollama_calls: int = 0
    total_tokens_used: int = 0
    processing_time_seconds: float = 0
    errors: List[str] = None
    nquads_file: str = ""
    total_nquads_written: int = 0
    ai_descriptions: List[AIGeneratedDescription] = None
    # 🆕 ENHANCED STATS
    total_activities_created: int = 0
    total_software_entities_created: int = 0
    # 🆕 FILTERED STATS
    total_instantiations_filtered_out: int = 0
    total_non_anonymized_instantiations: int = 0
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.ai_descriptions is None:
            self.ai_descriptions = []

class AITechnicalDescriptionGenerator:
    """Generatore di descrizioni AI per metadati tecnici delle Instantiation - ENHANCED VERSION CON FILTRO RECORD NON ANONIMIZZATI"""
    
    def __init__(self, 
                 blazegraph_endpoint: str = "http://localhost:10214/blazegraph/namespace/kb/sparql",
                 ollama_endpoint: str = "http://localhost:11434",
                 ollama_model: str = "llama3.2",
                 target_graph: str = "http://ficlit.unibo.it/ArchivioEvangelisti/ai_descriptions",
                 export_nquads: bool = False,
                 always_save_nquads: bool = True):
        
        self.blazegraph_endpoint = blazegraph_endpoint
        self.ollama_endpoint = ollama_endpoint
        self.ollama_model = ollama_model
        self.target_graph = target_graph
        self.export_nquads = export_nquads
        self.always_save_nquads = always_save_nquads
        
        # Setup HTTP session per Blazegraph
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/sparql-results+json',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'AITechnicalDescriptionGenerator/2.1-Filtered'
        })
        
        # Setup Ollama client
        self.ollama_client = ollama.Client(host=ollama_endpoint)
        
        # Prefissi RDF standard
        self.prefixes = """
        PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
        PREFIX bodi: <http://w3id.org/bodi#>
        PREFIX premis: <http://www.loc.gov/premis/rdf/v3/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dc: <http://purl.org/dc/terms/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        """
        
        self.logger = self._setup_logger()
        self.nquads_triples = []  # Buffer per N-Quads

        self.checkpoint_file = "ai_descriptions_checkpoint.json"
        self.processed_instantiations = set()
        self.last_checkpoint_count = 0
        
        # 🆕 ENHANCED: Contatori e cache per URI strutturati
        self.software_counter = 3
        self.activity_counter = 0
        self.ai_text_counter = 0  # 🆕 Contatore per AI Generated Text
        self.software_cache = {}  # {model_name: software_uri}
        self.model_documentation_cache = {}  # {model_name: doc_url}
        
        # Carica contatori esistenti
        self._load_ai_counters_from_json()

        # 🆕 TIMESTAMP E ENTITÀ CONDIVISE (COME NEL METADATA EXTRACTION)
        self.generation_timestamp = datetime.now()
        self.generation_date_str = self.generation_timestamp.isoformat()
        
        # Configura locale per formato inglese (come nel metadata extraction)
        try:
            locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
        except locale.Error:
            # Fallback silenzioso se locale non disponibile
            pass
            
        # Usa sempre formato inglese per consistenza
        self.generation_date_expr = self.generation_timestamp.strftime("%d %B %Y at %H:%M")
        
        # URI entità condivise
        self.lucia_person_uri = "http://ficlit.unibo.it/ArchivioEvangelisti/person_LuciaGiagnolini"
        self.generation_date_uri = f"{BASE_URIS['date']}{self.generation_timestamp.strftime('%Y%m%d_%H%M%S')}"
        
    def _setup_logger(self):
        """Setup logger con formato dettagliato"""
        logging.basicConfig(
            level=logging.INFO,
            format='[%(levelname)s] %(asctime)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        return logging.getLogger('AITechnicalDescriptionGenerator-Filtered')
    
    # 🆕 ENHANCED: Gestione persistenza contatori per AI descriptions
    def _load_ai_counters_from_json(self):
        """Carica i contatori AI da file JSON se esiste"""
        if not Path(AI_COUNTERS_JSON_FILE).exists():
            self.logger.info(f"File contatori AI non trovato: {AI_COUNTERS_JSON_FILE}")
            self.logger.info(f"Inizializzazione con contatori vuoti")
            return
        
        try:
            with open(AI_COUNTERS_JSON_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Carica contatori
            loaded_software_counter = data.get('software_counter', 3)  # Default 3 per partire da 0004
            self.software_counter = max(loaded_software_counter, 3) 
            self.activity_counter = data.get('activity_counter', 0)
            self.ai_text_counter = data.get('ai_text_counter', 0)  # 🆕
            
            # Carica cache software
            software_cache_data = data.get('software_cache', {})
            for model_name, uri_str in software_cache_data.items():
                self.software_cache[model_name] = uri_str
            
            self.model_documentation_cache = data.get('model_documentation_cache', {})
        
            self.logger.info(f"✅ Contatori AI caricati: software={self.software_counter}, activity={self.activity_counter}")
            if self.software_counter == 3:
                self.logger.info(f"🎯 Software counter impostato a 3 - primo URI sarà software_0004")
        
        except Exception as e:
            self.logger.error(f"❌ Errore caricamento contatori AI: {e}")
            # 🆕 FALLBACK: Imposta a 3 in caso di errore
            self.software_counter = 3

    
    def _save_ai_counters_to_json(self):
        """Salva i contatori AI su file JSON"""
        try:
            data = {
                'software_counter': self.software_counter,
                'activity_counter': self.activity_counter,
                'ai_text_counter': self.ai_text_counter,  # 🆕
                'software_cache': self.software_cache,
                'model_documentation_cache': self.model_documentation_cache,
                'last_updated': datetime.now().isoformat(),
                'metadata': {
                    'total_software_entities': len(self.software_cache),
                    'total_activities_generated': self.activity_counter,
                    'total_ai_texts_generated': self.ai_text_counter  # 🆕
                }
            }
            
            with open(AI_COUNTERS_JSON_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"💾 Contatori AI salvati: {AI_COUNTERS_JSON_FILE}")
            
        except Exception as e:
            self.logger.error(f"❌ Errore salvataggio contatori AI: {e}")

    def load_checkpoint(self) -> Set[str]:
        """Carica instantiation già processate da checkpoint file"""
        if not Path(self.checkpoint_file).exists():
            self.logger.info("📁 Nessun checkpoint trovato, inizio da zero")
            return set()
        
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            processed_set = set(data.get('processed_instantiations', []))
            last_update = data.get('last_updated', 'unknown')
            
            self.logger.info(f"📁 Checkpoint caricato: {len(processed_set)} instantiation già processate")
            self.logger.info(f"📅 Ultimo aggiornamento: {last_update}")
            
            return processed_set
            
        except Exception as e:
            self.logger.error(f"❌ Errore caricamento checkpoint: {e}")
            return set()

    def save_checkpoint(self, processed_instantiations: Set[str]):
        """Salva checkpoint delle instantiation processate"""
        try:
            data = {
                'processed_instantiations': list(processed_instantiations),
                'last_updated': datetime.now().isoformat(),
                'total_processed': len(processed_instantiations),
                'checkpoint_version': '1.0'
            }
            
            # Backup del checkpoint precedente
            if Path(self.checkpoint_file).exists():
                backup_file = f"{self.checkpoint_file}.backup"
                Path(self.checkpoint_file).rename(backup_file)
            
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"💾 Checkpoint salvato: {len(processed_instantiations)} instantiation")
            
        except Exception as e:
            self.logger.error(f"❌ Errore salvataggio checkpoint: {e}")

    def get_comprehensive_existing_descriptions(self) -> Set[str]:
        """Combina descrizioni esistenti da Blazegraph + checkpoint locale"""
        
        # 1. Carica da checkpoint locale (veloce)
        checkpoint_processed = self.load_checkpoint()
        
        # 2. Query Blazegraph per source of truth (potrebbe essere lenta)
        if not self.export_nquads:
            self.logger.info("🔍 Verificando descrizioni esistenti in Blazegraph...")
            blazegraph_processed = self.get_existing_ai_descriptions()
            
            # Combina entrambi
            all_processed = checkpoint_processed.union(blazegraph_processed)
            
            # Debug differenze
            only_checkpoint = checkpoint_processed - blazegraph_processed
            only_blazegraph = blazegraph_processed - checkpoint_processed
            
            if only_checkpoint:
                self.logger.warning(f"⚠️ {len(only_checkpoint)} instantiation nel checkpoint ma non in Blazegraph")
            if only_blazegraph:
                self.logger.info(f"ℹ️ {len(only_blazegraph)} instantiation in Blazegraph ma non nel checkpoint")
            
            return all_processed
        else:
            # Modalità export: usa solo checkpoint
            return checkpoint_processed
    
    # 🆕 ENHANCED: Funzioni per creare Software e Activity seguendo pattern metadata extraction
    def get_or_create_software_entity(self, model_name: str) -> str:
        """Crea o recupera Software entity per modello Ollama con URI strutturato"""
        
        # Normalizza nome modello per cache key E label consistente
        normalized_model = model_name.lower().strip()
        canonical_label = self._get_canonical_model_label(normalized_model)
        
        # Controlla cache
        if normalized_model in self.software_cache:
            software_uri = self.software_cache[normalized_model]
            self.logger.debug(f"[SOFTWARE-CACHE] Using cached: {canonical_label} -> {software_uri}")
            return software_uri
        
        # Crea nuovo Software entity
        self.software_counter += 1
        counter_str = f"{self.software_counter:04d}"
        
        software_uri = f"{BASE_URIS['software']}{counter_str}"
        self.software_cache[normalized_model] = software_uri
        
        # Genera URL documentazione
        doc_url = self._generate_ollama_doc_url(model_name)
        if doc_url:
            self.model_documentation_cache[normalized_model] = doc_url
        
        self.logger.info(f"[SOFTWARE-NEW] {canonical_label} -> software_{counter_str}")
        
        # Salva periodicamente
        if self.software_counter % 5 == 0:
            self._save_ai_counters_to_json()
        
        return software_uri
    
    def _get_canonical_model_label(self, normalized_model: str) -> str:
        """Ottiene label canonica per modello normalizzato"""
        # Mappa modelli comuni alla loro forma canonica
        canonical_models = {
            'llama3.2': 'Llama 3.2',
            'llama3.1': 'Llama 3.1', 
            'llama3': 'Llama 3',
            'llama2': 'Llama 2',
            'mistral': 'Mistral',
            'codellama': 'Code Llama',
            'phi3': 'Phi-3',
            'gemma': 'Gemma'
        }
        
        # Rimuovi versioni specifiche se presenti per matching
        base_model = normalized_model.split(':')[0]
        
        # Usa mapping canonico se disponibile, altrimenti capitalizza
        if base_model in canonical_models:
            canonical = canonical_models[base_model]
            # Se c'era una versione specifica, aggiungila
            if ':' in normalized_model:
                version = normalized_model.split(':')[1]
                canonical += f":{version}"
            return canonical
        else:
            # Fallback: capitalizza prima lettera
            return base_model.capitalize()
    
    def _generate_ollama_doc_url(self, model_name: str) -> str:
        """Genera URL documentazione Ollama per modello"""
        try:
            # Normalizza nome modello per URL Ollama
            clean_model = model_name.lower().strip()
            
            # Rimuovi versioni specifiche se presenti
            if ':' in clean_model:
                clean_model = clean_model.split(':')[0]
            
            # Mappa modelli comuni
            model_mapping = {
                'llama3.2': 'llama3.2',
                'llama3.1': 'llama3.1', 
                'llama3': 'llama3',
                'llama2': 'llama2',
                'mistral': 'mistral',
                'codellama': 'codellama',
                'phi3': 'phi3',
                'gemma': 'gemma'
            }
            
            ollama_model_name = model_mapping.get(clean_model, clean_model)
            return f"https://ollama.com/library/{ollama_model_name}"
            
        except Exception as e:
            self.logger.warning(f"⚠️ Impossibile generare URL doc per {model_name}: {e}")
            return "https://ollama.com/"
    
    def create_text_generation_activity(self, ai_text_uri: str, software_uri: str) -> str:
        """Crea Activity per generazione testo con URI strutturato e data"""
        
        # Estrai identificatore da ai_text_uri per creare URI activity correlato
        try:
            ai_text_id = ai_text_uri.split('_')[-1]  # Prende timestamp dalla fine
        except:
            ai_text_id = str(int(time.time()))
        
        self.activity_counter += 1
        counter_str = f"{self.activity_counter:04d}"
        
        activity_uri = f"{BASE_URIS['ai_text_generation_activity']}{counter_str}_{ai_text_id}"
        
        self.logger.debug(f"[ACTIVITY-NEW] Text generation -> activity_{counter_str}_{ai_text_id}")
        
        return activity_uri
    
    def test_blazegraph_connection(self) -> bool:
        """Testa connessione a Blazegraph"""
        if self.export_nquads:
            self.logger.info("✅ Modalità N-Quads - connessione Blazegraph per query solamente")
        
        try:
            query = self.prefixes + "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
            response = self.session.post(
                self.blazegraph_endpoint,
                data={'query': query},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                count = int(result["results"]["bindings"][0]["count"]["value"])
                self.logger.info(f"✅ Blazegraph OK - Triple nel dataset: {count:,}")
                return True
            else:
                self.logger.error(f"❌ Blazegraph fallito: HTTP {response.status_code}")
                return False
                
        except requests.exceptions.ConnectionError:
            self.logger.error(f"❌ Blazegraph non raggiungibile: {self.blazegraph_endpoint}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Errore Blazegraph: {e}")
            return False
    
    def test_ollama_connection(self) -> bool:
        """Testa connessione a Ollama e modello specificato"""
        try:
            # Test connessione base
            models_response = self.ollama_client.list()
            
            # Gestisci diverse strutture di risposta
            available_models = []
            
            # Caso 1: Oggetto ListResponse di Ollama (versioni recenti)
            if hasattr(models_response, 'models'):
                models_list = models_response.models
                
                for model_obj in models_list:
                    # Estrai nome del modello dall'oggetto Model
                    model_name = None
                    if hasattr(model_obj, 'model'):
                        model_name = model_obj.model
                    elif hasattr(model_obj, 'name'):
                        model_name = model_obj.name
                    elif isinstance(model_obj, str):
                        model_name = model_obj
                    
                    if model_name:
                        # Aggiungi sia il nome completo che quello senza tag
                        clean_name = model_name.split(':')[0]
                        available_models.append(clean_name)
                        available_models.append(model_name)
            
            # Caso 2: Struttura standard con 'models' dict key (versioni vecchie)
            elif isinstance(models_response, dict) and 'models' in models_response:
                for model in models_response['models']:
                    model_name = None
                    if isinstance(model, dict):
                        # Prova diverse chiavi possibili
                        for key in ['name', 'model', 'id', 'NAME']:
                            if key in model:
                                model_name = model[key]
                                break
                    elif isinstance(model, str):
                        model_name = model
                    
                    if model_name:
                        clean_name = model_name.split(':')[0]
                        available_models.append(clean_name)
                        available_models.append(model_name)
            
            # Rimuovi duplicati mantenendo l'ordine
            available_models = list(dict.fromkeys(available_models))
            
            self.logger.info(f"✅ Ollama OK - Modelli disponibili: {len(available_models)}")
            if available_models:
                self.logger.info(f"   Modelli: {', '.join(available_models[:5])}")
                if len(available_models) > 5:
                    self.logger.info(f"   ... e altri {len(available_models) - 5}")
            
            # Verifica modello specificato
            model_found = False
            actual_model_name = self.ollama_model
            
            # Cerca esatta corrispondenza
            if self.ollama_model in available_models:
                model_found = True
            # Cerca con :latest
            elif f"{self.ollama_model}:latest" in available_models:
                model_found = True
                actual_model_name = f"{self.ollama_model}:latest"
            # Cerca corrispondenze parziali
            else:
                for model in available_models:
                    if model.startswith(self.ollama_model):
                        model_found = True
                        actual_model_name = model
                        break
            
            if model_found:
                self.logger.info(f"✅ Modello '{actual_model_name}' disponibile")
                
                # Test generazione semplice
                test_response = self.ollama_client.generate(
                    model=actual_model_name,
                    prompt="Test connection. Respond with 'OK'.",
                    options={'num_predict': 5}
                )
                
                if test_response and 'response' in test_response:
                    response_text = test_response['response'].strip()
                    self.logger.info(f"✅ Test generazione riuscito: '{response_text}'")
                    # Aggiorna il nome del modello per uso futuro
                    self.ollama_model = actual_model_name
                    return True
                else:
                    self.logger.error("❌ Test generazione fallito - nessuna risposta valida")
                    return False
            else:
                self.logger.error(f"❌ Modello '{self.ollama_model}' non disponibile")
                if available_models:
                    self.logger.info(f"   Modelli disponibili: {', '.join(available_models)}")
                else:
                    self.logger.info("   Nessun modello trovato. Prova: ollama pull llama3.2")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Errore Ollama: {e}")
            self.logger.info("   Possibili soluzioni:")
            self.logger.info("   1. Verifica che Ollama sia in esecuzione: ollama serve")
            self.logger.info("   2. Installa un modello: ollama pull llama3.2")
            self.logger.info("   3. Verifica endpoint: curl http://localhost:11434/api/tags")
            return False
    
    def get_instantiations_with_metadata(self, limit: Optional[int] = None, page_size: int = 100) -> List[InstantiationMetadata]:
        """
        🆕 FILTRATO: Recupera Instantiation SOLAMENTE di Record che NON hanno redactedInformation = "yes"
        """
        self.logger.info("🔍 RECUPERO INSTANTIATION FILTRATE (SOLO RECORD SENZA redactedInformation)...")
        self.logger.info("🛡️ FILTRO: Esclusione automatica Record che HANNO la proprietà redactedInformation (qualsiasi valore)")
        
        all_instantiations_data = {}
        total_retrieved = 0
        total_filtered_out = 0
        page = 0
        
        if limit:
            max_pages = (limit + page_size - 1) // page_size
            self.logger.info(f"🎯 Limite richiesto: {limit} (usando {max_pages} pagine di {page_size})")
        else:
            self.logger.info(f"🌍 PROCESSAMENTO COMPLETO DATABASE FILTRATO (query ottimizzate, pagine di {page_size})")
        
        # 🔍 FASE 1: Query totale per statistiche pre-filtro
        self.logger.info("📊 Conteggio totale Instantiation nel database...")
        
        total_instantiations_query = self.prefixes + """
        SELECT (COUNT(DISTINCT ?instantiation) as ?totalCount) WHERE {
            ?instantiation rdf:type rico:Instantiation .
        }
        """
        
        try:
            response = self.session.post(
                self.blazegraph_endpoint,
                data={'query': total_instantiations_query},
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                total_instantiations = int(result["results"]["bindings"][0]["totalCount"]["value"])
                self.logger.info(f"📊 Totale Instantiation nel database: {total_instantiations:,}")
            else:
                total_instantiations = 0
                self.logger.warning("⚠️ Impossibile conteggiare Instantiation totali")
        except Exception as e:
            total_instantiations = 0
            self.logger.warning(f"⚠️ Errore conteggio totale: {e}")
        
        # FASE 2: Recupera Instantiation CON FILTRO RECORD NON ANONIMIZZATI
        while True:
            page += 1
            offset = (page - 1) * page_size
            
            current_page_size = page_size
            if limit and total_retrieved + page_size > limit:
                current_page_size = limit - total_retrieved
            
            self.logger.info(f"   📄 Pagina {page}: recupero {current_page_size} Instantiation filtrate (offset: {offset})")
            
            # 🔒 QUERY FILTRATA - INCLUDI SOLO RECORD CHE NON HANNO redactedInformation
            filtered_query = self.prefixes + f"""
            SELECT DISTINCT ?instantiation ?relatedRecord WHERE {{
                ?instantiation rdf:type rico:Instantiation .
                            
                    ?relatedRecord rico:hasOrHadInstantiation ?instantiation .
                    ?relatedRecord a rico:Record .

                FILTER NOT EXISTS {{
                    ?relatedRecord bodi:redactedInformation "yes" .
                }}
            }}
            ORDER BY ?instantiation
            LIMIT {current_page_size}
            OFFSET {offset}
            """
            
            try:
                start_time = time.time()
                response = self.session.post(
                    self.blazegraph_endpoint,
                    data={'query': filtered_query},
                    timeout=300  # 5 minuti per query filtrata
                )
                query_time = time.time() - start_time
                
                if response.status_code != 200:
                    self.logger.error(f"❌ Query filtrata pagina {page} fallita: HTTP {response.status_code}")
                    break
                
                result_data = response.json()
                bindings = result_data.get("results", {}).get("bindings", [])
                
                self.logger.info(f"      ✅ Pagina {page} filtrata completata in {query_time:.2f}s - {len(bindings)} Instantiation senza redactedInformation")
                
                if not bindings:
                    self.logger.info(f"   🏁 Fine dataset raggiunta alla pagina {page}")
                    break
                
                # Inizializza strutture dati per questa pagina
                page_instantiations = []
                for binding in bindings:
                    instantiation_uri = binding["instantiation"]["value"]
                    file_path = binding.get("filePath", {}).get("value", "Path non disponibile")
                    related_record_uri = binding.get("relatedRecord", {}).get("value", None)
                    
                    all_instantiations_data[instantiation_uri] = {
                        'file_path': file_path,
                        'metadata_dict': {},
                        'hash_code': None,
                        'related_record_uri': related_record_uri
                    }
                    page_instantiations.append(instantiation_uri)
                
                total_retrieved += len(page_instantiations)
                
                # Calcola filtrate se abbiamo il totale
                if total_instantiations > 0:
                    estimated_filtered = max(0, total_instantiations - total_retrieved - (offset - total_retrieved))
                    self.logger.info(f"      📈 Instantiation non anonimizzate recuperate: {total_retrieved}")
                    if estimated_filtered > 0:
                        self.logger.info(f"      🔒 Instantiation stimate filtrate (Record con redactedInformation): ~{estimated_filtered}")
                else:
                    self.logger.info(f"      📈 Instantiation senza redactedInformation recuperate: {total_retrieved}")
                
                # FASE 3: Recupera metadati per questa pagina (INVARIATO)
                if page_instantiations:
                    self.logger.info(f"      🔍 Recupero metadati per {len(page_instantiations)} Instantiation senza redactedInformation...")
                    
                    instantiation_filter = " ".join([f"<{uri}>" for uri in page_instantiations])
                    
                    metadata_query = self.prefixes + f"""
                    SELECT ?instantiation ?metadataType ?metadataValue WHERE {{
                        VALUES ?instantiation {{ {instantiation_filter} }}
                        ?instantiation bodi:hasTechnicalMetadata ?metadata .
                        ?metadata bodi:hasTechnicalMetadataType ?metadataTypeEntity .
                        ?metadataTypeEntity rdfs:label ?metadataType .
                        ?metadata rdf:value ?metadataValue .
                        FILTER(STRLEN(STR(?metadataValue)) > 0)
                    }}
                    """
                    
                    try:
                        metadata_start = time.time()
                        metadata_response = self.session.post(
                            self.blazegraph_endpoint,
                            data={'query': metadata_query},
                            timeout=180
                        )
                        metadata_time = time.time() - metadata_start
                        
                        if metadata_response.status_code == 200:
                            metadata_data = metadata_response.json()
                            metadata_bindings = metadata_data.get("results", {}).get("bindings", [])
                            
                            # Aggiungi metadati
                            for binding in metadata_bindings:
                                instantiation_uri = binding["instantiation"]["value"]
                                metadata_type = binding["metadataType"]["value"]
                                metadata_value = binding["metadataValue"]["value"]
                                
                                if instantiation_uri in all_instantiations_data:
                                    if metadata_type not in all_instantiations_data[instantiation_uri]['metadata_dict']:
                                        all_instantiations_data[instantiation_uri]['metadata_dict'][metadata_type] = []
                                    all_instantiations_data[instantiation_uri]['metadata_dict'][metadata_type].append(metadata_value)
                            
                            self.logger.info(f"      ✅ Metadati recuperati in {metadata_time:.2f}s - {len(metadata_bindings)} campi")
                        
                    except Exception as e:
                        self.logger.warning(f"      ⚠️ Errore recupero metadati: {e}")
                    
                    # FASE 4: Recupera hash per questa pagina (INVARIATO)
                    hash_query = self.prefixes + f"""
                    SELECT ?instantiation ?hashValue WHERE {{
                        VALUES ?instantiation {{ {instantiation_filter} }}
                        ?instantiation bodi:hasHashCode ?fixity .
                        ?fixity rdf:value ?hashValue .
                    }}
                    """
                    
                    try:
                        hash_response = self.session.post(
                            self.blazegraph_endpoint,
                            data={'query': hash_query},
                            timeout=60
                        )
                        
                        if hash_response.status_code == 200:
                            hash_data = hash_response.json()
                            hash_bindings = hash_data.get("results", {}).get("bindings", [])
                            
                            for binding in hash_bindings:
                                instantiation_uri = binding["instantiation"]["value"]
                                hash_value = binding["hashValue"]["value"]
                                
                                if instantiation_uri in all_instantiations_data:
                                    all_instantiations_data[instantiation_uri]['hash_code'] = hash_value
                    
                    except Exception as e:
                        self.logger.debug(f"      Hash query error (non critico): {e}")
                
                # Controlli limite
                if limit and total_retrieved >= limit:
                    self.logger.info(f"   🎯 Limite {limit} raggiunto")
                    break
                
                if len(page_instantiations) < current_page_size:
                    self.logger.info(f"   🏁 Ultima pagina rilevata")
                    break
                
                # Pausa ottimizzata
                if page % 5 == 0:
                    self.logger.info(f"   ⏱️ Pausa 1s dopo {page} pagine...")
                    time.sleep(1)
                    
            except Exception as e:
                self.logger.error(f"❌ Errore durante pagina {page}: {e}")
                break
        
        # 🆕 CALCOLO STATISTICHE FILTRO
        total_filtered_out = max(0, total_instantiations - total_retrieved) if total_instantiations > 0 else 0
        
        # Converti in oggetti (ENHANCED CON RECORD URI)
        instantiation_objects = []
        for instantiation_uri, data in all_instantiations_data.items():
            mime_type = None
            file_size = None
            
            for mt in ['Content-Type', 'Content-Type-Parser-Override', 'MIMEType']:
                if mt in data['metadata_dict']:
                    mime_type = data['metadata_dict'][mt][0]
                    break
            
            for fs in ['st_size', 'File Size', 'FileSize', 'Content-Length']:
                if fs in data['metadata_dict']:
                    file_size = data['metadata_dict'][fs][0]
                    break
            
            instantiation_obj = InstantiationMetadata(
                instantiation_uri=instantiation_uri,
                file_path=data['file_path'],
                metadata_dict=data['metadata_dict'],
                mime_type=mime_type,
                file_size=file_size,
                hash_code=data['hash_code'],
                related_record_uri=data['related_record_uri']  # 🆕 CAMPO RECORD
            )
            
            instantiation_objects.append(instantiation_obj)
        
        if limit and len(instantiation_objects) > limit:
            instantiation_objects = instantiation_objects[:limit]
        
        # 🆕 REPORT FINALE FILTRO
        self.logger.info(f"✅ Recupero filtrato completato")
        self.logger.info(f"📊 RISULTATI FINALI CON FILTRO:")
        self.logger.info(f"   📄 Pagine processate: {page}")
        self.logger.info(f"   🌍 Totale Instantiation database: {total_instantiations:,}")
        self.logger.info(f"   ✅ Instantiation SENZA redactedInformation: {len(instantiation_objects):,}")
        if total_filtered_out > 0:
            filter_percentage = (total_filtered_out / total_instantiations) * 100 if total_instantiations > 0 else 0
            self.logger.info(f"   🔒 Instantiation FILTRATE (Record con redactedInformation): {total_filtered_out:,} ({filter_percentage:.1f}%)")
        
        if instantiation_objects:
            total_metadata_fields = sum(len(inst.metadata_dict) for inst in instantiation_objects)
            avg_metadata_per_inst = total_metadata_fields / len(instantiation_objects) if instantiation_objects else 0
            self.logger.info(f"   📋 Campi metadati totali (non anonimizzati): {total_metadata_fields:,}")
            self.logger.info(f"   📊 Media metadati per Instantiation: {avg_metadata_per_inst:.1f}")
        
        return instantiation_objects
    
    def get_existing_ai_descriptions(self) -> Set[str]:
        """Ottiene le Instantiation che hanno già descrizioni AI"""
        if self.export_nquads:
            return set()
        
        self.logger.info("🔍 CONTROLLO DESCRIZIONI AI ESISTENTI...")
        
        query = self.prefixes + """
        SELECT ?instantiation WHERE {
            ?instantiation bodi:hasTechnicalDescription ?aiText .
            ?aiText rdf:type bodi:TechnicalDescription .
        }
        """
        
        try:
            response = self.session.post(
                self.blazegraph_endpoint,
                data={'query': query},
                timeout=120
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Query descrizioni esistenti fallita: HTTP {response.status_code}")
                return set()
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            existing_instantiations = set()
            for binding in bindings:
                instantiation_uri = binding["instantiation"]["value"]
                existing_instantiations.add(instantiation_uri)
            
            self.logger.info(f"✅ Instantiation con descrizioni AI esistenti: {len(existing_instantiations)}")
            return existing_instantiations
            
        except Exception as e:
            self.logger.error(f"❌ Errore controllo descrizioni esistenti: {e}")
            return set()
    
    def generate_ai_description(self, instantiation_metadata: InstantiationMetadata) -> Optional[str]:
        """Genera descrizione AI per un'Instantiation tramite Ollama"""
        
        # Costruisci prompt con tutti i metadati
        metadata_summary = self._build_metadata_summary(instantiation_metadata)
        
        prompt = f"""Based on these technical metadata, write a concise description of this digital file for archival purposes.

        File: {instantiation_metadata.file_path}
        {metadata_summary}

        Describe in 2-3 sentences: file type, format, key technical properties, and creation/modification details. Only include information that is available in the metadata. Do not mention metadata fields' names in your response. If the author information is different from Valerio, Evangelisti or Valerio Evangelisti, do not cite them."""
                
        try:
            self.logger.debug(f"🤖 Generating description for {instantiation_metadata.instantiation_uri.split('/')[-1]}")
            
            response = self.ollama_client.generate(
                model=self.ollama_model,
                prompt=prompt,
                options={
                    'temperature': 0.3,  # Più deterministico
                    'num_predict': 150,  # Limite ragionevole per descrizioni concise
                    'top_p': 0.9
                }
            )
            
            if response and 'response' in response:
                description = response['response'].strip()
                
                # Pulisci la descrizione
                description = self._clean_description(description)
                
                if description:
                    self.logger.debug(f"✅ Descrizione generata: {len(description)} caratteri")
                    return description
                else:
                    self.logger.warning(f"⚠️ Descrizione vuota generata")
                    return None
            else:
                self.logger.error(f"❌ Risposta Ollama non valida")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Errore generazione AI: {e}")
            return None
    
    def _build_metadata_summary(self, instantiation_metadata: InstantiationMetadata) -> str:
        """Costruisce riassunto leggibile dei metadati"""
        lines = []
        
        # Informazioni chiave prima
        if instantiation_metadata.mime_type:
            lines.append(f"MIME Type: {instantiation_metadata.mime_type}")
        
        if instantiation_metadata.file_size:
            lines.append(f"File Size: {instantiation_metadata.file_size}")
        
        if instantiation_metadata.hash_code:
            lines.append(f"Hash: {instantiation_metadata.hash_code[:16]}...")
        
        # Raggruppa metadati per categoria
        important_fields = [
            'ImageWidth', 'ImageHeight', 'tiff:ImageWidth', 'tiff:ImageLength',
            'Duration', 'AudioChannels', 'AudioSampleRate', 'VideoFrameRate',
            'Pages', 'meta:page-count', 'Words', 'meta:word-count',
            'Creator', 'dc:creator', 'Author', 'Title', 'dc:title',
            'CreateDate', 'dcterms:created', 'ModifyDate', 'dcterms:modified'
        ]
        
        # Aggiungi metadati importanti
        for field in important_fields:
            if field in instantiation_metadata.metadata_dict:
                values = instantiation_metadata.metadata_dict[field]
                if values:
                    lines.append(f"{field}: {values[0]}")
        
        # Aggiungi altri metadati (limitati)
        other_fields = []
        for field, values in instantiation_metadata.metadata_dict.items():
            if field not in important_fields and values and len(other_fields) < 10:
                other_fields.append(f"{field}: {values[0]}")
        
        lines.extend(other_fields)
        
        # Limita lunghezza totale
        summary = "\n".join(lines)
        if len(summary) > 3000:  # Limite per evitare prompt troppo lunghi
            summary = summary[:3000] + "..."
        
        return summary
    
    def _clean_description(self, description: str) -> str:
        """Pulisce e normalizza la descrizione generata"""
        # Rimuovi newline extra e spazi
        description = re.sub(r'\s+', ' ', description)
        description = description.strip()
        
        # Rimuovi virgolette se l'intera descrizione è quotata
        if description.startswith('"') and description.endswith('"'):
            description = description[1:-1]
        
        # Ensure it ends with punctuation
        if description and not description[-1] in '.!?':
            description += '.'
        
        return description
    
    def generate_ai_text_uri(self, instantiation_uri: str) -> str:
        """Genera URI per entità TechnicalDescription seguendo pattern metadata extraction"""
        
        # 🔢 COUNTER STRATEGY - Segue pattern metadata extraction
        self.ai_text_counter += 1
        counter_str = f"{self.ai_text_counter:06d}"  # Zero-padding come metadata extraction
        
        # URI strutturato con contatore globale
        ai_text_uri = f"{BASE_URIS['ai_generated_desc']}{counter_str}"
        
        self.logger.debug(f"[AI-TEXT-NEW] Generated: ai_generated_desc_{counter_str}")
        
        # Salva periodicamente ogni 25 AI texts (come negli altri contatori)
        if self.ai_text_counter % 25 == 0:
            self._save_ai_counters_to_json()
            self.logger.debug(f"[AI-TEXT-COUNTER] Checkpoint salvato: {self.ai_text_counter} AI texts generated")
        
        return ai_text_uri
    
    def generate_ai_description_triples(self, ai_descriptions: List[AIGeneratedDescription]) -> List[str]:
        """Genera triple RDF per le descrizioni AI e le relazioni - CON URI COMPLETE"""
        self.logger.info("📝 GENERAZIONE TRIPLE DESCRIZIONI AI CON URI COMPLETE...")
        
        triples = []
        
        # 🔧 URI NAMESPACE COMPLETE (come nel metadata extraction)
        RICO_NS = "https://www.ica.org/standards/RiC/ontology#"
        RICO_BODI_NS = "http://example.org/rico-bodi#"
        RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#"
        DCTERMS_NS = "http://purl.org/dc/terms/"
        XSD_NS = "http://www.w3.org/2001/XMLSchema#"
        
        # 🆕 USA ENTITÀ DATE CONDIVISA GIÀ INIZIALIZZATA
        date_uri = self.generation_date_uri
        date_str = self.generation_date_str
        date_expr = self.generation_date_expr

        # 🔧 Triple per l'entità Date CON URI COMPLETE (SENZA TIPO XSD)
        triples.append(f'<{date_uri}> <{RDF_NS}type> <{RICO_NS}Date> .')
        triples.append(f'<{date_uri}> <{RICO_NS}normalizedDateValue> "{date_str}" .')
        triples.append(f'<{date_uri}> <{RICO_NS}expressedDate> "{date_expr}" .')
        
        # 🔧 ENTITÀ PERSONA CON URI COMPLETE
        lucia_person_uri = "http://ficlit.unibo.it/ArchivioEvangelisti/person_LuciaGiagnolini"
        triples.append(f'<{lucia_person_uri}> <{RDF_NS}type> <{RICO_NS}Person> .')
        triples.append(f'<{lucia_person_uri}> <{RDFS_NS}label> "Lucia Giagnolini" .')
        
        for desc in ai_descriptions:
            # Escape della descrizione per RDF
            escaped_description = desc.description.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
            
            # Crea Software e Activity URIs
            software_uri = self.get_or_create_software_entity(desc.model_used)
            activity_uri = self.create_text_generation_activity(desc.ai_text_uri, software_uri)
            
            # Aggiorna il dataclass
            desc.software_uri = software_uri
            desc.activity_uri = activity_uri
            
            # 🔧 === TRIPLE PER TechnicalDescription CON URI COMPLETE ===
            triples.append(f'<{desc.ai_text_uri}> <{RDF_NS}type> <{RICO_BODI_NS}TechnicalDescription> .')
            triples.append(f'<{desc.ai_text_uri}> <{RDF_NS}value> "{escaped_description}" .')
            triples.append(f'<{desc.ai_text_uri}> <{RICO_BODI_NS}hasHumanValidation> "false"^^<{XSD_NS}boolean> .')
            
            # 🔧 TechnicalDescription ↔ Activity CON URI COMPLETE
            triples.append(f'<{desc.ai_text_uri}> <{RICO_BODI_NS}generatedBy> <{activity_uri}> .')
            triples.append(f'<{activity_uri}> <{RICO_BODI_NS}hasGenerated> <{desc.ai_text_uri}> .')
            
            # 🔧 === TRIPLE PER Activity CON URI COMPLETE ===
            triples.append(f'<{activity_uri}> <{RDF_NS}type> <{RICO_NS}Activity> .')
            triples.append(f'<{activity_uri}> <{RDFS_NS}label> "Text generation" .')
            
            # 🔧 Activity ↔ Date CON URI COMPLETE
            triples.append(f'<{activity_uri}> <{RICO_NS}occurredAtDate> <{date_uri}> .')
            triples.append(f'<{date_uri}> <{RICO_NS}isDateOfOccurrenceOf> <{activity_uri}> .')
            
            # 🔧 Activity ↔ Person CON URI COMPLETE
            triples.append(f'<{activity_uri}> <{RICO_BODI_NS}hasOrHadSupervisor> <{lucia_person_uri}> .')
            triples.append(f'<{lucia_person_uri}> <{RICO_BODI_NS}isOrWasSupervisorOf> <{activity_uri}> .')
            
            # 🔧 Activity ↔ Software CON URI COMPLETE
            triples.append(f'<{activity_uri}> <{RICO_NS}isOrWasPerformedBy> <{software_uri}> .')
            triples.append(f'<{software_uri}> <{RICO_NS}performsOrPerformed> <{activity_uri}> .')
            
            # 🔧 === TRIPLE PER Software CON URI COMPLETE ===
            triples.append(f'<{software_uri}> <{RDF_NS}type> <{RICO_BODI_NS}Software> .')
            
            canonical_label = self._get_canonical_model_label(desc.model_used.lower().strip())
            triples.append(f'<{software_uri}> <{RDFS_NS}label> "{canonical_label}" .')
            
            # Documentazione Software
            normalized_key = desc.model_used.lower().strip()
            doc_url = self.model_documentation_cache.get(normalized_key)
            if not doc_url:
                doc_url = self._generate_ollama_doc_url(desc.model_used)
                self.model_documentation_cache[normalized_key] = doc_url
            
            if doc_url:
                triples.append(f'<{software_uri}> <{RICO_BODI_NS}hasDocumentation> <{doc_url}> .')
            
            # 🔧 === TRIPLE PER relazioni Instantiation ↔ TechnicalDescription CON URI COMPLETE ===
            triples.append(f'<{desc.instantiation_uri}> <{RICO_BODI_NS}hasTechnicalDescription> <{desc.ai_text_uri}> .')
            triples.append(f'<{desc.ai_text_uri}> <{RICO_BODI_NS}isTechnicalDescriptionOf> <{desc.instantiation_uri}> .')
        
        self.logger.info(f"✅ Generate {len(triples)} triple con URI COMPLETE per consistenza")
        
        return triples
    
    def convert_triple_to_nquads(self, triple: str) -> str:
        """Converte tripla RDF in formato N-Quads"""
        if triple.endswith(' .'):
            triple = triple[:-2]
        return f"{triple} <{self.target_graph}> .\n"
    
    def insert_triples(self, triples: List[str], dry_run: bool = False, chunk_size: int = 1000) -> bool:
        """Inserisce triple nel grafo usando batch per evitare timeout"""
        if not triples:
            self.logger.info("✅ Nessuna triple da inserire")
            return True
        
        # Aggiungi sempre al buffer N-Quads se abilitato
        if self.always_save_nquads or self.export_nquads:
            for triple in triples:
                nquad = self.convert_triple_to_nquads(triple)
                self.nquads_triples.append(nquad)
        
        if dry_run:
            self.logger.info(f"🧪 DRY-RUN: Simulazione inserimento {len(triples)} triple")
            self.logger.info(f"🎯 Nel grafo: <{self.target_graph}>")
            return True
        
        if self.export_nquads:
            self.logger.info(f"💾 N-QUADS: Aggiunte {len(triples)} triple al buffer")
            return True
        
        # Inserimento reale in Blazegraph con chunking
        self.logger.info(f"💾 INSERIMENTO {len(triples)} TRIPLE IN BLAZEGRAPH (chunking: {chunk_size})...")
        
        # Dividi in chunk
        chunks = [triples[i:i + chunk_size] for i in range(0, len(triples), chunk_size)]
        successful_chunks = 0
        total_inserted = 0
        
        for chunk_num, chunk in enumerate(chunks, 1):
            try:
                self.logger.info(f"   📦 Chunk {chunk_num}/{len(chunks)}: inserimento {len(chunk)} triple...")
                
                triples_str = '\n    '.join(chunk)
                
                insert_query = f"""
                {self.prefixes}
                INSERT DATA {{
        GRAPH <{self.target_graph}> {{
            {triples_str}
        }}
                }}
                """
                
                start_time = time.time()
                response = self.session.post(
                    self.blazegraph_endpoint,
                    data={'update': insert_query},
                    timeout=180  # 3 minuti per chunk
                )
                chunk_time = time.time() - start_time
                
                if response.status_code == 200:
                    successful_chunks += 1
                    total_inserted += len(chunk)
                    self.logger.info(f"      ✅ Chunk {chunk_num} inserito in {chunk_time:.2f}s")
                else:
                    self.logger.error(f"      ❌ Chunk {chunk_num} fallito: HTTP {response.status_code}")
                    
            except Exception as e:
                self.logger.error(f"      ❌ Errore chunk {chunk_num}: {e}")
            
            # Pausa tra chunk per non sovraccaricare
            if chunk_num < len(chunks):
                time.sleep(0.5)
        
        success_rate = (successful_chunks / len(chunks)) * 100
        self.logger.info(f"📊 RIEPILOGO INSERIMENTO:")
        self.logger.info(f"   ✅ Chunk riusciti: {successful_chunks}/{len(chunks)} ({success_rate:.1f}%)")
        self.logger.info(f"   📊 Triple inserite: {total_inserted:,}/{len(triples):,}")
        
        return successful_chunks == len(chunks)
    
    def save_nquads_to_file(self, filename: str = None) -> str:
        """Salva buffer N-Quads su file"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"ai_descriptions_filtered_{timestamp}.nq"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for triple in self.nquads_triples:
                    f.write(triple)
            
            self.logger.info(f"💾 N-Quads salvati: {filename}")
            self.logger.info(f"📊 Triple scritte: {len(self.nquads_triples):,}")
            return filename
            
        except Exception as e:
            self.logger.error(f"❌ Errore salvataggio N-Quads: {e}")
            return None
    
    def run_ai_description_generation(self, 
                                limit: Optional[int] = None,
                                batch_size: int = 10,
                                page_size: int = 100,
                                dry_run: bool = False,
                                incremental_insert_every: int = 100) -> ProcessingResult:
        """Esegue l'intero processo di generazione descrizioni AI - ENHANCED VERSION + FILTRATO CON CHECKPOINTING"""
        
        self.logger.info("🚀 AVVIO GENERAZIONE DESCRIZIONI AI FILTRATE (Record senza redactedInformation)")
        self.logger.info("🔒 FILTRO ATTIVO: Esclusi Record che HANNO la proprietà redactedInformation yes")
        self.logger.info(f"📡 Blazegraph: {self.blazegraph_endpoint}")
        self.logger.info(f"🤖 Ollama: {self.ollama_endpoint} (modello: {self.ollama_model})")
        self.logger.info(f"🎯 Grafo destinazione: <{self.target_graph}>")
        
        self.logger.info(f"🆕 ENHANCED FEATURES:")
        self.logger.info(f"   ✅ Rico:Activity per ogni generazione")
        self.logger.info(f"   💻 bodi:Software con documentazione")
        self.logger.info(f"   🔍 bodi:hasHumanValidation (default: false)")
        self.logger.info(f"   🔗 Relazioni bidirezionali complete")
        self.logger.info(f"   📁 Cache persistente: {AI_COUNTERS_JSON_FILE}")
        self.logger.info(f"   🔢 AI Generated Text: URI con contatore globale")
        self.logger.info(f"   🔄 Checkpointing e ripresa automatica")
        self.logger.info(f"   💾 Inserimento incrementale ogni {incremental_insert_every} descrizioni")
        self.logger.info(f"   🔒 FILTRO PRIVACY: Solo Instantiation di Record SENZA redactedInformation")
        
        if limit:
            self.logger.info(f"🔢 Limite Instantiation: {limit}")
        else:
            self.logger.info(f"🌍 Limite Instantiation: NESSUNO (tutto il database filtrato)")
        
        self.logger.info(f"📦 Batch size AI: {batch_size}")
        self.logger.info(f"📄 Page size query: {page_size}")
        self.logger.info(f"📁 Checkpoint file: {self.checkpoint_file}")
        
        if not limit:
            estimated_instantiations = 5000  # Stima conservativa dopo filtro
            estimated_time_hours = (estimated_instantiations * 3) / 3600  # 3 secondi per descrizione
            self.logger.info(f"⚠️ PROCESSO COMPLETO DATABASE FILTRATO:")
            self.logger.info(f"   📊 Instantiation stimate (dopo filtro): ~{estimated_instantiations:,}+")
            self.logger.info(f"   ⏱️ Tempo stimato: ~{estimated_time_hours:.1f} ore")
            self.logger.info(f"   🤖 Uso intensivo di Ollama previsto")
            self.logger.info(f"   💡 Usa Ctrl+C per interrompere in qualsiasi momento")
            self.logger.info(f"   🔄 Ripresa automatica garantita da checkpoint")
        
        self.logger.info("="*70)
        
        start_time = time.time()
        result = ProcessingResult()
        
        try:
            # Test connessioni
            if not self.test_blazegraph_connection():
                result.errors.append("Connessione Blazegraph fallita")
                return result
            
            if not self.test_ollama_connection():
                result.errors.append("Connessione Ollama fallita")
                return result
            
            # 🆕 CARICA INSTANTIATION GIÀ PROCESSATE
            existing_descriptions = self.get_comprehensive_existing_descriptions()
            self.processed_instantiations = existing_descriptions.copy()
            
            # Recupera Instantiation con metadati CON FILTRO
            instantiations = self.get_instantiations_with_metadata(limit, page_size)
            if not instantiations:
                self.logger.info("✅ Nessuna Instantiation non anonimizzata da processare")
                return result
            
            result.total_instantiations_processed = len(instantiations)
            
            # Conta quelle che hanno Record collegati per statistiche
            with_record = sum(1 for inst in instantiations if inst.related_record_uri)
            without_record = len(instantiations) - with_record
            
            result.total_non_anonymized_instantiations = len(instantiations)
            
            self.logger.info(f"📊 DETTAGLIO INSTANTIATION FILTRATE:")
            self.logger.info(f"   ✅ Collegate a Record NON anonimizzati: {with_record}")
            self.logger.info(f"   ℹ️ Senza Record collegato (comunque incluse): {without_record}")
            
            # 🆕 FILTRA INSTANTIATION GIÀ PROCESSATE
            to_process = [inst for inst in instantiations 
                        if inst.instantiation_uri not in existing_descriptions]
            
            if len(to_process) < len(instantiations):
                skipped = len(instantiations) - len(to_process)
                self.logger.info(f"⚡ RIPRESA: Saltate {skipped} instantiation già processate")
                self.logger.info(f"🔄 CONTINUAZIONE: {len(to_process)} instantiation rimanenti")
            
            if not to_process:
                self.logger.info("✅ Tutte le Instantiation filtrate hanno già descrizioni AI!")
                return result
            
            self.logger.info(f"🔄 PROCESSAMENTO {len(to_process)} INSTANTIATION FILTRATE...")
            
            # 🆕 PROCESSAMENTO CON INSERIMENTO INCREMENTALE
            ai_descriptions = []
            ollama_calls = 0
            incremental_insertions = 0
            
            for i in range(0, len(to_process), batch_size):
                batch = to_process[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(to_process) + batch_size - 1) // batch_size
                
                self.logger.info(f"\n📦 BATCH {batch_num}/{total_batches} ({len(batch)} Instantiation non anonimizzate)")
                
                batch_descriptions = []
                
                for j, instantiation in enumerate(batch, 1):
                    inst_id = instantiation.instantiation_uri.split('/')[-1]
                    record_info = f" (Record: {instantiation.related_record_uri.split('/')[-1] if instantiation.related_record_uri else 'None'})"
                    self.logger.info(f"   🤖 {j}/{len(batch)}: Generating for {inst_id}{record_info}")
                    
                    # Genera descrizione AI
                    description = self.generate_ai_description(instantiation)
                    ollama_calls += 1
                    
                    if description:
                        ai_text_uri = self.generate_ai_text_uri(instantiation.instantiation_uri)
                        generation_timestamp = datetime.now().isoformat()
                        
                        ai_desc = AIGeneratedDescription(
                            instantiation_uri=instantiation.instantiation_uri,
                            ai_text_uri=ai_text_uri,
                            description=description,
                            model_used=self.ollama_model,
                            generation_timestamp=generation_timestamp,
                            metadata_count=len(instantiation.metadata_dict),
                            file_path=instantiation.file_path,
                            has_human_validation=False
                        )
                        
                        batch_descriptions.append(ai_desc)
                        ai_descriptions.append(ai_desc)
                        
                        # 🆕 AGGIUNGI A PROCESSED SET
                        self.processed_instantiations.add(instantiation.instantiation_uri)
                        
                        self.logger.info(f"      ✅ Generated: {description[:100]}...")
                    else:
                        result.errors.append(f"Generazione fallita per {inst_id}")
                        self.logger.warning(f"      ❌ Generation failed for {inst_id}")
                
                # 🆕 INSERIMENTO INCREMENTALE OGNI N DESCRIZIONI
                if len(ai_descriptions) % incremental_insert_every == 0 and len(ai_descriptions) > 0:
                    descriptions_to_insert = ai_descriptions[-incremental_insert_every:]
                    
                    if descriptions_to_insert and not dry_run:
                        self.logger.info(f"\n💾 INSERIMENTO INCREMENTALE #{incremental_insertions + 1}: {len(descriptions_to_insert)} descrizioni")
                        
                        # Genera e inserisci triple
                        triples = self.generate_ai_description_triples(descriptions_to_insert)
                        success = self.insert_triples(triples, dry_run=False)
                        
                        if success:
                            incremental_insertions += 1
                            self.logger.info(f"✅ Inserimento incrementale #{incremental_insertions} completato")
                            
                            # 🆕 SALVA CHECKPOINT
                            self.save_checkpoint(self.processed_instantiations)
                            self.logger.info(f"💾 Checkpoint salvato dopo {len(self.processed_instantiations)} instantiation")
                        else:
                            self.logger.error(f"❌ Errore inserimento incrementale #{incremental_insertions + 1}")
                            result.errors.append("Errore inserimento incrementale")
                
                # Pausa tra batch per non sovraccaricare Ollama
                if i + batch_size < len(to_process):
                    self.logger.info(f"   ⏱️ Pausa 2s prima del prossimo batch...")
                    time.sleep(2)
            
            result.total_descriptions_generated = len(ai_descriptions)
            result.total_ollama_calls = ollama_calls
            result.ai_descriptions = ai_descriptions
            
            # 🆕 ENHANCED: Statistiche aggiuntive
            if ai_descriptions:
                unique_models = set(desc.model_used for desc in ai_descriptions)
                result.total_software_entities_created = len(unique_models)
                result.total_activities_created = len(ai_descriptions)
            
            # 🆕 INSERIMENTO FINALE per descrizioni rimanenti
            remaining_count = len(ai_descriptions) % incremental_insert_every
            if remaining_count > 0 and ai_descriptions and not dry_run:
                remaining_descriptions = ai_descriptions[-remaining_count:]
                self.logger.info(f"\n💾 INSERIMENTO FINALE: {len(remaining_descriptions)} descrizioni rimanenti")
                
                triples = self.generate_ai_description_triples(remaining_descriptions)
                success = self.insert_triples(triples, dry_run=False)
                
                if success:
                    self.logger.info(f"✅ Inserimento finale completato")
                    # Salva checkpoint finale
                    self.save_checkpoint(self.processed_instantiations)
                else:
                    result.errors.append("Errore inserimento finale")
            
            # Aggiorna statistiche risultato
            if ai_descriptions:
                if not dry_run:
                    result.total_ai_text_entities_created = len(ai_descriptions)
                    result.total_relationships_created = len(ai_descriptions) * 6
            
            # Salva N-Quads se necessario
            if (self.always_save_nquads or self.export_nquads) and self.nquads_triples:
                nquads_filename = self.save_nquads_to_file()
                if nquads_filename:
                    result.nquads_file = nquads_filename
                    result.total_nquads_written = len(self.nquads_triples)
            
            # 🆕 ENHANCED: Salva contatori finali
            self._save_ai_counters_to_json()
            
            result.processing_time_seconds = time.time() - start_time
            
            # RIEPILOGO FINALE ENHANCED CON FILTRO
            self.logger.info("\n" + "="*70)
            self.logger.info("📋 RIEPILOGO GENERAZIONE AI DESCRIZIONI - RECORD NON ANONIMIZZATI")
            self.logger.info("="*70)
            self.logger.info(f"⏱️ Tempo elaborazione: {result.processing_time_seconds:.2f} secondi")
            self.logger.info(f"🔒 FILTRO APPLICATO: Solo Record senza proprietà redactedInformation")
            self.logger.info(f"📊 Instantiation senza redactedInformation trovate: {result.total_non_anonymized_instantiations}")
            self.logger.info(f"⚡ Instantiation saltate (già processate): {len(existing_descriptions)}")
            self.logger.info(f"🆕 Instantiation processate in questa sessione: {result.total_descriptions_generated}")
            self.logger.info(f"📈 Totale instantiation processate: {len(self.processed_instantiations)}")
            self.logger.info(f"🔧 Chiamate Ollama in questa sessione: {result.total_ollama_calls}")
            
            # 🆕 ENHANCED STATS CON FILTRO
            self.logger.info(f"🆕 ENHANCED STATISTICHE FILTRATE:")
            self.logger.info(f"   🎭 Activity create: {result.total_activities_created}")
            self.logger.info(f"   💻 Software entities create: {result.total_software_entities_created}")
            self.logger.info(f"   💾 Inserimenti incrementali: {incremental_insertions}")
            self.logger.info(f"   📁 Checkpoint file: {self.checkpoint_file}")
            self.logger.info(f"   🔄 Sistema ripresa: ATTIVO")
            self.logger.info(f"   🔒 Record anonimizzati: ESCLUSI automaticamente")
            
            if not dry_run and not self.export_nquads:
                self.logger.info(f"📝 Entità TechnicalDescription create: {result.total_ai_text_entities_created}")
                self.logger.info(f"🔗 Relazioni create: {result.total_relationships_created}")
                self.logger.info(f"🎯 Nel grafo: <{self.target_graph}>")
            
            if result.nquads_file:
                self.logger.info(f"💾 File N-Quads: {result.nquads_file}")
                self.logger.info(f"📊 Triple N-Quads: {result.total_nquads_written:,}")
            
            self.logger.info(f"📁 Contatori persistenti: {AI_COUNTERS_JSON_FILE}")
            
            if result.errors:
                self.logger.warning(f"⚠️ Errori: {len(result.errors)}")
                for error in result.errors[:5]:
                    self.logger.warning(f"   • {error}")
            else:
                self.logger.info("🎉 PROCESSO ENHANCED FILTRATO COMPLETATO SENZA ERRORI!")
            
            self.logger.info("="*70)
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Errore fatale: {e}")
            result.errors.append(f"Errore fatale: {e}")
            result.processing_time_seconds = time.time() - start_time
            
            # Salva checkpoint anche in caso di errore
            if hasattr(self, 'processed_instantiations') and self.processed_instantiations:
                self.save_checkpoint(self.processed_instantiations)
                self.logger.info(f"💾 Checkpoint salvato dopo errore: {len(self.processed_instantiations)} instantiation")
            
            return result
    
    def save_report(self, result: ProcessingResult, filename: str = None):
        """Salva report dettagliato in JSON - ENHANCED VERSION + FILTRATO"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"ai_descriptions_filtered_report_{timestamp}.json"
        
        report = {
            'generation_info': {
                'timestamp': datetime.now().isoformat(),
                'blazegraph_endpoint': self.blazegraph_endpoint,
                'ollama_endpoint': self.ollama_endpoint,
                'ollama_model': self.ollama_model,
                'target_graph': self.target_graph,
                'export_nquads': self.export_nquads,
                'processing_time_seconds': result.processing_time_seconds,
                'success': len(result.errors) == 0,
                'version': '2.1 Enhanced + Filtered',
                # 🆕 ENHANCED INFO
                'ai_counters_file': AI_COUNTERS_JSON_FILE,
                'software_entities_in_cache': len(self.software_cache),
                'current_software_counter': self.software_counter,
                'current_activity_counter': self.activity_counter,
                # 🆕 FILTRO INFO
                'privacy_filter_applied': True,
                'excluded_condition': 'Record with redactedInformation = "yes"'
            },
            'summary': {
                'total_instantiations_processed': result.total_instantiations_processed,
                'total_descriptions_generated': result.total_descriptions_generated,
                'total_ai_text_entities_created': result.total_ai_text_entities_created,
                'total_relationships_created': result.total_relationships_created,
                'total_ollama_calls': result.total_ollama_calls,
                'nquads_file': result.nquads_file,
                'total_nquads_written': result.total_nquads_written,
                'errors_count': len(result.errors),
                # 🆕 ENHANCED SUMMARY
                'total_activities_created': result.total_activities_created,
                'total_software_entities_created': result.total_software_entities_created,
                'default_human_validation': False,
                # 🆕 FILTRO SUMMARY
                'total_non_anonymized_instantiations': result.total_non_anonymized_instantiations,
                'total_instantiations_filtered_out': result.total_instantiations_filtered_out
            },
            'privacy_filter': {
                'enabled': True,
                'condition': 'Exclude Instantiation linked to Records with bodi:redactedInformation = "yes"',
                'method': 'NOT EXISTS clause in SPARQL query',
                'instantiations_excluded_count': result.total_instantiations_filtered_out,
                'instantiations_included_count': result.total_non_anonymized_instantiations
            },
            'errors': result.errors,
            'ai_descriptions': [
                {
                    'instantiation_uri': desc.instantiation_uri,
                    'ai_text_uri': desc.ai_text_uri,
                    'description': desc.description,
                    'model_used': desc.model_used,
                    'generation_timestamp': desc.generation_timestamp,
                    'metadata_count': desc.metadata_count,
                    'file_path': desc.file_path,
                    # 🆕 ENHANCED FIELDS
                    'activity_uri': desc.activity_uri,
                    'software_uri': desc.software_uri,
                    'has_human_validation': desc.has_human_validation
                }
                for desc in result.ai_descriptions
            ],
            # 🆕 ENHANCED: Informazioni sui Software creati
            'software_entities': {
                model_name: software_uri 
                for model_name, software_uri in self.software_cache.items()
            },
            'model_documentation': self.model_documentation_cache
        }
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            self.logger.info(f"📄 Report ENHANCED FILTRATO salvato: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"❌ Errore salvataggio report: {e}")
            return None

def parse_arguments():
    """Parser argomenti da riga di comando - ENHANCED VERSION + FILTRATO"""
    parser = argparse.ArgumentParser(
        description="AI Technical Descriptions Generator ENHANCED + FILTRATO - Genera descrizioni AI solo per Record NON anonimizzati",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi di utilizzo ENHANCED + FILTRATO:

  # Genera descrizioni per Record NON anonimizzati (raccomandato)
  python ai_technical_descriptions_filtered.py
  
  # Test con poche Instantiation non anonimizzate
  python ai_technical_descriptions_filtered.py --limit 10 --dry-run
  
  # Solo export N-Quads (non inserisce in Blazegraph)
  python ai_technical_descriptions_filtered.py --export-nquads
  
  # Usa modello Ollama diverso
  python ai_technical_descriptions_filtered.py --ollama-model llama3.1
  
  # Ottimizzazioni per database grandi
  python ai_technical_descriptions_filtered.py --page-size 50 --batch-size 5

"""
    )
    
    parser.add_argument(
        '--blazegraph-endpoint',
        default='http://localhost:10214/blazegraph/namespace/kb/sparql',
        help='Endpoint SPARQL Blazegraph (default: localhost:10214)'
    )
    
    parser.add_argument(
        '--ollama-endpoint',
        default='http://localhost:11434',
        help='Endpoint Ollama (default: localhost:11434)'
    )
    
    parser.add_argument(
        '--ollama-model',
        default='llama3.2',
        help='Modello Ollama da utilizzare (default: llama3.2)'
    )
    
    parser.add_argument(
        '--target-graph',
        default='http://ficlit.unibo.it/ArchivioEvangelisti/ai_descriptions',
        help='URI del grafo di destinazione (default: ai_descriptions)'
    )
    
    parser.add_argument(
        '--export-nquads',
        action='store_true',
        help='Esporta SOLO in N-Quads (non inserisce in Blazegraph)'
    )
    
    parser.add_argument(
        '--no-auto-nquads',
        action='store_true',
        help='Disabilita salvataggio automatico N-Quads'
    )
    
    parser.add_argument(
        '--nquads-file',
        help='Nome file N-Quads personalizzato'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Limite numero Instantiation da processare (default: NESSUN LIMITE - tutto il database filtrato)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Numero Instantiation per batch AI (default: 10)'
    )
    
    parser.add_argument(
        '--page-size',
        type=int,
        default=100,
        help='Numero Instantiation per pagina query (default: 100)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Modalità simulazione - non crea descrizioni reali'
    )
    
    parser.add_argument(
        '--test-only',
        action='store_true',
        help='Solo test connessioni'
    )
    
    parser.add_argument(
        '--report-file',
        help='Nome file report personalizzato'
    )

    parser.add_argument(
        '--incremental-every',
        type=int,
        default=100,
        help='Inserimento incrementale ogni N descrizioni (default: 100)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Output verboso'
    )
    
    return parser.parse_args()

def main():
    """Funzione principale ENHANCED + FILTRATO"""
    args = parse_arguments()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='[%(levelname)s] %(asctime)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    print("="*70)
    print("🤖 AI TECHNICAL DESCRIPTIONS GENERATOR ENHANCED + FILTRATO v2.1")
    print("📝 Generatore Descrizioni AI con Activity, Software e Human Validation")
    print("🔒 FILTRATO: Solo Record NON anonimizzati")
    print("🔗 CON SUPPORTO OLLAMA E URI STRUTTURATI")
    print("="*70)
    print(f"📡 Blazegraph: {args.blazegraph_endpoint}")
    print(f"🤖 Ollama: {args.ollama_endpoint} (modello: {args.ollama_model})")
    print(f"🎯 Grafo destinazione: {args.target_graph}")
    print(f"🔒 FILTRO PRIVACY: Esclusi Record che HANNO redactedInformation (qualsiasi valore)")
    
    print(f"🆕 ENHANCED FEATURES v2.1 + FILTRATO:")
    print(f"   ✅ Rico:Activity per ogni generazione")
    print(f"   💻 bodi:Software con documentazione")
    print(f"   🔍 bodi:hasHumanValidation (default: false)")
    print(f"   🔗 Relazioni bidirezionali complete")
    print(f"   📁 Cache persistente: {AI_COUNTERS_JSON_FILE}")
    print(f"   🔢 AI Generated Text: contatori globali")
    print(f"   🔒 Privacy Filter: Solo Record non anonimizzati")
    print(f"   🔄 Checkpointing e ripresa automatica")
    
    always_save_nquads = not args.no_auto_nquads
    
    if args.export_nquads:
        print("💾 Modalità: SOLO EXPORT N-QUADS ENHANCED + FILTRATO")
    else:
        mode = "DRY-RUN" if args.dry_run else "PRODUZIONE"
        print(f"🔴 Modalità: {mode} ENHANCED + FILTRATO")
    
    if always_save_nquads:
        print("💾 File N-Quads: CREAZIONE AUTOMATICA ENHANCED")
    else:
        print("💾 File N-Quads: DISABILITATO")
    
    if args.limit:
        print(f"🔢 Limite Instantiation: {args.limit}")
    else:
        print(f"🌍 Limite Instantiation: NESSUNO (tutto il database filtrato)")
    
    print(f"📦 Batch size AI: {args.batch_size}")
    print(f"📄 Page size query: {args.page_size}")
    print(f"💾 Inserimento incrementale ogni: {args.incremental_every} descrizioni")
    print("="*70)
    
    try:
        # Crea generatore ENHANCED + FILTRATO
        generator = AITechnicalDescriptionGenerator(
            blazegraph_endpoint=args.blazegraph_endpoint,
            ollama_endpoint=args.ollama_endpoint,
            ollama_model=args.ollama_model,
            target_graph=args.target_graph,
            export_nquads=args.export_nquads,
            always_save_nquads=always_save_nquads
        )
        
        # Test connessioni
        if not generator.test_blazegraph_connection() or not generator.test_ollama_connection():
            print("❌ Test connessioni fallito")
            sys.exit(1)
        
        if args.test_only:
            print("✅ TEST CONNESSIONI ENHANCED + FILTRATO COMPLETATO CON SUCCESSO")
            print("🚀 Pronto per generare descrizioni AI Enhanced per Record non anonimizzati")
            print("🔒 Filtro privacy attivo: Esclusi Record con redactedInformation = 'yes'")
            return
        
        # Informazioni filtro privacy
        print("\n🔒 INFORMAZIONI FILTRO PRIVACY:")
        print("   ❌ ESCLUSE: Instantiation di Record che HANNO bodi:redactedInformation (qualsiasi valore)")
        print("   ✅ INCLUSE: Solo Instantiation di Record SENZA la proprietà redactedInformation")
        print("   🛡️ Include solo Record mai processati dal privacy system")
        print("   📊 Le statistiche mostreranno quanti Record sono stati filtrati")
        
        # Stima impatto filtro
        if not args.limit:
            print("\n⚠️ STIMA IMPATTO FILTRO:")
            print("   📊 Il filtro ridurrà significativamente il numero di Instantiation processate")
            print("   🔒 I Record anonimizzati (con omitted info) verranno automaticamente esclusi")
            print("   ⚡ Questo renderà il processamento più veloce e rispettoso della privacy")
            print("   💡 Solo i contenuti pubblicamente visibili riceveranno descrizioni AI")
        
        print("="*70)
        
        # Esegui generazione ENHANCED + FILTRATO
        result = generator.run_ai_description_generation(
            limit=args.limit,
            batch_size=args.batch_size,
            page_size=args.page_size,
            dry_run=args.dry_run,
            incremental_insert_every=args.incremental_every
        )
        
        # Salva N-Quads se richiesto manualmente
        if args.export_nquads and args.nquads_file and not result.nquads_file:
            nquads_filename = generator.save_nquads_to_file(args.nquads_file)
            if nquads_filename:
                result.nquads_file = nquads_filename
                result.total_nquads_written = len(generator.nquads_triples)
        
        # Salva report ENHANCED + FILTRATO
        if args.report_file or result.total_descriptions_generated > 0:
            generator.save_report(result, args.report_file)
        
        # Report finale del filtro
        if result.total_non_anonymized_instantiations > 0:
            print(f"\n📊 REPORT FILTRO PRIVACY:")
            print(f"   ✅ Instantiation di Record SENZA redactedInformation: {result.total_non_anonymized_instantiations:,}")
            if result.total_instantiations_filtered_out > 0:
                filter_percentage = (result.total_instantiations_filtered_out / (result.total_non_anonymized_instantiations + result.total_instantiations_filtered_out)) * 100
                print(f"   🔒 Instantiation di Record CON redactedInformation (filtrate): {result.total_instantiations_filtered_out:,} ({filter_percentage:.1f}%)")
            print(f"   🎯 Descrizioni AI generate: {result.total_descriptions_generated:,}")
            print(f"   🛡️ Privacy protection: Solo Record 'puliti' processati")
        
        # Exit code
        if result.errors:
            print(f"⚠️ Uscita con errori: {len(result.errors)}")
            sys.exit(1)
        elif result.total_descriptions_generated == 0:
            print("✅ Nessuna descrizione da generare (tutte le Instantiation non anonimizzate già processate)")
            sys.exit(0)
        else:
            print("✅ Processo ENHANCED + FILTRATO completato con successo")
            if result.nquads_file:
                print(f"💾 File N-Quads: {result.nquads_file}")
            print(f"📁 Contatori persistenti: {AI_COUNTERS_JSON_FILE}")
            print(f"🔒 Privacy protection: RISPETTATO per {result.total_descriptions_generated} descrizioni")
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\n⚠️ Processo interrotto dall'utente")
        print("💾 Checkpoint salvato automaticamente per ripresa futura")
        print("🔒 Privacy protection: Mantenuto anche durante interruzione")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Errore fatale: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
