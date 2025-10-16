#!/usr/bin/env python3
"""
Dataset Validator - SPARQL Validation System for the Evangelisti Archive

Performs structured SPARQL queries to verify the logic and consistency of the RDF dataset
loaded into Blazegraph. 

VALIDATION CATEGORIES:
1. 📊 General Statistics - Total counts, entity type distribution  
2. 🏗️ Archival Structural Integrity - Records/RecordSets/Instantiation, relationships, and orphan detection
3. 📋 Technical Metadata Validation - Tika, ExifTool, FileSystem metadata
4. 🔐 Hash Validation and Complete Integrity - SHA-256, algorithms, format, duplicates, integrity
5. 🔬 Advanced Consistency - Complex checks and edge cases
6. 📊 CSV Export Metadata Types - Detailed report with metadata type counts


"""

import argparse
import json
import logging
import requests
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from urllib.parse import quote


@dataclass
class ValidationResult:
    """Risultato di una singola validazione"""
    query_name: str
    description: str
    status: str  # 'PASS', 'FAIL', 'WARNING', 'INFO'
    result_count: int
    details: List[Dict[str, Any]]
    execution_time_ms: float
    error_message: Optional[str] = None



class SPARQLValidationEngine:
    """Engine ottimizzato per dataset enormi (16GB+) con Analytic Query Mode e throttling integrato"""
    
    def __init__(self, endpoint_url: str = "http://localhost:10214/blazegraph/namespace/kb/sparql"):
        self.endpoint = endpoint_url
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/sparql-results+json',
            'User-Agent': 'EvangelistiValidator-HeavyDataset/2.0'
        })
        
        # Timeout esteso per dataset grandi
        self.session.timeout = 1800  # 30 minuti
        
        # INTEGRATO: Configurazione throttling per dataset grandi
        self.query_delay = 3.0  # 3 secondi tra query (ottimale per 16GB dataset)
        self.last_query_time = 0  # Timestamp ultima query
        
        # Prefissi ottimizzati con query hints per dataset grandi
        self.prefixes = """
        PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
        PREFIX bodi: <http://w3id.org/bodi#>
        PREFIX premis: <http://www.loc.gov/premis/rdf/v3/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dc: <http://purl.org/dc/terms/>
        """
    
    def test_connection(self) -> bool:
        """Testa la connessione al server SPARQL"""
        try:
            test_query = "ASK { ?s ?p ?o }"
            full_query = self.prefixes + test_query
            
            response = self.session.post(
                self.endpoint,
                data={'query': full_query},
                timeout=30  # Timeout più breve per test connessione
            )
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"❌ Errore test connessione: {e}")
            return False
    
    def enable_analytic_mode(self) -> bool:
        """Tenta di abilitare modalità analytic per performance su dataset grandi"""
        try:
            # Query analytic mode hint per Blazegraph
            analytic_query = """
            PREFIX hint: <http://www.bigdata.com/queryHints#>
            ASK { 
                hint:Query hint:analytic "true" .
                ?s ?p ?o 
            } LIMIT 1
            """
            
            response = self.session.post(
                self.endpoint,
                data={'query': analytic_query},
                timeout=30
            )
            
            return response.status_code == 200
            
        except Exception:
            return False
    
    def setup_truth_maintenance_optimization(self) -> bool:
        """Setup ottimizzazioni truth maintenance per dataset pesanti"""
        try:
            # Questi sono query hints che possono migliorare le performance
            # su dataset grandi in Blazegraph
            return True  # Placeholder - implementazione specifica dipende da Blazegraph
        except Exception:
            return False
    
    def restore_truth_maintenance(self):
        """Restore configurazioni originali"""
        # Placeholder per cleanup
        pass
    
    def _throttle_query(self, query_name: str):
        """Implementa throttling intelligente integrato per dataset pesanti"""
        current_time = time.time()
        time_since_last = current_time - self.last_query_time
        
        # Solo se non è la prima query e non è passato abbastanza tempo
        if self.last_query_time > 0 and time_since_last < self.query_delay:
            sleep_time = self.query_delay - time_since_last
            print(f"   ⏳ Pausa server {sleep_time:.1f}s...")
            time.sleep(sleep_time)
        
        self.last_query_time = time.time()

    def execute_query(self, query: str, query_name: str) -> ValidationResult:
        """Esegue una query SPARQL con throttling integrato e restituisce il risultato strutturato"""
        
        # INTEGRATO: Applica throttling automatico
        self._throttle_query(query_name)
        
        full_query = self.prefixes + query
        start_time = time.time()
        
        try:
            response = self.session.post(
                self.endpoint,
                data={'query': full_query},
                timeout=1800  # 30 minuti
            )
            
            execution_time = (time.time() - start_time) * 1000
            
            if response.status_code != 200:
                return ValidationResult(
                    query_name=query_name,
                    description="Query execution failed",
                    status='FAIL',
                    result_count=0,
                    details=[],
                    execution_time_ms=execution_time,
                    error_message=f"HTTP {response.status_code}: {response.text[:200]}"
                )
            
            result_data = response.json()
            bindings = result_data.get("results", {}).get("bindings", [])
            
            # Converte i binding in formato più leggibile
            details = []
            for binding in bindings:
                detail = {}
                for var, value_info in binding.items():
                    detail[var] = value_info.get('value', str(value_info))
                details.append(detail)
            
            return ValidationResult(
                query_name=query_name,
                description="Query executed successfully",
                status='INFO',
                result_count=len(details),
                details=details,
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return ValidationResult(
                query_name=query_name,
                description="Query execution error",
                status='FAIL',
                result_count=0,
                details=[],
                execution_time_ms=execution_time,
                error_message=str(e)
            )

    
class DatasetValidator:
    """Validatore principale del dataset con query strutturate e pause integrate"""
    
    def __init__(self, sparql_engine: SPARQLValidationEngine, logger=None):
        self.engine = sparql_engine
        self.logger = logger or self._setup_default_logger()
        self.results: List[ValidationResult] = []
        
        # INTEGRATO: Pause ottimali per dataset grandi
        self.category_delay = 8.0  # 8 secondi tra categorie (per far riposare Blazegraph)

    def run_validation_suite(self, validation_level: str = 'full') -> Dict[str, Any]:
        """Esegue la suite completa di validazioni con resilienza agli errori e throttling integrato"""
        self.logger.info("🚀 AVVIO VALIDAZIONE DATASET EVANGELISTI")
        self.logger.info(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"📊 Livello validazione: {validation_level}")
        self.logger.info(f"🔗 Endpoint SPARQL: {self.engine.endpoint}")
        self.logger.info(f"⏰ Throttling integrato: {self.engine.query_delay}s tra query, {self.category_delay}s tra categorie")
        self.logger.info("🛡️ Modalità fault-tolerant: continua anche se singoli step falliscono")
        self.logger.info("🐘 Ottimizzato per dataset pesanti (16GB+)")
        
        start_time = time.time()
        
        # Test connessione
        if not self.engine.test_connection():
            self.logger.error("❌ Impossibile connettersi a Blazegraph")
            return {'status': 'CONNECTION_FAILED', 'error': 'Connection failed'}
        
        # Lista delle categorie da eseguire con gestione errori
        categories_to_run = []
        
        if validation_level in ['basic', 'full']:
            categories_to_run.extend([
                ('Statistiche Generali', self._run_basic_statistics),
                ('Integrità Strutturale Archivistica', self._run_structural_integrity),
            ])
        
        if validation_level == 'full':
            categories_to_run.extend([
                ('Validazione Metadati Tecnici', self._run_metadata_validation),
                ('Validazione Hash e Integrità Completa', self._run_comprehensive_hash_validation),
                ('Consistenza Avanzata', self._run_advanced_consistency),
                ('Export CSV Metadata Types', self._run_csv_export)
            ])
        
        self.logger.info(f"📋 Categorie da eseguire: {len(categories_to_run)}")
        
        # Esegui ogni categoria con gestione errori E PAUSE INTEGRATE
        failed_categories = []
        successful_categories = []
        
        for i, (category_name, category_func) in enumerate(categories_to_run, 1):
            try:
                self.logger.info(f"\n🔄 Esecuzione categoria {i}/{len(categories_to_run)}: {category_name}")
                category_func()
                successful_categories.append(category_name)
                self.logger.info(f"✅ Categoria completata: {category_name}")
                
                # INTEGRATO: Pausa intelligente tra categorie (eccetto l'ultima)
                if i < len(categories_to_run):
                    self.logger.info(f"🛏️ Riposo server {self.category_delay}s prima della prossima categoria...")
                    time.sleep(self.category_delay)
                
            except Exception as e:
                self.logger.error(f"❌ Errore in categoria '{category_name}': {e}")
                self.logger.warning(f"🔄 Continuo con le categorie successive...")
                failed_categories.append((category_name, str(e)))
                
                # Crea un risultato di fallimento per questa categoria
                error_result = ValidationResult(
                    query_name=f"{category_name.lower().replace(' ', '_')}_category_error",
                    description=f"ERRORE CATEGORIA: {category_name}",
                    status='FAIL',
                    result_count=0,
                    details=[],
                    execution_time_ms=0,
                    error_message=str(e)
                )
                self.results.append(error_result)
                
                # Continua con la prossima categoria
                continue
        
        total_time = time.time() - start_time
        
        # Log riepilogo esecuzione
        self.logger.info(f"\n📊 RIEPILOGO ESECUZIONE CATEGORIE:")
        self.logger.info(f"✅ Categorie completate: {len(successful_categories)}/{len(categories_to_run)}")
        if successful_categories:
            for cat in successful_categories:
                self.logger.info(f"   • {cat}")
        
        if failed_categories:
            self.logger.warning(f"❌ Categorie fallite: {len(failed_categories)}")
            for cat, error in failed_categories:
                self.logger.warning(f"   • {cat}: {error}")
        
        self._log_execution_summary()
        
        return self._generate_final_report(total_time, failed_categories)
    
    def _run_basic_statistics(self):
        """1. STATISTICHE GENERALI"""
        self.logger.info("\n" + "="*60)
        self.logger.info("📊 CATEGORIA 1: STATISTICHE GENERALI DEL DATASET")
        self.logger.info("="*60)
        
        queries = [
            {
                'name': 'total_triples',
                'description': '🔢 DIMENSIONE DATASET - Conteggio totale triple RDF nel database',
                'query': 'SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }',
                'validation': lambda r: self._validate_triple_count_info(r)
            },
            {
                'name': 'entity_composition_counts',
                'description': '📊 COMPOSIZIONE ARCHIVIO - Conteggi Record/RecordSet/Instantiation',
                'query': '''
                SELECT 
                  (COUNT(DISTINCT ?record) AS ?totalRecords)
                  (COUNT(DISTINCT ?recordset) AS ?totalRecordSets) 
                  (COUNT(DISTINCT ?inst) AS ?totalInstantiations)
                WHERE {
                  {
                    ?record rdf:type rico:Record .
                  } UNION {
                    ?recordset rdf:type rico:RecordSet .
                  } UNION {
                    ?inst rdf:type rico:Instantiation .
                  }
                }
                ''',
                'validation': lambda r: self._validate_entity_counts(r)
            },
                        {
                'name': 'rico_record_count',
                'description': '📄 RECORD - Conteggio entità Record',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type rico:Record . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'rico:Record')
            },
            {
                'name': 'rico_recordset_count',
                'description': '📁 RECORDSET - Conteggio entità RecordSet',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type rico:RecordSet . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'rico:RecordSet')
            },
            {
                'name': 'rico_instantiation_count',
                'description': '💾 INSTANTIATION - Conteggio entità Instantiation',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type rico:Instantiation . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'rico:Instantiation')
            },
            {
                'name': 'rico_identifier_count',
                'description': '🆔 IDENTIFIER - Conteggio entità Identifier',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type rico:Identifier . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'rico:Identifier')
            },
            {
                'name': 'rico_identifiertype_count',
                'description': '🏷️ IDENTIFIER TYPE - Conteggio entità IdentifierType',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type rico:IdentifierType . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'rico:IdentifierType')
            },
            {
                'name': 'technical_metadata_count',
                'description': '⚙️ TECHNICAL METADATA - Conteggio entità TechnicalMetadata',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type bodi:TechnicalMetadata . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'bodi:TechnicalMetadata')
            },
            {
                'name': 'rico_person_count',
                'description': '👤 PERSON - Conteggio entità Person',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type rico:Person . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'rico:Person')
            },
            {
                'name': 'technical_metadata_type_count',
                'description': '🔧 TECHNICAL METADATA TYPE - Conteggio entità TechnicalMetadataType',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type bodi:TechnicalMetadataType . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'bodi:TechnicalMetadataType')
            },
            {
                'name': 'rico_activity_count',
                'description': '🔄 ACTIVITY - Conteggio entità Activity',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type rico:Activity . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'rico:Activity')
            },
            {
                'name': 'premis_fixity_count',
                'description': '🔒 FIXITY - Conteggio entità Fixity',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type premis:Fixity . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'premis:Fixity')
            },
            {
                'name': 'prov_location_count',
                'description': '📍 LOCATION - Conteggio entità Location',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type prov:Location . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'prov:Location')
            },
            {
                'name': 'rico_extent_count',
                'description': '📏 EXTENT - Conteggio entità Extent',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type rico:Extent . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'rico:Extent')
            },
            {
                'name': 'software_stack_count',
                'description': '🏗️ SOFTWARE STACK - Conteggio entità SoftwareStack',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type bodi:SoftwareStack . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'bodi:SoftwareStack')
            },
            {
                'name': 'software_count',
                'description': '💻 SOFTWARE - Conteggio entità Software',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type bodi:Software . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'bodi:Software')
            },
            {
                'name': 'software_component_count',
                'description': '🧩 SOFTWARE COMPONENT - Conteggio entità SoftwareComponent',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type bodi:SoftwareComponent . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'bodi:SoftwareComponent')
            },
            {
                'name': 'algorithm_count',
                'description': '🔢 ALGORITHM - Conteggio entità Algorithm',
                'query': 'SELECT (COUNT(?entity) as ?count) WHERE { ?entity rdf:type bodi:Algorithm . }',
                'validation': lambda r: self._validate_single_entity_count(r, 'bodi:Algorithm')
            },      
            {
            'name': 'record_without_label_ask',
            'description': '⚡ RECORD senza label - Test rapido',
            'query': '''
                ASK {
                    ?entity rdf:type rico:Record .
                    FILTER NOT EXISTS { ?entity rdfs:label ?label }
                }
            ''',
            'validation': lambda r: self._validate_ask_false(
                r,
                "Record senza rdfs:label",
                "🔍 Alcuni Record non hanno etichette descrittive.",
                "💡 Se TRUE, eseguire una SELECT per vedere quali Record mancano di label."
            )
        },
        {
            'name': 'recordset_without_label_ask',
            'description': '⚡ RECORDSET senza label - Test rapido',
            'query': '''
                ASK {
                    ?entity rdf:type rico:RecordSet .
                    FILTER NOT EXISTS { ?entity rdfs:label ?label }
                }
            ''',
            'validation': lambda r: self._validate_ask_false(
                r,
                "RecordSet senza rdfs:label",
                "🔍 Alcuni RecordSet non hanno etichette descrittive.",
                "💡 Se TRUE, eseguire una SELECT per vedere quali RecordSet mancano di label."
            )
        },
        {
            'name': 'instantiation_without_label_ask',
            'description': '⚡ INSTANTIATION senza label - Test rapido',
            'query': '''
                ASK {
                    ?entity rdf:type rico:Instantiation .
                    FILTER NOT EXISTS { ?entity rdfs:label ?label }
                }
            ''',
            'validation': lambda r: self._validate_ask_false(
                r,
                "Instantiation senza rdfs:label",
                "🔍 Alcune Instantiation non hanno etichette descrittive.",
                "💡 Se TRUE, eseguire una SELECT per vedere quali Instantiation mancano di label."
            )
        },
        {
            'name': 'technicalmetadatatype_without_label_ask',
            'description': '⚡ TECHNICAL METADATA TYPE senza label - Test rapido',
            'query': '''
                ASK {
                    ?entity rdf:type bodi:TechnicalMetadataType .
                    FILTER NOT EXISTS { ?entity rdfs:label ?label }
                }
            ''',
            'validation': lambda r: self._validate_ask_false(
                r,
                "TechnicalMetadataType senza rdfs:label",
                "🔍 Alcuni tipi di metadato tecnico non hanno etichette descrittive.",
                "💡 Se TRUE, eseguire una SELECT per vedere quali tipi mancano di label."
            )
        },
        {
            'name': 'activity_without_label_ask',
            'description': '⚡ ACTIVITY senza label - Test rapido',
            'query': '''
                ASK {
                    ?entity rdf:type rico:Activity .
                    FILTER NOT EXISTS { ?entity rdfs:label ?label }
                }
            ''',
            'validation': lambda r: self._validate_ask_false(
                r,
                "Activity senza rdfs:label",
                "🔍 Alcune Activity non hanno etichette descrittive.",
                "💡 Se TRUE, eseguire una SELECT per vedere quali Activity mancano di label."
            )
        },
        {
            'name': 'person_without_label_ask',
            'description': '⚡ PERSON senza label - Test rapido',
            'query': '''
                ASK {
                    ?entity rdf:type rico:Person .
                    FILTER NOT EXISTS { ?entity rdfs:label ?label }
                }
            ''',
            'validation': lambda r: self._validate_ask_false(
                r,
                "Person senza rdfs:label",
                "🔍 Alcune entità di tipo Person non hanno etichette descrittive.",
                "💡 Se TRUE, eseguire una SELECT per vedere quali Person mancano di label."
            )
        }

        ]
        
        self._execute_query_batch(queries, "STATISTICHE GENERALI DEL DATASET")

    def _validate_mime_type_distribution(self, result: ValidationResult) -> ValidationResult:
        """Valida la distribuzione dei MIME types nel dataset"""
        if result.status == 'FAIL':
            return result
            
        if result.result_count == 0:
            result.status = 'WARNING'
            result.description = "⚠️ NESSUN MIME TYPE: Non trovati MIME types nel dataset\n   💡 SIGNIFICATO: Potrebbe mancare l'estrazione metadati o la proprietà MIMEType"
        else:
            # Analizza i risultati
            total_files = sum(int(detail.get('count', 0)) for detail in result.details)
            unique_types = result.result_count
            
            # Trova i tipi più comuni
            top_types = sorted(result.details, key=lambda x: int(x.get('count', 0)), reverse=True)[:5]
            
            result.status = 'INFO'
            result.description = f"📊 DISTRIBUZIONE MIME TYPES:\n"
            result.description += f"   📈 File totali: {total_files:,}\n"
            result.description += f"   🏷️ Tipi unici: {unique_types:,}\n"
            result.description += f"   🔝 Top 5 tipi:\n"
            
            for i, detail in enumerate(top_types, 1):
                mime_type = detail.get('value', 'Unknown')
                count = int(detail.get('count', 0))
                percentage = (count / total_files * 100) if total_files > 0 else 0
                result.description += f"      {i}. {mime_type}: {count:,} file ({percentage:.1f}%)\n"
            
            result.description += f"   💡 SIGNIFICATO: Panoramica dei formati di file nell'archivio"
            
            # Verifica se ci sono tipi inaspettati o problematici
            problematic_types = []
            for detail in result.details:
                mime_type = detail.get('value', '').lower()
                if 'error' in mime_type or 'unknown' in mime_type or mime_type == '':
                    problematic_types.append(detail)
            
            if problematic_types:
                result.status = 'WARNING'
                problem_count = sum(int(d.get('count', 0)) for d in problematic_types)
                result.description += f"\n   ⚠️ ATTENZIONE: {problem_count:,} file con MIME type problematici"
        
        return result

    def download_mime_types_csv(self, filename: str = None) -> str:
        """Scarica tutti i MIME types con conteggio in CSV usando la query specifica"""
        self.logger.info("📊 DOWNLOAD MIME TYPES CSV...")
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"mime_types_distribution_{timestamp}.csv"
        
        query = self.engine.prefixes + """
        SELECT ?value (COUNT(*) AS ?count)
        WHERE {
        # Trova il tipo MIMEType
        ?metadataType rdf:type bodi:TechnicalMetadataType ;
                        rdfs:label "MIMEType" .

        # Trova le istanze di metadata che usano questo tipo
        ?metadataInstance bodi:hasTechnicalMetadataType ?metadataType .

        # Trova il valore del metadata (assumendo rdfs:label come proprietà valore)
        ?metadataInstance rdfs:label ?value .
        }
        GROUP BY ?value
        ORDER BY ?value
        """
        
        try:
            start_time = time.time()
            
            # Richiesta CSV diretta
            response = self.engine.session.post(
                self.engine.endpoint,
                data={'query': query},
                headers={
                    'Accept': 'text/csv',
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                timeout=1800  # 30 minuti per query grandi
            )
            
            query_time = time.time() - start_time
            
            if response.status_code == 200:
                # Salva CSV
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                # Conta righe e analizza per statistiche
                lines = response.text.strip().split('\n')
                header_line = lines[0] if lines else ""
                data_lines = lines[1:] if len(lines) > 1 else []
                
                # Calcola statistiche dai dati CSV
                total_files = 0
                unique_types = len(data_lines)
                top_types = []
                
                for line in data_lines:
                    if line.strip():
                        parts = line.split(',')
                        if len(parts) >= 2:
                            try:
                                mime_type = parts[0].strip('"')
                                count = int(parts[1].strip('"'))
                                total_files += count
                                top_types.append((mime_type, count))
                            except (ValueError, IndexError):
                                continue
                
                # Ordina per conteggio (Top 5)
                top_types.sort(key=lambda x: x[1], reverse=True)
                top_5 = top_types[:5]
                
                self.logger.info(f"✅ MIME Types CSV scaricato in {query_time:.2f}s")
                self.logger.info(f"📄 File salvato: {filename}")
                self.logger.info(f"📊 Statistiche MIME Types:")
                self.logger.info(f"   📈 File totali: {total_files:,}")
                self.logger.info(f"   🏷️ MIME types unici: {unique_types}")
                
                if top_5:
                    self.logger.info(f"   🔝 Top 5 MIME types:")
                    for i, (mime_type, count) in enumerate(top_5, 1):
                        percentage = (count / total_files * 100) if total_files > 0 else 0
                        # Tronca MIME type lunghi per display
                        display_mime = mime_type if len(mime_type) <= 25 else mime_type[:22] + "..."
                        self.logger.info(f"      {i}. {display_mime}: {count:,} ({percentage:.1f}%)")
                
                return filename
                
            else:
                self.logger.error(f"❌ Download MIME CSV fallito: HTTP {response.status_code}")
                if response.text:
                    self.logger.error(f"Errore: {response.text[:200]}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Errore durante download MIME CSV: {e}")
            return None

    def _validate_ask_true(self, result: ValidationResult, short_desc: str, meaning: str, action: str) -> ValidationResult:
        """Valida che una query ASK ritorni true (opposto di _validate_ask_false)"""
        if result.status == 'FAIL':
            return result
        
        # Per query ASK, controlla il risultato booleano
        ask_result = False
        if result.details and len(result.details) > 0:
            first_result = result.details[0]
            ask_result = (
                first_result.get('result') == 'true' or
                first_result.get('ASK') == 'true' or 
                str(first_result.get('boolean', 'false')).lower() == 'true'
            )
        
        if ask_result:
            result.status = 'PASS'
            result.description = f"✅ CONFERMATO: {short_desc} verificato"
        else:
            result.status = 'FAIL'
            result.description = f"❌ PROBLEMA: {short_desc} non verificato\n   {meaning}\n   {action}"
        
        return result

    def _validate_distribution_info(self, result: ValidationResult) -> ValidationResult:
        """Valida informazioni di distribuzione generiche"""
        if result.status == 'FAIL':
            return result
        
        if result.result_count == 0:
            result.status = 'WARNING'
            result.description = "⚠️ NESSUNA DISTRIBUZIONE: Non trovati dati per l'analisi"
        else:
            result.status = 'INFO'
            result.description = f"📊 DISTRIBUZIONE TROVATA: {result.result_count} categorie/tipi\n"
            
            # Mostra prime 10 voci se disponibili
            display_count = min(10, len(result.details))
            for i, detail in enumerate(result.details[:display_count], 1):
                # Trova il campo principale (primo che non è 'count')
                main_field = None
                count_field = 0
                
                for key, value in detail.items():
                    if key.lower() == 'count':
                        try:
                            count_field = int(value)
                        except (ValueError, TypeError):
                            count_field = 0
                    elif main_field is None:
                        main_field = value
                
                if main_field:
                    # Tronca valori lunghi
                    if len(str(main_field)) > 50:
                        main_field = str(main_field)[:47] + "..."
                        
                    result.description += f"   {i:2d}. {main_field}: {count_field:,}\n"
            
            if len(result.details) > display_count:
                result.description += f"   ... +{len(result.details) - display_count} altri"
        
        return result

    def _validate_metadata_completeness(self, result: ValidationResult) -> ValidationResult:
        """Valida completezza dei metadati"""
        if result.status == 'FAIL':
            return result
        
        if result.result_count == 0:
            result.status = 'PASS'
            result.description = "✅ METADATI COMPLETI: Nessun problema di completezza rilevato"
        else:
            result.status = 'WARNING'
            incomplete_count = sum(int(detail.get('count', 0)) for detail in result.details if 'count' in detail)
            
            result.description = f"⚠️ METADATI INCOMPLETI: {incomplete_count:,} elementi con metadati mancanti\n"
            result.description += f"   🔍 SIGNIFICATO: File o entità senza metadati tecnici completi\n"
            result.description += f"   💡 AZIONE: Verificare pipeline estrazione metadati per copertura completa"
            
            # Mostra esempi se disponibili
            if result.details:
                result.description += f"\n   📋 Dettagli: {result.result_count} tipi di incompletezza rilevati"
        
        return result  


    def _validate_count_zero(self, result: ValidationResult, entity_type: str, error_message: str, suggestion: str) -> ValidationResult:
        """
        Valida che il count sia zero (nessun elemento trovato)
        """
        # Check se la query è già fallita
        if result.status == 'FAIL':
            return result
            
        try:
            # FIX: Usa result.details invece di result direttamente
            if not result.details or len(result.details) == 0:
                result.status = 'WARNING'
                result.description = f"⚠️ NESSUN RISULTATO per {entity_type}"
                return result
            
            # Estrai il count dal primo risultato
            count_value = result.details[0].get('count', '0')
            try:
                count = int(count_value)
            except (ValueError, TypeError):
                result.status = 'FAIL'
                result.description = f"❌ ERRORE PARSING COUNT per {entity_type}: {count_value}"
                return result
            
            if count == 0:
                result.status = 'PASS'
                result.description = f"✅ PERFETTO: Nessun {entity_type} trovato"
            else:
                result.status = 'FAIL'
                result.description = f"🚨 PROBLEMA: {count:,} {entity_type} rilevati\n   {error_message}\n   {suggestion}"
                
            return result
            
        except (KeyError, IndexError, AttributeError) as e:
            result.status = 'FAIL'
            result.description = f"❌ ERRORE PARSING RISULTATO per {entity_type}: {str(e)}"
            result.error_message = f"Parsing error: {str(e)}"
            return result
    
    def _run_structural_integrity(self):
        """2. INTEGRITÀ STRUTTURALE - Validazione completa struttura archivistica e rilevamento orfani"""
        self.logger.info("\n" + "="*60)
        self.logger.info("🏗️ CATEGORIA 2: INTEGRITÀ STRUTTURALE ARCHIVISTICA")
        self.logger.info("="*60)
        
        queries = [

            {
                'name': 'root_nodes_analysis',
                'description': '🏠 NODI ROOT - Analisi punti di partenza gerarchia (directory principali)',
                'query': '''
                SELECT ?rootId ?rootUri ?label ?hasParent WHERE {
                ?rootUri rdf:type rico:RecordSet .
                ?rootUri rico:hasOrHadIdentifier ?identifier .
                ?identifier rdf:type rico:Identifier .
                ?identifier rdfs:label ?rootId .
                ?rootUri rdfs:label ?label .
                
                FILTER(?rootId IN ("RS1_RS2", "RS1_RS1", "RS1_RS3"))
                
                BIND(EXISTS { ?rootUri rico:isOrWasIncludedIn ?parent } AS ?hasParent)
                }
                ORDER BY ?rootId
                ''',
                'validation': lambda r: self._validate_root_nodes(r)
            },
                        {
                'name': 'self_inclusion_check_ask',
                'description': '⚡ AUTO-INCLUSIONE ASK - Test veloce cicli diretti',
                'query': '''
                ASK {
                    {
                        ?entity rico:isOrWasIncludedIn ?entity .
                    } UNION {
                        ?entity rico:isOrWasPartOf ?entity .
                    }
                }
                ''',
                'validation': lambda r: self._validate_ask_false(r,
                    "auto-inclusioni nella gerarchia",
                    "🔍 Entità che includono se stesse nella gerarchia.",
                    "💡 Verificare logica generazione relazioni gerarchiche.")
            },
                        {
                'name': 'circular_hierarchy_check_ask',
                'description': '⚡ INCLUSIONE CIRCOLARE ASK - Test veloce cicli bidirezionali',
                'query': '''
                ASK {
                    ?entityA rico:isOrWasIncludedIn ?entityB .
                    ?entityB rico:isOrWasIncludedIn ?entityA .
                    FILTER(?entityA != ?entityB)
                }
                ''',
                'validation': lambda r: self._validate_ask_false(r,
                    "cicli bidirezionali nella gerarchia",
                    "🔍 Due entità si includono reciprocamente.",
                    "💡 Identificare direzione corretta relazione gerarchica.")
            },
            {
                'name': 'hierarchy_depth_inconsistencies',
                'description': '📏 PROFONDITÀ INCONGRUENTI - Relazioni parent-child con profondità incongruenti',
                'query': '''
                SELECT ?childInst ?childDepth ?parentInst ?parentDepth ?depthDifference WHERE { 
                    ?childInst rico:isOrWasIncludedIn ?parentInst . 
                    ?childInst bodi:hierarchyDepth ?childDepth . 
                    ?parentInst bodi:hierarchyDepth ?parentDepth . 
                    BIND((?childDepth - ?parentDepth) AS ?depthDifference) 
                    
                    # La differenza dovrebbe essere esattamente 1 
                    FILTER(?depthDifference != 1) 
                } ORDER BY ?depthDifference
                ''',
                'validation': lambda r: self._validate_zero_results_warning(r,
                    "relazioni parent-child con profondità incongruenti trovate",
                    "🔍 SIGNIFICATO: La differenza di profondità tra parent e child non è 1. Indica possibile errore nel calcolo delle profondità gerarchiche.",
                    "💡 AZIONE: Ricalcolare le profondità gerarchiche assicurandosi che child = parent + 1.")
            },
            {
                'name': 'files_with_children_ask',
                'description': '⚡ FILE CON FIGLI ASK - Test veloce Record con children',
                'query': '''
                ASK {
                    ?record rdf:type rico:Record .
                    ?record rico:includesOrIncluded ?child .
                }
                ''',
                'validation': lambda r: self._validate_ask_false(r,
                    "file (Record) con children",
                    "🔍 I file dovrebbero essere foglie nella gerarchia.",
                    "💡 Verificare se sono file o dovrebbero essere RecordSet.")
            },
            {
                'name': 'orphan_instantiations_ask',
                'description': '⚡ ISTANZIAZIONI ORFANE ASK - Test veloce instantiation senza record',
                'query': '''
                ASK {
                    ?inst rdf:type rico:Instantiation .
                    FILTER NOT EXISTS { ?record rico:hasOrHadInstantiation ?inst }
                }
                ''',
                'validation': lambda r: self._validate_ask_false(r,
                    "istanziazioni orfane",
                    "🔍 File elaborati ma non collegati alla struttura archivistica.",
                    "💡 Verificare logica collegamento Record→Instantiation.")
            },
            {
                'name': 'orphan_records_ask',
                'description': '⚡ RECORD ORFANI ASK - Test veloce record senza istanziazione',
                'query': '''
                ASK {
                    ?record rdf:type ?recordType .
                    FILTER(?recordType IN (rico:Record, rico:RecordSet))
                    FILTER NOT EXISTS { ?record rico:hasOrHadInstantiation ?inst }
                    FILTER EXISTS { ?record rico:isOrWasIncludedIn ?parent }
                }
                ''',
                'validation': lambda r: self._validate_ask_false(r,
                    "record non-root senza istanziazioni",
                    "🔍 Record nella struttura logica senza rappresentazione fisica.",
                    "💡 Verificare che tutti i record abbiano istanziazione.")
            }
            ,
            {
            'name': 'location_duplicates_check',
            'description': '📍 DUPLICATI LOCATION - Verifica unicità percorsi fisici',
            'query': '''SELECT ?locationUri (COUNT(?inst) AS ?usageCount) WHERE {
                ?inst prov:atLocation ?locationUri .
            }
            GROUP BY ?locationUri
            HAVING (COUNT(?inst) > 1)''',
            'validation': lambda r: self._validate_zero_results_warning(r,
                "location condivise tra multiple instantiation trovate",
                "🔍 SIGNIFICATO: Stesso percorso fisico assegnato a istanziazioni diverse. Potrebbe indicare errore nella generazione o veri duplicati.",
                "💡 AZIONE: Verificare se sono legittimi duplicati fisici o errore di mapping.")
        }
        ]
        
        self._execute_query_batch(queries, "INTEGRITÀ REFERENZIALE METADATI")
        self._cleanup_heavy_dataset_optimizations()

    def _run_metadata_validation(self):
        """4. VALIDAZIONE METADATI - Consistenza metadati tecnici da Tika, ExifTool, FileSystem"""
        self.logger.info("\n" + "="*60)
        self.logger.info("📋 CATEGORIA 4: VALIDAZIONE METADATI TECNICI")
        self.logger.info("="*60)
        
        queries = [
            
            {
                'name': 'orphan_records_ask',
                'description': '⚡ RECORD ORFANI ASK - Test veloce record senza istanziazione',
                'query': '''
                ASK {
                    ?record rdf:type ?recordType .
                    FILTER(?recordType IN (rico:Record, rico:RecordSet))
                    FILTER NOT EXISTS { ?record rico:hasOrHadInstantiation ?inst }
                    FILTER EXISTS { ?record rico:isOrWasIncludedIn ?parent }
                }
                ''',
                'validation': lambda r: self._validate_ask_false(r,
                    "record non-root senza istanziazioni",
                    "🔍 Record nella struttura logica senza rappresentazione fisica.",
                    "💡 Verificare che tutti i record abbiano istanziazione.")
            }
            ,
            {
                'name': 'instantiation_without_metadata_ask',
                'description': '⚡ INSTANTIATION SENZA METADATI ASK - Test veloce copertura',
                'query': '''
                ASK {
                    ?inst rdf:type rico:Instantiation .
                    FILTER NOT EXISTS { ?inst bodi:hasTechnicalMetadata ?metadata }
                }
                ''',
                'validation': lambda r: self._validate_ask_false(r,
                    "instantiation senza metadati",
                    "🔍 File senza metadati tecnici estratti.",
                    "💡 Verificare pipeline estrazione metadati.")
            },
            
            {
            'name': 'metadata_stats_simple',
            'description': '📈 STATISTICHE METADATI - Conteggi base per tipo',
            'query': '''
            SELECT 
                (COUNT(DISTINCT ?activity) AS ?activities)
                (COUNT(DISTINCT ?metaType) AS ?metadataTypes) 
                (COUNT(DISTINCT ?metadata) AS ?metadataInstances)
            WHERE { 
                {
                    ?activity rdf:type rico:Activity . 
                    FILTER(CONTAINS(STR(?activity), "metaextr"))
                } UNION {
                    ?metaType rdf:type bodi:TechnicalMetadataType .
                } UNION {
                    ?metadata rdf:type bodi:TechnicalMetadata .
                }
            }
            ''',
            'validation': lambda r: self._validate_basic_info(r)
        },
             {
            'name': 'activity_incomplete_optimized',
            'description': '⚡ ACTIVITY INCOMPLETE - Test activity senza supervisor/date (ottimizzato)',
            'query': '''
            ASK {
                ?activity rdf:type rico:Activity .
                FILTER(CONTAINS(STR(?activity), "metaextr"))
                
                MINUS {
                    ?activity bodi:hasOrHadSupervisor ?supervisor .
                    ?activity rico:occurredAtDate ?date .
                }
            }
            ''',
            'validation': lambda r: self._validate_ask_false(r,
                "activity di estrazione incomplete",
                "🔍 Activity senza supervisor o data di esecuzione.",
                "💡 Verificare completezza create_extraction_activity.")
        },
                    
        {
                'name': 'metadata_without_activity_count',
                'description': '⚡ METADATI SENZA ACTIVITY - Count diretto',
                'query': '''
                    SELECT (COUNT(?metadata) AS ?count) WHERE {
                        ?metadata rdf:type bodi:TechnicalMetadata .
                        FILTER NOT EXISTS { ?metadata bodi:generatedBy ?activity }
                    }
                ''',
                'validation': lambda r: self._validate_count_zero(r,
                    "metadati senza activity generatrice",
                    "🔍 Trovati metadati tecnici non collegati all'activity.",
                    "💡 Verificare collegamento bodi:generatedBy.")
            },
                    {
            'name': 'exiftool_metadata_count',
            'description': '📊 EXIFTOOL - Conteggio metadati',
            'query': '''
                SELECT (COUNT(*) AS ?count) WHERE {
                    ?metadata rdf:type bodi:TechnicalMetadata .
                    ?metadata bodi:hasTechnicalMetadataType ?type .
                    FILTER(CONTAINS(STR(?type), "exiftool_tmtype"))
                }
            ''',
            'validation': lambda r: self._validate_basic_info(r)
        },

        {
            'name': 'tika_metadata_count',
            'description': '📊 APACHE TIKA - Conteggio metadati',
            'query': '''
                SELECT (COUNT(*) AS ?count) WHERE {
                    ?metadata rdf:type bodi:TechnicalMetadata .
                    ?metadata bodi:hasTechnicalMetadataType ?type .
                    FILTER(CONTAINS(STR(?type), "apachetika_tmtype"))
                }
            ''',
            'validation': lambda r: self._validate_basic_info(r)
        },

        {
            'name': 'filesystem_metadata_count',
            'description': '📊 FILESYSTEM - Conteggio metadati',
            'query': '''
                SELECT (COUNT(*) AS ?count) WHERE {
                    ?metadata rdf:type bodi:TechnicalMetadata .
                    ?metadata bodi:hasTechnicalMetadataType ?type .
                    FILTER(CONTAINS(STR(?type), "os_tmtype"))
                }
            ''',
            'validation': lambda r: self._validate_basic_info(r)
        },

                {
    'name': 'metadata_without_type_count',
    'description': '⚡ METADATI SENZA TIPO - Count veloce',
    'query': '''
    SELECT (COUNT(?metadata) AS ?count) WHERE {
        ?metadata rdf:type bodi:TechnicalMetadata .
        FILTER NOT EXISTS { ?metadata bodi:hasTechnicalMetadataType ?type }
    }
    ''',
    'validation': lambda r: self._validate_count_zero(r,
        "metadati senza tipo",
        "🔍 Metadati tecnici non collegati a un tipo specifico.",
        "💡 Verificare creazione collegamenti hasTechnicalMetadataType.")
},
{
    'name': 'metadata_type_orphans_count',
    'description': '⚡ TIPI ORFANI - Count veloce tipi senza utilizzo',
    'query': '''
    SELECT (COUNT(?type) AS ?count) WHERE {
        ?type rdf:type bodi:TechnicalMetadataType .
        FILTER NOT EXISTS { ?metadata bodi:hasTechnicalMetadataType ?type }
    }
    ''',
    'validation': lambda r: self._validate_count_zero(r,
        "tipi di metadato orfani",
        "🔍 Tipi definiti ma mai utilizzati da metadati.",
        "💡 Rimuovere tipi inutilizzati o verificare collegamenti.")
},
            {
            'name': 'mime_type_distribution',
            'description': '📊 DISTRIBUZIONE MIME TYPES - Analisi tipi di file nel dataset',
            'query': '''
                SELECT ?value (COUNT(*) AS ?count)
                WHERE {
                  # Trova il tipo MIMEType
                  ?metadataType rdf:type bodi:TechnicalMetadataType ;
                                rdfs:label "MIMEType" .
                  # Trova le istanze di metadata che usano questo tipo
                  ?metadataInstance bodi:hasTechnicalMetadataType ?metadataType .
                  # Trova il valore del metadata (assumendo rdfs:label come proprietà valore)
                  ?metadataInstance rdfs:label ?value .
                }
                GROUP BY ?value
                ORDER BY DESC(?count)
            ''',
            'validation': lambda r: self._validate_mime_type_distribution(r)
        }
        ]
        
        self._execute_query_batch(queries, "VALIDAZIONE METADATI TECNICI")

    def _run_comprehensive_hash_validation(self):
        """5. VALIDAZIONE HASH E INTEGRITÀ COMPLETA - Tutti i controlli hash raggruppati"""
        self.logger.info("\n" + "="*60)
        self.logger.info("🔐 CATEGORIA 5: VALIDAZIONE HASH E INTEGRITÀ COMPLETA")
        self.logger.info("="*60)
        
        queries = [
            {
                'name': 'hash_without_algorithm_ask',
                'description': '⚡ HASH SENZA ALGORITMO ASK - Test veloce hash senza creatore',
                'query': '''
                ASK {
                    ?hash rdf:type premis:Fixity .
                    FILTER NOT EXISTS { 
                        ?hash rico:hasCreator ?algorithm . 
                        ?algorithm rdf:type bodi:Algorithm . 
                    }
                }
                ''',
                'validation': lambda r: self._validate_ask_false(r,
                    "hash senza algoritmo creatore",
                    "🔍 Hash calcolati senza riferimento algoritmo.",
                    "💡 Collegare ogni hash al suo algoritmo (SHA-256).")
            }
            ,
            {
                'name': 'hash_missing_algorithm',
                'description': '🔍 HASH SENZA ALGORITMO - Trova hash che non specificano l\'algoritmo creatore',
                'query': '''
                SELECT ?fixity ?hashValue ?inst 
                WHERE { 
                    # Pattern principale: trova tutti i fixity con hash
                    ?inst bodi:hasHashCode ?fixity . 
                    ?fixity rdf:value ?hashValue . 
                    
                    # ESCLUDI quelli che hanno anche questo pattern
                    MINUS { 
                        ?fixity rico:hasCreator ?algorithm . 
                        ?algorithm rdf:type bodi:Algorithm . 
                    }
                }
                ''',
                'validation': lambda r: self._validate_zero_results_critical(r,
                    "hash senza algoritmo creatore trovati",
                    "🔍 SIGNIFICATO: Hash calcolati ma privi di riferimento all'algoritmo utilizzato. Necessario per verifiche di integrità.",
                    "💡 AZIONE: Collegare ogni hash al suo algoritmo (SHA-256) con rico:hasCreator.")
            },
            {
                'name': 'hash_algorithm_consistency',
                'description': '🔒 ALGORITMO HASH - Verifica utilizzo esclusivo SHA-256',
                'query': '''
                SELECT ?algorithm ?characteristic (COUNT(?hash) as ?hash_count) WHERE {
                    ?hash rdf:type premis:Fixity .
                    ?hash rico:hasCreator ?algorithm .
                    ?algorithm rico:hasTechnicalCharacteristic ?characteristic .
                } GROUP BY ?algorithm ?characteristic
                ''',
                'validation': lambda r: self._validate_hash_algorithms(r)
            },
            {
    'name': 'hash_format_invalid_ask',
    'description': '⚡ FORMATO HASH INVALIDO ASK - Test veloce formato non SHA-256',
    'query': '''
    ASK {
                ?fixity rdf:type premis:Fixity .
                ?fixity rdf:value ?hashValue .
                FILTER(!REGEX(STR(?hashValue), "^[a-fA-F0-9]{64}$"))
            }
            ''',
            'validation': lambda r: self._validate_ask_false(r,
                "hash con formato non SHA-256",
                "🔍 Hash che non rispettano formato SHA-256 standard.",
                "💡 Verificare algoritmo calcolo hash (64 caratteri hex).")
        },
            {
            'name': 'multiple_hashes_per_file_ask',
            'description': '⚡ HASH MULTIPLI ASK - Test veloce file con più hash',
            'query': '''
            ASK {
                ?inst rdf:type rico:Instantiation .
                ?inst bodi:hasHashCode ?hash1 .
                ?inst bodi:hasHashCode ?hash2 .
                FILTER(?hash1 != ?hash2)
            }
            ''',
            'validation': lambda r: self._validate_ask_false(r,
                "file con hash multipli",
                "🔍 Instantiation con più di un hash associato.",
                "💡 Ogni file dovrebbe avere esattamente un hash.")
        },
            {
                'name': 'duplicate_hash_analysis',
                'description': '👥 DUPLICATI FILE - Analisi file con hash identici (potenziali copie)',
                'query': '''
                SELECT ?hash_value (COUNT(?inst) as ?duplicate_count) WHERE {
                    ?inst bodi:hasHashCode ?hash .
                    ?hash rdf:value ?hash_value .
                } GROUP BY ?hash_value HAVING (?duplicate_count > 1)
                ORDER BY DESC(?duplicate_count)
                ''',
                'validation': lambda r: self._validate_hash_duplicates(r)
            }
        ]
        
        self._execute_query_batch(queries, "VALIDAZIONE HASH E INTEGRITÀ COMPLETA")


    def _run_advanced_consistency(self):
        """7. CONSISTENZA AVANZATA - Controlli complessi e edge cases specifici"""
        self.logger.info("\n" + "="*60)
        self.logger.info("🔬 CATEGORIA 7: CONSISTENZA AVANZATA E EDGE CASES")
        self.logger.info("="*60)
        
        queries = [
            {
                'name': 'path_format_invalid_ask',
                'description': '⚡ PATH INVALIDI ASK - Test veloce percorsi senza slash iniziale',
                'query': '''
                ASK {
                    ?location rdf:type prov:Location .
                    ?location rdfs:label ?path .
                    FILTER(!STRSTARTS(?path, "/"))
                }
                ''',
                'validation': lambda r: self._validate_ask_false(r,
                    "percorsi senza slash iniziale",
                    "🔍 Percorsi che non seguono formato Unix standard.",
                    "💡 Standardizzare percorsi per iniziare con '/'.")
            },
                        {
                'name': 'multiple_paths_per_instantiation_ask',
                'description': '⚡ PATH MULTIPLI ASK - Test veloce instantiation con più location',
                'query': '''
                ASK {
                    ?inst rdf:type rico:Instantiation .
                    ?inst prov:atLocation ?loc1 .
                    ?inst prov:atLocation ?loc2 .
                    FILTER(?loc1 != ?loc2)
                }
                ''',
                'validation': lambda r: self._validate_ask_false(r,
                    "instantiation con multiple location",
                    "🔍 File con più percorsi fisici associati.",
                    "💡 Ogni instantiation dovrebbe avere una sola location.")
            }
        ]
        
        self._execute_query_batch(queries, "CONSISTENZA AVANZATA E EDGE CASES")

    def download_metadata_types_csv(self, filename: str = None) -> str:
        """Scarica tutti i metadata types con strumento e conteggio in CSV"""
        self.logger.info("📊 DOWNLOAD METADATA TYPES CSV...")
        
        # Usa nome personalizzato se specificato nel validator
        if not filename and hasattr(self, 'custom_metadata_csv_filename'):
            filename = self.custom_metadata_csv_filename
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"metadata_types_report_{timestamp}.csv"
        
        query = self.engine.prefixes + """
        SELECT ?metadataType ?metadataTypeLabel ?softwareLabel (COUNT(?metadataInstance) AS ?count)
        WHERE {
            ?metadataType rdf:type bodi:TechnicalMetadataType .
            ?metadataType rdfs:label ?metadataTypeLabel .
            ?metadataType bodi:generatedBy ?software .
            ?software rdf:type bodi:Software .
            ?software rdfs:label ?softwareLabel .
            ?metadataInstance bodi:hasTechnicalMetadataType ?metadataType .
        }
        GROUP BY ?metadataType ?metadataTypeLabel ?softwareLabel
        ORDER BY DESC(?count) ?softwareLabel ?metadataTypeLabel
        """
        
        try:
            start_time = time.time()
            
            # Richiesta CSV diretta
            response = self.engine.session.post(
                self.engine.endpoint,
                data={'query': query},
                headers={
                    'Accept': 'text/csv',
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                timeout=1800  # 30 minuti per query grandi
            )
            
            query_time = time.time() - start_time
            
            if response.status_code == 200:
                # Salva CSV
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                # Conta righe per statistiche
                lines = response.text.count('\n')
                file_size = len(response.text)
                
                self.logger.info(f"✅ CSV scaricato in {query_time:.2f}s")
                self.logger.info(f"📄 File salvato: {filename}")
                self.logger.info(f"📊 Righe totali: {lines:,}")
                self.logger.info(f"📁 Dimensione: {file_size:,} caratteri")
                
                return filename
                
            else:
                self.logger.error(f"❌ Download CSV fallito: HTTP {response.status_code}")
                if response.text:
                    self.logger.error(f"Errore: {response.text[:200]}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Errore durante download CSV: {e}")
            return None
    
    def _run_csv_export(self):
        """8. EXPORT CSV - Download report metadata types + MIME types distribution"""
        self.logger.info("\n" + "="*60)
        self.logger.info("📊 CATEGORIA 8: EXPORT CSV METADATA TYPES E MIME TYPES")
        self.logger.info("="*60)
        
        csv_results = []
        
        # 1. Download CSV metadata types (esistente)
        if not hasattr(self, 'skip_metadata_csv') or not self.skip_metadata_csv:
            self.logger.info("📋 Generazione CSV metadata types completo...")
            csv_filename = self.download_metadata_types_csv()
            if csv_filename:
                self.logger.info(f"✅ Report metadata types generato: {csv_filename}")
                csv_results.append(('metadata_types', csv_filename))
            else:
                csv_results.append(('metadata_types', 'FAILED'))
        
        # 2. Download CSV MIME types distribution (NUOVO)
        if not hasattr(self, 'skip_mime_csv') or not self.skip_mime_csv:
            self.logger.info("\n🎭 Generazione CSV MIME types distribution...")
            mime_csv_filename = self.download_mime_types_csv()
            if mime_csv_filename:
                self.logger.info(f"✅ Report MIME types generato: {mime_csv_filename}")
                csv_results.append(('mime_types', mime_csv_filename))
            else:
                csv_results.append(('mime_types', 'FAILED'))
        
        # 3. Crea ValidationResult per entrambi i CSV
        successful_csvs = [result for result in csv_results if result[1] != 'FAILED']
        failed_csvs = [result for result in csv_results if result[1] == 'FAILED']
        
        if successful_csvs:
            details = [{'csv_type': csv_type, 'csv_file': filename} for csv_type, filename in successful_csvs]
            
            csv_result = ValidationResult(
                query_name="csv_export_combined",
                description=f"📊 CSV EXPORT COMPLETATO: {len(successful_csvs)} file generati",
                status='INFO',
                result_count=len(successful_csvs),
                details=details,
                execution_time_ms=0
            )
            
            # Log riassuntivo
            self.logger.info(f"\n📊 RIEPILOGO EXPORT CSV:")
            for csv_type, filename in successful_csvs:
                self.logger.info(f"   ✅ {csv_type}: {filename}")
            
            if failed_csvs:
                for csv_type, _ in failed_csvs:
                    self.logger.info(f"   ❌ {csv_type}: FAILED")
            
            self.results.append(csv_result)
        else:
            # Tutti i CSV sono falliti
            csv_error_result = ValidationResult(
                query_name="csv_export_combined",
                description="❌ CSV EXPORT COMPLETAMENTE FALLITO",
                status='FAIL',
                result_count=0,
                details=[],
                execution_time_ms=0,
                error_message="Tutti i download CSV sono falliti"
            )
            self.results.append(csv_error_result)

    def _setup_heavy_dataset_optimizations(self):
        """Setup ottimizzazioni per dataset enormi (16GB+)"""
        self.logger.info("🐘 Setup ottimizzazioni per dataset 16GB+...")
        
        analytic_ok = self.engine.enable_analytic_mode()
        tm_ok = self.engine.setup_truth_maintenance_optimization()
        
        if not analytic_ok:
            self.logger.warning("⚠️ Analytic mode non configurato - usando modalità standard")
        if not tm_ok:
            self.logger.warning("⚠️ Truth maintenance non ottimizzato")
            
        return analytic_ok or tm_ok

    def _cleanup_heavy_dataset_optimizations(self):
        """Cleanup ottimizzazioni dataset enormi"""
        self.logger.info("🧹 Cleanup ottimizzazioni dataset pesante...")
        self.engine.restore_truth_maintenance()
    
    # =============== METODI DI VALIDAZIONE ===============
    
    def _validate_triple_count_info(self, result: ValidationResult) -> ValidationResult:
        """Valida il conteggio delle triple - solo informativo"""
        if result.status == 'FAIL':
            return result
            
        count = int(result.details[0]['count']) if result.details else 0
        
        result.status = 'INFO'
        if count > 1000000:
            result.description = f"📊 DATASET GRANDE: {count:,} triple RDF nel database\n   💡 SIGNIFICATO: Dataset sostanzioso con molte informazioni strutturate"
        elif count > 100000:
            result.description = f"📊 DATASET MEDIO: {count:,} triple RDF nel database\n   💡 SIGNIFICATO: Dataset di dimensioni medie"
        elif count > 10000:
            result.description = f"📊 DATASET PICCOLO: {count:,} triple RDF nel database\n   💡 SIGNIFICATO: Dataset contenuto ma utilizzabile"
        else:
            result.description = f"📊 DATASET MINIMO: {count:,} triple RDF nel database\n   💡 SIGNIFICATO: Dataset molto piccolo, potrebbe essere incompleto"
            
        return result
    
    def _validate_metadata_distribution_simple(self, result: ValidationResult) -> ValidationResult:
        """Valida distribuzione metadati semplificata per triples mode"""
        if result.status == 'FAIL':
            return result
            
        if result.result_count == 0:
            result.status = 'WARNING'
            result.description = "⚠️ NESSUN METADATO: Non trovati metadati tecnici nel dataset"
        else:
            total_metadata = sum(int(detail.get('count', 0)) for detail in result.details)
            tools_found = set(detail.get('toolType', 'Unknown') for detail in result.details)
            
            result.status = 'PASS' if len(tools_found) >= 2 else 'WARNING'
            result.description = f"📊 METADATI TROVATI: {total_metadata:,} da {len(tools_found)} strumenti\n"
            
            for detail in result.details:
                tool = detail.get('toolType', 'Unknown')
                count = int(detail.get('count', 0))
                result.description += f"   🔹 {tool}: {count:,}\n"
                
            result.description += f"   💡 NOTA: Analisi senza named graphs"
        
        return result
    
    def _validate_basic_info(self, result: ValidationResult) -> ValidationResult:
        """Validazione base per query informative"""
        if result.status == 'FAIL':
            return result
        
        result.status = 'INFO'
        if result.details:
            # Crea descrizione basata sui campi disponibili
            info_lines = []
            for key, value in result.details[0].items():
                if isinstance(value, str) and value.isdigit():
                    value = f"{int(value):,}"
                info_lines.append(f"   📊 {key}: {value}")
            
            result.description = "📈 INFORMAZIONI DATASET:\n" + "\n".join(info_lines)
        else:
            result.description = "📊 Nessuna informazione disponibile"
        
        return result

    def _validate_ask_false(self, result: ValidationResult, short_desc: str, meaning: str, action: str) -> ValidationResult:
        """Valida che una query ASK ritorni false"""
        if result.status == 'FAIL':
            return result
        
        # Per query ASK, controlla il risultato booleano
        ask_result = False
        if result.details and len(result.details) > 0:
            # Blazegraph può restituire diversi formati
            first_result = result.details[0]
            ask_result = (
                first_result.get('result') == 'true' or
                first_result.get('ASK') == 'true' or 
                str(first_result.get('boolean', 'false')).lower() == 'true'
            )
        
        if not ask_result:
            result.status = 'PASS'
            result.description = f"✅ PERFETTO: {short_desc} non rilevati"
        else:
            result.status = 'FAIL'
            result.description = f"🚨 PROBLEMA: {short_desc} esistono\n   {meaning}\n   {action}"
        
        return result
    
    def _validate_entity_counts(self, result: ValidationResult) -> ValidationResult:
        """Valida i conteggi delle entità principali"""
        if result.status == 'FAIL':
            return result
            
        if result.details:
            detail = result.details[0]
            records = int(detail.get('totalRecords', 0))
            recordsets = int(detail.get('totalRecordSets', 0))
            instantiations = int(detail.get('totalInstantiations', 0))
            
            result.status = 'INFO'
            result.description = f"📊 CONTEGGI ENTITÀ:\n"
            result.description += f"   📄 Record (file): {records:,}\n"
            result.description += f"   📁 RecordSet (directory): {recordsets:,}\n"
            result.description += f"   💾 Instantiation (fisiche): {instantiations:,}\n"
            result.description += f"   🧮 Totale logico (R+RS): {records + recordsets:,}\n"
            
            # Verifica bilanciamento logico-fisico
            total_logical = records + recordsets
            if instantiations == total_logical:
                result.description += f"   ✅ BILANCIAMENTO PERFETTO: Ogni entità logica ha istanziazione fisica"
            elif instantiations > total_logical:
                result.description += f"   ⚠️ ISTANZIAZIONI ECCEDENTI: {instantiations - total_logical} istanziazioni in più del previsto"
            else:
                result.description += f"   ❌ ISTANZIAZIONI MANCANTI: {total_logical - instantiations} istanziazioni mancanti"
                result.status = 'WARNING'
        else:
            result.status = 'FAIL'
            result.description = "❌ IMPOSSIBILE OTTENERE CONTEGGI ENTITÀ"
            
        return result
    
    def _validate_zero_results_critical(self, result: ValidationResult, short_desc: str, meaning: str, action: str) -> ValidationResult:
        """Valida che non ci siano risultati - versione critica con spiegazioni dettagliate"""
        if result.status == 'FAIL':
            return result
            
        if result.result_count == 0:
            result.status = 'PASS'
            result.description = f"✅ PERFETTO: Nessun problema critico - {short_desc.replace('trovate', 'rilevate').replace('trovati', 'rilevati')}"
        else:
            result.status = 'FAIL'
            result.description = f"🚨 PROBLEMA CRITICO: {result.result_count} {short_desc}\n   {meaning}\n   {action}"
            
        return result
    
    def _validate_zero_results_warning(self, result: ValidationResult, short_desc: str, meaning: str, action: str) -> ValidationResult:
        """Valida che non ci siano risultati - versione warning con spiegazioni"""
        if result.status == 'FAIL':
            return result
            
        if result.result_count == 0:
            result.status = 'PASS'
            result.description = f"✅ OTTIMO: Nessun problema - {short_desc.replace('trovate', 'rilevate').replace('trovati', 'rilevati')}"
        else:
            result.status = 'WARNING'
            result.description = f"⚠️ ATTENZIONE: {result.result_count} {short_desc}\n   {meaning}\n   {action}"
            
        return result
    
    def _validate_entity_distribution(self, result: ValidationResult) -> ValidationResult:
        """Valida la distribuzione dei tipi di entità"""
        if result.status == 'FAIL':
            return result
            
        # Verifica che ci siano almeno i tipi base
        types_found = {detail['type'].split('#')[-1] for detail in result.details}
        required_types = {'Record', 'RecordSet', 'Instantiation'}
        
        missing_types = required_types - types_found
        if missing_types:
            result.status = 'FAIL'
            result.description = f"❌ TIPI MANCANTI: {missing_types} non trovati\n   🔍 SIGNIFICATO: Mancano tipi di entità fondamentali nell'archivio\n   💡 AZIONE: Verificare che la pipeline generi tutti i tipi di entità necessari"
        else:
            result.status = 'PASS'
            distribution_text = []
            for detail in result.details:
                type_name = detail['type'].split('#')[-1]
                count = int(detail['count'])
                distribution_text.append(f"{type_name}: {count:,}")
            
            result.description = f"✅ DISTRIBUZIONE COMPLETA: {len(types_found)} tipi di entità trovati\n"
            result.description += f"   📊 Breakdown: {', '.join(distribution_text)}\n"
            result.description += f"   💡 SIGNIFICATO: Tutti i tipi base presenti, dataset ben strutturato"
            
        return result
    
    def _validate_dataset_overview(self, result: ValidationResult) -> ValidationResult:
        """Valida panoramica dataset per triples mode"""
        if result.status == 'FAIL':
            return result
            
        result.status = 'INFO'
        if result.details:
            detail = result.details[0]
            subjects = int(detail.get('totalSubjects', 0))
            triples = int(detail.get('totalTriples', 0))
            types = int(detail.get('totalTypes', 0))
            
            result.description = f"🗺️ PANORAMICA DATASET (TRIPLES MODE):\n"
            result.description += f"   📊 Soggetti unici: {subjects:,}\n"
            result.description += f"   🔢 Triple totali: {triples:,}\n"
            result.description += f"   🏷️ Tipi entità: {types:,}\n"
            result.description += f"   💡 NOTA: Dati aggregati da triple store senza named graphs"
        else:
            result.description = "📊 Nessuna informazione disponibile"
        
        return result
    
    def _validate_metadata_stats_simple(self, result: ValidationResult) -> ValidationResult:
        """Valida statistiche metadati semplici"""
        if result.status == 'FAIL':
            return result
            
        result.status = 'INFO'
        if result.details:
            detail = result.details[0]
            activities = int(detail.get('activities', 0))
            metadata_types = int(detail.get('metadataTypes', 0))
            metadata_instances = int(detail.get('metadataInstances', 0))
            extents = int(detail.get('extents', 0))
            
            result.description = f"📈 STATISTICHE METADATI:\n"
            result.description += f"   ⚙️ Activities: {activities:,}\n"
            result.description += f"   🏷️ Metadata Types: {metadata_types:,}\n"
            result.description += f"   📋 Metadata Instances: {metadata_instances:,}\n"
            result.description += f"   📏 Extents: {extents:,}\n"
            result.description += f"   💡 AGGREGAZIONE: Conteggi globali senza suddivisione per grafo"
        else:
            result.description = "📊 Nessuna statistica disponibile"
        
        return result

    def _validate_metadata_tool_distribution_simple(self, result: ValidationResult) -> ValidationResult:
        """Valida distribuzione tool semplificata"""
        if result.status == 'FAIL':
            return result
            
        result.status = 'INFO'
        if result.result_count == 0:
            result.description = "📊 Nessun metadata type trovato"
        else:
            tool_counts = {}
            for detail in result.details:
                tool = detail.get('tool', 'Unknown')
                usage = int(detail.get('usageCount', 0))
                tool_counts[tool] = tool_counts.get(tool, 0) + usage
            
            distribution_lines = ["📊 DISTRIBUZIONE METADATA TYPES PER TOOL:"]
            for tool, count in sorted(tool_counts.items(), key=lambda x: x[1], reverse=True):
                distribution_lines.append(f"   🛠️ {tool}: {count:,} utilizzi")
            
            distribution_lines.append(f"   💡 TOTAL TYPES: {result.result_count}")
            result.description = '\n'.join(distribution_lines)
            
        return result
    
    def _validate_metadata_distribution(self, result: ValidationResult) -> ValidationResult:
        """Valida la distribuzione dei metadati per fonte con spiegazioni"""
        if result.status == 'FAIL':
            return result
            
        sources = []
        for detail in result.details:
            graph = detail['graph']
            count = int(detail['count'])
            if 'TechMeta' in graph:
                if 'FileSystem' in graph or 'FS_TechMeta' in graph:
                    sources.append(('FileSystem', count))
                elif 'ApacheTika' in graph or 'AT_TechMeta' in graph:
                    sources.append(('Tika', count))
                elif 'ExifTool' in graph or 'ET_TechMeta' in graph:
                    sources.append(('ExifTool', count))
        
        if len(sources) >= 2:  # Almeno 2 fonti di metadati
            result.status = 'PASS'
            sources_desc = ', '.join([f"{name}: {count:,}" for name, count in sources])
            result.description = f"✅ BUONA COPERTURA: Metadati da {len(sources)} fonti diverse\n   📊 Distribuzione: {sources_desc}\n   💡 SIGNIFICATO: I file sono stati processati da multiple fonti di estrazione"
        else:
            result.status = 'WARNING'
            if sources:
                sources_desc = ', '.join([f"{name}: {count:,}" for name, count in sources])
                result.description = f"⚠️ COPERTURA LIMITATA: Solo {len(sources)} fonte di metadati\n   📊 Fonte disponibile: {sources_desc}\n   💡 ATTENZIONE: Mancano metadati da altre fonti (Tika, ExifTool, FileSystem)"
            else:
                result.description = f"❌ NESSUN METADATO: Non sono stati trovati metadati tecnici\n   🔍 SIGNIFICATO: L'estrazione metadati potrebbe essere fallita completamente\n   💡 AZIONE: Verificare che la pipeline di estrazione metadati funzioni"
            
        return result
    
    def _validate_metadata_graph_statistics(self, result: ValidationResult) -> ValidationResult:
        """Valida le statistiche dettagliate dei metadati per graph"""
        if result.status == 'FAIL':
            return result
            
        result.status = 'INFO'
        if result.result_count == 0:
            result.description = "📊 NESSUNA STATISTICA METADATI DISPONIBILE"
        else:
            stats_lines = ["📊 STATISTICHE DETTAGLIATE METADATI PER GRAPH:"]
            for detail in result.details:
                graph_name = detail.get('graph', 'Unknown').split('/')[-1]
                activities = int(detail.get('activities', 0))
                metadata_types = int(detail.get('metadataTypes', 0))
                metadata_instances = int(detail.get('metadataInstances', 0))
                extents = int(detail.get('extents', 0))
                exceptions = int(detail.get('exceptions', 0))
                
                stats_lines.append(f"   🔹 {graph_name}:")
                stats_lines.append(f"      ⚙️ Activities: {activities:,}")
                stats_lines.append(f"      🏷️ Metadata Types: {metadata_types:,}")
                stats_lines.append(f"      📋 Metadata Instances: {metadata_instances:,}")
                stats_lines.append(f"      📏 Extents: {extents:,}")
                if exceptions > 0:
                    stats_lines.append(f"      ⚠️ Exceptions: {exceptions:,}")
            
            result.description = '\n'.join(stats_lines)
            
        return result
    
    def _validate_metadata_duplicates(self, result: ValidationResult) -> ValidationResult:
        """Valida la presenza di metadati duplicati"""
        if result.status == 'FAIL':
            return result
            
        if result.result_count == 0:
            result.status = 'PASS'
            result.description = "✅ NESSUN METADATO DUPLICATO: Ogni field ha valore unico per instantiation"
        else:
            result.status = 'WARNING'
            max_duplicates = max(int(detail['duplicates']) for detail in result.details)
            total_duplicate_sets = result.result_count
            
            result.description = f"⚠️ METADATI DUPLICATI RILEVATI: {total_duplicate_sets} set di duplicati\n"
            result.description += f"   🔝 Max duplicati per field: {max_duplicates}\n"
            result.description += f"   💡 SIGNIFICATO: Stesso field-value ripetuto per una instantiation\n"
            result.description += f"   💡 AZIONE: Verificare logica estrazione per evitare duplicazioni"
            
        return result
    
    def _validate_single_entity_count(self, result: ValidationResult, entity_type: str) -> ValidationResult:
        """Validazione per conteggi singoli di entità"""
        if result.status == 'FAIL':
            return result
        
        if not result.details:
            result.status = 'WARNING'
            result.description = f"⚠️ NESSUN RISULTATO per {entity_type}"
            return result
        
        count = int(result.details[0].get('count', 0))
        
        if count == 0:
            result.status = 'WARNING'
            result.description = f"⚠️ ZERO ENTITÀ: Nessuna entità di tipo {entity_type} trovata\n   💡 SIGNIFICATO: Questo tipo potrebbe non essere presente nel dataset o esserci un errore di tipizzazione"
        elif count > 0:
            result.status = 'PASS'
            # Formatta il numero con separatori delle migliaia
            result.description = f"✅ TROVATE {count:,} entità di tipo {entity_type}"
            
            # Aggiungi contesto specifico per alcuni tipi
            if count > 1000000:
                result.description += f"\n   📊 VOLUME ALTO: Dataset ricco di {entity_type.split(':')[-1]}"
            elif count > 100000:
                result.description += f"\n   📊 VOLUME MEDIO: Buona presenza di {entity_type.split(':')[-1]}"
            elif count < 10 and 'TechnicalMetadata' not in entity_type:
                result.description += f"\n   💡 VOLUME BASSO: Poche entità di questo tipo nel dataset"
        else:
            result.status = 'FAIL'
            result.description = f"❌ CONTEGGIO NON VALIDO per {entity_type}: {count}"
        
        return result
    
    def _validate_graph_overview(self, result: ValidationResult) -> ValidationResult:
        """Valida la panoramica generale dei grafi nel sistema"""
        if result.status == 'FAIL':
            return result
            
        result.status = 'INFO'
        if result.result_count == 0:
            result.description = "📊 NESSUN GRAFO IDENTIFICATO nel sistema"
        else:
            overview_lines = ["🗺️ PANORAMICA COMPLETA GRAFI NEL SISTEMA:"]
            total_triples = 0
            total_subjects = 0
            
            for detail in result.details:
                graph_name = detail.get('graph', 'Unknown')
                subjects = int(detail.get('totalSubjects', 0))
                triples = int(detail.get('totalTriples', 0))
                entity_type = detail.get('primaryEntityType', 'Mixed')
                
                total_triples += triples
                total_subjects += subjects
                
                # Estrai nome grafo leggibile
                display_name = graph_name.split('/')[-1] if '/' in graph_name else graph_name
                entity_desc = entity_type.split('#')[-1] if '#' in entity_type else entity_type
                
                overview_lines.append(f"   📊 {display_name}:")
                overview_lines.append(f"      🔢 Triple: {triples:,}")
                overview_lines.append(f"      👥 Soggetti: {subjects:,}")
                overview_lines.append(f"      🏷️ Tipo primario: {entity_desc}")
            
            overview_lines.append(f"   📈 TOTALI SISTEMA:")
            overview_lines.append(f"      🗄️ Grafi: {result.result_count}")
            overview_lines.append(f"      🔢 Triple totali: {total_triples:,}")
            overview_lines.append(f"      👥 Soggetti totali: {total_subjects:,}")
            overview_lines.append(f"   💡 SIGNIFICATO: Mostra la distribuzione e struttura del dataset RDF")
            
            result.description = '\n'.join(overview_lines)
            
        return result
    
    def _validate_metadata_tool_distribution(self, result: ValidationResult) -> ValidationResult:
        """Valida la distribuzione dei metadata types per tool"""
        if result.status == 'FAIL':
            return result
            
        tool_counts = {}
        for detail in result.details:
            tool = detail.get('tool', 'Unknown')
            usage = int(detail.get('usageCount', 0))
            tool_counts[tool] = tool_counts.get(tool, 0) + usage
        
        result.status = 'INFO'
        distribution_lines = ["📊 DISTRIBUZIONE METADATA TYPES PER TOOL:"]
        for tool, count in sorted(tool_counts.items()):
            distribution_lines.append(f"   🛠️ {tool}: {count:,} utilizzi")
        
        distribution_lines.append(f"   💡 SIGNIFICATO: Mostra quale tool ha estratto più metadati")
        result.description = '\n'.join(distribution_lines)
        
        return result
    
    def _validate_hash_algorithms(self, result: ValidationResult) -> ValidationResult:
        """Valida che si usi solo SHA-256 con spiegazioni"""
        if result.status == 'FAIL':
            return result
            
        sha256_found = False
        other_algorithms = []
        
        for detail in result.details:
            characteristic = detail.get('characteristic', '')
            if 'SHA-256' in characteristic or 'sha256' in characteristic.lower():
                sha256_found = True
            else:
                other_algorithms.append(characteristic)
        
        if sha256_found and not other_algorithms:
            result.status = 'PASS'
            result.description = "✅ ALGORITMO CORRETTO: Solo SHA-256 utilizzato per l'integrità\n   🔒 SIGNIFICATO: Standard crittografico sicuro e uniforme applicato"
        elif sha256_found and other_algorithms:
            result.status = 'WARNING'
            others_str = ', '.join(other_algorithms[:3])
            result.description = f"⚠️ ALGORITMI MISTI: SHA-256 + altri algoritmi\n   📊 Altri trovati: {others_str}\n   💡 RACCOMANDAZIONE: Standardizzare su SHA-256 per coerenza"
        else:
            result.status = 'FAIL'
            if other_algorithms:
                others_str = ', '.join(other_algorithms[:3])
                result.description = f"❌ ALGORITMO SBAGLIATO: SHA-256 non trovato\n   📊 Algoritmi utilizzati: {others_str}\n   🔍 SIGNIFICATO: Gli hash potrebbero non essere compatibili o sicuri\n   💡 AZIONE: Riconfigurare per utilizzare SHA-256"
            else:
                result.description = f"❌ NESSUN ALGORITMO: Non sono stati trovati algoritmi hash\n   💡 AZIONE: Verificare la configurazione del calcolo hash"
            
        return result
    
    def _validate_hash_duplicates(self, result: ValidationResult) -> ValidationResult:
        """Valida la presenza di hash duplicati con spiegazioni"""
        if result.status == 'FAIL':
            return result
            
        if result.result_count == 0:
            result.status = 'PASS'
            result.description = "✅ NESSUN DUPLICATO: Tutti i file hanno hash unici\n   🔍 SIGNIFICATO: Non ci sono file identici nell'archivio"
        else:
            max_duplicates = max(int(detail['duplicate_count']) for detail in result.details)
            total_duplicate_files = sum(int(detail['duplicate_count']) for detail in result.details)
            
            result.status = 'INFO'
            result.description = f"📊 DUPLICATI RILEVATI: {result.result_count} hash condivisi\n   📈 File duplicati totali: {total_duplicate_files:,}\n   🔝 Max copie stesso file: {max_duplicates}\n   💡 SIGNIFICATO: Possibili backup, copie o file identici nell'archivio"
            
        return result
    
    def _validate_single_hash_per_file(self, result: ValidationResult) -> ValidationResult:
        """Valida che ogni file abbia un solo hash"""
        if result.status == 'FAIL':
            return result
            
        if result.result_count == 0:
            result.status = 'PASS'
            result.description = "✅ HASH UNICI: Ogni instantiation ha esattamente un hash"
        else:
            max_hashes = max(int(detail['fixityCount']) for detail in result.details)
            result.status = 'WARNING'
            result.description = f"⚠️ HASH MULTIPLI: {result.result_count} instantiation con più hash\n"
            result.description += f"   🔝 Max hash per file: {max_hashes}\n"
            result.description += f"   💡 SIGNIFICATO: File con multiple rappresentazioni hash (dovrebbe essere 1)\n"
            result.description += f"   💡 AZIONE: Verificare logica calcolo hash per evitare duplicazioni"
            
        return result
    
    def _validate_root_nodes(self, result: ValidationResult) -> ValidationResult:
        """Valida che le root logiche corrispondano alle root degli ID specifici"""
        if result.status == 'FAIL':
            return result
        
        # Raggruppa per rootId
        logical_roots = set()
        id_based_roots = set()
        
        for detail in result.details:
            root_id = detail.get('rootId', '')
            source = detail.get('source', '')
            
            if source == 'logical':
                logical_roots.add(root_id)
            elif source == 'id_based':
                id_based_roots.add(root_id)
        
        # Confronta i due set
        logical_only = logical_roots - id_based_roots
        id_only = id_based_roots - logical_roots
        common = logical_roots & id_based_roots
        
        if logical_roots == id_based_roots:
            result.status = 'PASS'
            result.description = f"✅ VALIDAZIONE PERFETTA: Le root logiche corrispondono agli ID specifici\n"
            result.description += f"   🎯 Root comuni: {len(common)} → {sorted(common)}\n"
            result.description += f"   💡 SIGNIFICATO: La logica di rilevamento root è corretta e coerente"
        else:
            result.status = 'WARNING'
            result.description = f"⚠️ DISCREPANZA ROOT RILEVATA:\n"
            result.description += f"   🔍 Solo nella logica: {sorted(logical_only)}\n"
            result.description += f"   🔍 Solo negli ID: {sorted(id_only)}\n"
            result.description += f"   ✅ Comuni: {sorted(common)}\n"
            result.description += f"   💡 AZIONE: Verificare perché i due metodi danno risultati diversi"
        
        return result
        
    # =============== METODI UTILITY ===============
    
    def _execute_query_batch(self, queries: List[Dict], category: str):
        """Esegue un batch di query con logging garantito per ogni query"""
        
        successful_queries = 0
        failed_queries = 0
        
        # Header categoria migliorato
        self.logger.info(f"\n" + "="*70)
        self.logger.info(f"🔄 CATEGORIA: {category}")
        self.logger.info(f"📋 Query totali: {len(queries)}")
        self.logger.info(f"🕐 Inizio: {datetime.now().strftime('%H:%M:%S')}")
        self.logger.info("="*70)
        
        # Flush per garantire output immediato
        for handler in self.logger.handlers:
            handler.flush()
        sys.stdout.flush()
        
        for i, query_info in enumerate(queries, 1):
            query_name = query_info['name']
            description = query_info['description']
            
            # LOG INIZIO QUERY - SEMPRE VISIBILE
            self.logger.info(f"\n┌─ QUERY {i}/{len(queries)}: {query_name}")
            
            # Estrai descrizione pulita
            if ' - ' in description:
                emoji_title, full_desc = description.split(' - ', 1)
                self.logger.info(f"│  {emoji_title}")
                self.logger.info(f"│  🔍 {full_desc}")
            else:
                self.logger.info(f"│  📋 {description}")
            
            # Flush dopo ogni log importante
            for handler in self.logger.handlers:
                handler.flush()
            sys.stdout.flush()
            
            # Indica esecuzione in corso
            self.logger.info(f"│  ⏳ Esecuzione...")
            
            try:
                # Esegui query
                result = self.engine.execute_query(query_info['query'], query_name)
                
                # Log tempo esecuzione
                self.logger.info(f"│  ⏱️ Completata in {result.execution_time_ms:.1f}ms")
                
                # Applica validazione se presente
                if 'validation' in query_info:
                    try:
                        self.logger.info(f"│  🔍 Validazione...")
                        result = query_info['validation'](result)
                        self.logger.info(f"│  ✅ Validata")
                    except Exception as validation_error:
                        self.logger.error(f"│  ❌ Errore validazione: {validation_error}")
                        result.status = 'FAIL'
                        result.error_message = f"Validation error: {validation_error}"
                
                # Log risultato con icona
                status_icon = {
                    'PASS': '✅', 
                    'FAIL': '❌', 
                    'WARNING': '⚠️', 
                    'INFO': '📊'
                }.get(result.status, '❓')
                
                self.logger.info(f"│  {status_icon} {result.status} - {result.result_count} risultati")
                
                # Log descrizione (gestisce multi-linea)
                if '\n' in result.description:
                    lines = result.description.split('\n')
                    for line in lines:
                        if line.strip():
                            self.logger.info(f"│  {line}")
                else:
                    self.logger.info(f"│  📝 {result.description}")
                
                # Log dettagli per problemi con limite
                if result.status in ['FAIL', 'WARNING'] and result.details:
                    sample_size = min(3, len(result.details))
                    self.logger.info(f"│  🔍 Esempi ({sample_size}/{len(result.details)}):")
                    
                    for j, detail in enumerate(result.details[:sample_size], 1):
                        try:
                            detail_items = []
                            for k, v in detail.items():
                                # Tronca valori lunghi
                                if isinstance(v, str) and len(v) > 40:
                                    if 'http' in v:
                                        v = "..." + v[-25:]
                                    else:
                                        v = v[:25] + "..."
                                detail_items.append(f"{k}={v}")
                            
                            detail_str = ', '.join(detail_items)
                            self.logger.info(f"│     {j}. {detail_str}")
                            
                        except Exception as detail_error:
                            self.logger.info(f"│     {j}. [Errore formato: {detail_error}]")
                    
                    if len(result.details) > sample_size:
                        self.logger.info(f"│     ... +{len(result.details) - sample_size} altri")
                
                # Log errori se presenti
                if result.error_message:
                    self.logger.error(f"│  ❌ Errore: {result.error_message}")
                
                self.results.append(result)
                successful_queries += 1
                
            except Exception as e:
                # Log errore fatale
                self.logger.error(f"│  💥 ERRORE FATALE: {e}")
                
                # Crea risultato di errore
                error_result = ValidationResult(
                    query_name=query_name,
                    description=f"ERRORE FATALE: {description}",
                    status='FAIL',
                    result_count=0,
                    details=[],
                    execution_time_ms=0,
                    error_message=str(e)
                )
                
                self.results.append(error_result)
                failed_queries += 1
            
            finally:
                # Chiudi box query
                self.logger.info(f"└─ Query {i} completata")
                
                # Flush per garantire output immediato
                for handler in self.logger.handlers:
                    handler.flush()
                sys.stdout.flush()
        
        # Riepilogo finale migliorato
        self.logger.info(f"\n" + "="*70)
        self.logger.info(f"📊 RIEPILOGO CATEGORIA: {category}")
        self.logger.info(f"✅ Successi: {successful_queries}/{len(queries)} ({successful_queries/len(queries)*100:.1f}%)")
        
        if failed_queries > 0:
            self.logger.warning(f"❌ Fallimenti: {failed_queries}/{len(queries)}")
        
        # Analisi risultati per status
        recent_results = self.results[-len(queries):]
        status_counts = {}
        for status in ['PASS', 'FAIL', 'WARNING', 'INFO']:
            count = sum(1 for r in recent_results if r.status == status)
            if count > 0:
                status_counts[status] = count
        
        self.logger.info(f"📈 Risultati: {status_counts}")
        self.logger.info(f"🕐 Completata: {datetime.now().strftime('%H:%M:%S')}")
        self.logger.info("="*70)
        
        # Flush finale
        for handler in self.logger.handlers:
            handler.flush()
        sys.stdout.flush()


    # ANCHE: Migliora il setup del logger nel costruttore
    def _setup_default_logger(self):
        """Setup logger migliorato con flush garantito"""
        
        # Configurazione base
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',  # Formato semplificato
            datefmt='%H:%M:%S',
            stream=sys.stdout
        )
        
        logger = logging.getLogger('DatasetValidator')
        
        # Configura handler per flush immediato
        for handler in logger.handlers:
            handler.setLevel(logging.INFO)
            
        return logger

    def _log_execution_summary(self):
        """Log summary finale con tutte le query eseguite"""
        
        self.logger.info(f"\n" + "="*70)
        self.logger.info(f"📋 SUMMARY COMPLETO ESECUZIONE")
        self.logger.info("="*70)
        
        # Raggruppa per categoria
        categories = {}
        for result in self.results:
            # Cerca di determinare categoria dal nome query
            if 'statistics' in result.query_name or 'count' in result.query_name:
                cat = 'Statistiche'
            elif 'hierarchy' in result.query_name or 'orphan' in result.query_name:
                cat = 'Strutturale'
            elif 'metadata' in result.query_name:
                cat = 'Metadati'
            elif 'hash' in result.query_name:
                cat = 'Hash'
            else:
                cat = 'Altro'
            
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(result)
        
        # Log per categoria
        for cat_name, results in categories.items():
            self.logger.info(f"\n🔹 {cat_name}: {len(results)} query")
            
            for result in results:
                status_icon = {
                    'PASS': '✅', 'FAIL': '❌', 
                    'WARNING': '⚠️', 'INFO': '📊'
                }.get(result.status, '❓')
                
                self.logger.info(f"   {status_icon} {result.query_name} ({result.result_count} risultati)")
        
        self.logger.info("="*70)
        
        # Flush finale
        for handler in self.logger.handlers:
            handler.flush()
        sys.stdout.flush()
    
    def _generate_final_report(self, total_time: float, failed_categories: List[Tuple[str, str]] = None) -> Dict[str, Any]:
        """Genera il report finale della validazione con info su categorie fallite"""
        if failed_categories is None:
            failed_categories = []
            
        stats = {
            'PASS': sum(1 for r in self.results if r.status == 'PASS'),
            'FAIL': sum(1 for r in self.results if r.status == 'FAIL'),
            'WARNING': sum(1 for r in self.results if r.status == 'WARNING'),
            'INFO': sum(1 for r in self.results if r.status == 'INFO')
        }
        
        self.logger.info("\n" + "="*70)
        self.logger.info("📋 REPORT FINALE VALIDAZIONE DATASET")
        self.logger.info("="*70)
        self.logger.info(f"🛡️ Modalità fault-tolerant: Esecuzione completata nonostante errori")
        self.logger.info(f"📋 STRUTTURA VALIDAZIONE:")
        self.logger.info(f"   1. 📊 Statistiche Generali - Dimensione e composizione dataset")
        self.logger.info(f"   2. 🏗️ Integrità Strutturale - Records/RecordSets/Instantiation + Orfani")  
        self.logger.info(f"   3. 📋 Validazione Metadati - Tika, ExifTool, FileSystem")
        self.logger.info(f"   4. 🔐 Validazione Hash Completa - Algoritmi, formato, duplicati, integrità")
        self.logger.info(f"   5. 🔬 Consistenza Avanzata - Edge cases e controlli complessi")
        self.logger.info(f"   6. 📊 Export CSV Metadata Types - Report dettagliato metadati")
        self.logger.info(f"Tempo totale: {total_time:.2f} secondi")
        self.logger.info(f"Query eseguite: {len(self.results)}")
        
        if failed_categories:
            self.logger.warning(f"⚠️ Categorie con errori fatali: {len(failed_categories)}")
            for cat_name, error in failed_categories:
                self.logger.warning(f"   • {cat_name}: {error[:100]}...")
        
        self.logger.info(f"📊 RISULTATI VALIDAZIONE:")
        self.logger.info(f"  ✅ PASS (Perfetti): {stats['PASS']}")
        self.logger.info(f"  📊 INFO (Informativi): {stats['INFO']}")  
        self.logger.info(f"  ⚠️ WARNING (Da verificare): {stats['WARNING']}")
        self.logger.info(f"  ❌ FAIL (Problemi critici): {stats['FAIL']}")
        
        # Classificazione finale con considerazione errori di categoria
        overall_status = 'EXCELLENT'
        if failed_categories:
            overall_status = 'PARTIAL_EXECUTION'
        elif stats['FAIL'] > 0:
            overall_status = 'CRITICAL_ISSUES'
        elif stats['WARNING'] > 5:
            overall_status = 'NEEDS_ATTENTION'
        elif stats['WARNING'] > 0:
            overall_status = 'GOOD_WITH_WARNINGS'
        
        status_messages = {
            'EXCELLENT': '🎉 DATASET ECCELLENTE - Tutte le validazioni superate perfettamente',
            'GOOD_WITH_WARNINGS': '✅ DATASET BUONO - Qualità alta con alcuni aspetti da verificare',
            'NEEDS_ATTENTION': '⚠️ DATASET RICHIEDE ATTENZIONE - Diversi aspetti da esaminare',
            'CRITICAL_ISSUES': '❌ DATASET CON PROBLEMI CRITICI - Correzioni necessarie prima dell\'uso',
            'PARTIAL_EXECUTION': '🛡️ VALIDAZIONE PARZIALE - Completata nonostante errori di esecuzione'
        }
        
        self.logger.info(f"\n{status_messages[overall_status]}")
        
        # Problemi critici da evidenziare
        critical_failures = [r for r in self.results if r.status == 'FAIL']
        if critical_failures:
            self.logger.info(f"\n🚨 PROBLEMI CRITICI RILEVATI:")
            for failure in critical_failures:
                # Estrai solo la prima linea della descrizione per il summary
                description_line = failure.description.split('\n')[0]
                self.logger.info(f"  • {failure.query_name}: {description_line}")
                if failure.error_message:
                    self.logger.info(f"    Errore: {failure.error_message[:100]}...")
        
        # Evidenzia anche i warning più importanti
        important_warnings = [r for r in self.results if r.status == 'WARNING' and r.result_count > 0]
        if important_warnings:
            self.logger.info(f"\n⚠️ PRINCIPALI WARNING DA VERIFICARE:")
            for warning in important_warnings[:5]:  # Top 5 warnings
                description_line = warning.description.split('\n')[0]
                self.logger.info(f"  • {warning.query_name}: {description_line}")
        
        # Statistiche 
        execution_errors = len([r for r in self.results if r.error_message])
        total_attempted = len(self.results)
        success_rate = ((total_attempted - execution_errors) / total_attempted * 100) if total_attempted > 0 else 0
        
        self.logger.info(f"\n🛡️ STATISTICHE SUCCESSI:")
        self.logger.info(f"   Tasso successo esecuzione: {success_rate:.1f}%")
        self.logger.info(f"   Query con errori: {execution_errors}/{total_attempted}")
        self.logger.info(f"   Categorie fallite: {len(failed_categories)}")
        
        # Statistiche dettagliate per tipo di controllo
        structural_issues = len([r for r in self.results if 'structural' in r.query_name.lower() and r.status == 'FAIL'])
        metadata_issues = len([r for r in self.results if 'metadata' in r.query_name.lower() and r.status in ['FAIL', 'WARNING']])
        hash_issues = len([r for r in self.results if 'hash' in r.query_name.lower() and r.status == 'FAIL'])
        
        self.logger.info(f"\n📊 BREAKDOWN PROBLEMI PER AREA:")
        self.logger.info(f"   🏗️ Struttura archivistica: {structural_issues} problemi critici")
        self.logger.info(f"   📋 Metadati tecnici: {metadata_issues} problemi totali")
        self.logger.info(f"   🔐 Hash e integrità: {hash_issues} problemi critici")
        
        self.logger.info("="*70)
        
        # Salva report dettagliato
        self._save_detailed_report(stats, overall_status, total_time, failed_categories)
        
        return {
            'status': overall_status,
            'stats': stats,
            'total_time': total_time,
            'total_queries': len(self.results),
            'critical_issues': len(critical_failures),
            'failed_categories': len(failed_categories),
            'execution_success_rate': success_rate,
            'structural_issues': structural_issues,
            'metadata_issues': metadata_issues,
            'hash_issues': hash_issues
        }
    
    def _save_detailed_report(self, stats: Dict, overall_status: str, total_time: float, failed_categories: List[Tuple[str, str]] = None):
        """Salva report dettagliato in formato JSON con info su categorie fallite"""
        if failed_categories is None:
            failed_categories = []
            
        report = {
            'validation_info': {
                'timestamp': datetime.now().isoformat(),
                'endpoint': self.engine.endpoint,
                'total_time_seconds': total_time,
                'overall_status': overall_status,
                'fault_tolerant_mode': True,
                'failed_categories_count': len(failed_categories),
                'version': '2.0_enhanced'
            },
            'execution_summary': {
                'total_queries_attempted': len(self.results),
                'queries_with_execution_errors': len([r for r in self.results if r.error_message]),
                'categories_failed': len(failed_categories),
                'execution_success_rate_percent': ((len(self.results) - len([r for r in self.results if r.error_message])) / len(self.results) * 100) if len(self.results) > 0 else 0
            },
            'summary': stats,
            'failed_categories': [
                {'category_name': cat_name, 'error_message': error_msg}
                for cat_name, error_msg in failed_categories
            ],
            'critical_issues': [
                {
                    'query_name': r.query_name,
                    'description': r.description.split('\n')[0],  # Prima linea
                    'result_count': r.result_count,
                    'sample_issues': r.details[:3] if r.details else []
                }
                for r in self.results if r.status == 'FAIL'
            ],
            'detailed_results': []
        }
        
        for result in self.results:
            report['detailed_results'].append({
                'query_name': result.query_name,
                'description': result.description,
                'status': result.status,
                'result_count': result.result_count,
                'execution_time_ms': result.execution_time_ms,
                'error_message': result.error_message,
                'sample_results': result.details[:5] if result.details else []
            })
        
        report_file = f"dataset_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            self.logger.info(f"📄 Report dettagliato salvato: {report_file}")
        except Exception as e:
            self.logger.error(f"Errore nel salvare il report: {e}")


def parse_arguments():
    """Parser degli argomenti da riga di comando semplificato (throttling integrato)"""
    parser = argparse.ArgumentParser(
        description="Sistema di Validazione SPARQL per Dataset Archivio Evangelisti",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi di utilizzo:
  # Validazione completa (throttling automatico)
  python dataset_validator.py
  
  # Validazione di base
  python dataset_validator.py --level basic
  
  # Endpoint personalizzato
  python dataset_validator.py --endpoint http://localhost:10214/blazegraph/namespace/kb/sparql
  
  # Solo test connessione
  python dataset_validator.py --test-only

NOTA: Il throttling è ora integrato automaticamente nel codice:
- 3 secondi di pausa tra query consecutive
- 8 secondi di pausa tra categorie di validazione
Ottimizzato per dataset pesanti (16GB+) senza configurazione manuale.
"""
    )
    
    parser.add_argument(
        '--endpoint',
        default='http://localhost:10214/blazegraph/namespace/kb/sparql',
        help='Endpoint SPARQL di ReserachSpace (Blazegraph)'
    )
    
    parser.add_argument(
        '--level',
        choices=['basic', 'full'],
        default='full',
        help='Livello di validazione (default: full)'
    )
    
    parser.add_argument(
        '--test-only',
        action='store_true',
        help='Esegue solo il test di connessione'
    )

    parser.add_argument(
        '--csv-file',
        help='Nome file CSV personalizzato per metadata types (default: auto-generato con timestamp)'
    )

    parser.add_argument(
    '--mime-csv-file',
    help='Nome file CSV personalizzato per MIME types distribution (default: auto-generato con timestamp)'
    )

    parser.add_argument(
        '--skip-csv',
        action='store_true',
        help='Salta il download CSV metadata types'
    )

    parser.add_argument(
        '--skip-metadata-csv',
        action='store_true',
        help='Salta il download CSV metadata types (mantiene MIME types CSV)'
    )
    
    parser.add_argument(
        '--skip-mime-csv',
        action='store_true',
        help='Salta il download CSV MIME types (mantiene metadata types CSV)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Output verboso con dettagli aggiuntivi'
    )
    
    return parser.parse_args()


def main():
    """Funzione principale con throttling automatico integrato"""
    args = parse_arguments()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='[%(levelname)s] %(asctime)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    print("="*70)
    print("🔍 SISTEMA DI VALIDAZIONE SPARQL - ARCHIVIO EVANGELISTI")
    print("🔧 VERSIONE 2.1 - Con CSV MIME Types Distribution")
    print("="*70)
    print(f"📡 Endpoint Blazegraph: {args.endpoint}")
    print(f"📊 Livello validazione: {args.level.upper()}")
    
    # Mostra configurazione CSV
    csv_config = []
    if not args.skip_csv:
        if not args.skip_metadata_csv:
            csv_config.append("Metadata Types")
        if not args.skip_mime_csv:
            csv_config.append("MIME Types Distribution")
    
    if csv_config:
        print(f"📄 CSV da generare: {', '.join(csv_config)}")
        if args.csv_file:
            print(f"   📝 Metadata types file: {args.csv_file}")
        if args.mime_csv_file:
            print(f"   🎭 MIME types file: {args.mime_csv_file}")
    else:
        print(f"📄 CSV: Disabilitati (--skip-csv)")



    print(f"⏰ Throttling automatico: 3s tra query, 8s tra categorie")
    print(f"🐘 Ottimizzato per dataset pesanti (16GB+)")
    print(f"🛡️ Modalità: Fault-tolerant (continua anche se singole query falliscono)")
    print(f"📋 Categorie di validazione:")
    if args.level == 'basic':
        print(f"   • Statistiche Generali, Integrità Strutturale")
    else:
        print(f"   • Tutte e 7 le categorie (dalla struttura di base ai controlli avanzati)")
    print(f"🔍 Controlli aggiuntivi: duplicati URI, metadati orfani, hash format, profondità")
    print(f"🔐 Sezione Hash Dedicata: algoritmi, formato, duplicati, integrità completa")
    print("="*70)
    
    try:
        # Crea engine SPARQL con throttling integrato
        engine = SPARQLValidationEngine(args.endpoint)
        
        # Test connessione
        if not engine.test_connection():
            print("❌ Impossibile connettersi a Blazegraph")
            sys.exit(1)
        
        if args.test_only:
            print("✅ TEST CONNESSIONE COMPLETATO CON SUCCESSO")
            return
        
        # Crea validatore con configurazione CSV
        validator = DatasetValidator(engine)
        
        # Configura opzioni CSV nel validator
        if args.skip_csv or args.skip_metadata_csv:
            validator.skip_metadata_csv = True
        if args.skip_csv or args.skip_mime_csv:
            validator.skip_mime_csv = True
        
        # Imposta nomi file personalizzati se specificati
        if args.csv_file:
            validator.custom_metadata_csv_filename = args.csv_file
        if args.mime_csv_file:
            validator.custom_mime_csv_filename = args.mime_csv_file
        
        # Esegui validazione
        result = validator.run_validation_suite(args.level)
        
        # ... resto della gestione risultati esistente ...
        
    except KeyboardInterrupt:
        print("\n⚠️ Validazione interrotta dall'utente")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Errore fatale: {e}")
        sys.exit(1)
   
if __name__ == "__main__":
    main()