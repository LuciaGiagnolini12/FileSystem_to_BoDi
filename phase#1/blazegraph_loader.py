#!/usr/bin/env python3
"""
Blazegraph REST API Loader - Sostituto per DataLoader
Carica file .nq in Blazegraph server tramite REST API invece di accesso diretto al journal
VERSIONE CON SUPPORTO FILE GRANDI
"""

import requests
import time
import os
import math
from pathlib import Path
from typing import List, Dict, Tuple
import logging

# === AGGIUNTA: CLASSE PER GESTIRE FILE GRANDI ===
class LargeFileHandler:
    """Gestisce il caricamento di file N-Quads molto grandi"""
    
    def __init__(self, logger, max_chunk_size_mb: int = 500):
        self.logger = logger
        self.max_chunk_size = max_chunk_size_mb * 1024 * 1024  # Converti in bytes
        
    def should_split_file(self, file_path: Path) -> bool:
        """Determina se un file deve essere diviso"""
        if not file_path.exists():
            return False
        
        file_size = file_path.stat().st_size
        return file_size > self.max_chunk_size
    
    def split_nquads_file(self, file_path: Path) -> List[Path]:
        """Divide un file N-Quads in chunk più piccoli"""
        if not self.should_split_file(file_path):
            return [file_path]
        
        file_size = file_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        max_size_mb = self.max_chunk_size / (1024 * 1024)
        
        self.logger.info(f"📂 File troppo grande: {file_path.name} ({file_size_mb:.1f}MB)")
        self.logger.info(f"🔨 Divisione in chunk da max {max_size_mb:.0f}MB...")
        
        chunk_files = []
        chunk_size = 0
        current_chunk = 1
        current_chunk_path = None
        current_chunk_file = None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as source_file:
                for line_num, line in enumerate(source_file, 1):
                    # Inizia nuovo chunk se necessario
                    if current_chunk_file is None or chunk_size >= self.max_chunk_size:
                        # Chiudi chunk precedente
                        if current_chunk_file:
                            current_chunk_file.close()
                            self.logger.info(f"  ✅ Chunk {current_chunk-1}: {current_chunk_path.name} ({chunk_size/1024/1024:.1f}MB)")
                        
                        # Apri nuovo chunk
                        current_chunk_path = file_path.parent / f"{file_path.stem}_chunk_{current_chunk:03d}.nq"
                        current_chunk_file = open(current_chunk_path, 'w', encoding='utf-8')
                        chunk_files.append(current_chunk_path)
                        chunk_size = 0
                        current_chunk += 1
                    
                    # Scrivi linea nel chunk corrente
                    current_chunk_file.write(line)
                    chunk_size += len(line.encode('utf-8'))
                    
                    # Progress report
                    if line_num % 100000 == 0:
                        progress_mb = chunk_size / 1024 / 1024
                        self.logger.debug(f"    Chunk {current_chunk-1}: {progress_mb:.1f}MB processati...")
                
                # Chiudi ultimo chunk
                if current_chunk_file:
                    current_chunk_file.close()
                    self.logger.info(f"  ✅ Chunk {current_chunk-1}: {current_chunk_path.name} ({chunk_size/1024/1024:.1f}MB)")
            
            self.logger.info(f"🎉 File diviso in {len(chunk_files)} chunk")
            return chunk_files
            
        except Exception as e:
            self.logger.error(f"❌ Errore divisione file: {e}")
            # Cleanup chunk files parziali
            for chunk_file in chunk_files:
                if chunk_file.exists():
                    chunk_file.unlink()
            return [file_path]  # Fallback al file originale
    
    def cleanup_chunk_files(self, chunk_files: List[Path], original_file: Path):
        """Pulisce i file chunk dopo il caricamento"""
        for chunk_file in chunk_files:
            if chunk_file != original_file and chunk_file.exists():
                try:
                    chunk_file.unlink()
                    self.logger.debug(f"🗑️ Rimosso chunk: {chunk_file.name}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Impossibile rimuovere chunk {chunk_file.name}: {e}")

class BlazegraphRESTLoader:
    """Caricatore che usa REST API di Blazegraph invece di DataLoader diretto"""
    
    def __init__(self, base_url: str = "http://localhost:9999/blazegraph", namespace: str = "kb"):
        self.base_url = base_url.rstrip('/')
        self.namespace = namespace
        self.namespace_url = f"{self.base_url}/namespace/{self.namespace}"
        self.sparql_update_url = f"{self.namespace_url}/sparql"
        self.data_upload_url = f"{self.namespace_url}"
        
        # Setup logging
        self.logger = logging.getLogger('BlazegraphRESTLoader')
    
    def test_connection(self) -> bool:
        """Testa la connessione al server Blazegraph"""
        try:
            # Test query COUNT
            response = requests.post(
                self.sparql_update_url,
                data={'query': 'SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }'},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                count = int(result["results"]["bindings"][0]["count"]["value"])
                self.logger.info(f"✅ Connessione OK - Triple esistenti: {count:,}")
                return True
            else:
                self.logger.error(f"❌ Server risponde ma con errore: HTTP {response.status_code}")
                return False
                
        except requests.exceptions.ConnectionError:
            self.logger.error(f"❌ Server Blazegraph non raggiungibile su {self.base_url}")
            self.logger.error("   Assicurati che sia in esecuzione: java -jar blazegraph.jar")
            return False
        except Exception as e:
            self.logger.error(f"❌ Errore test connessione: {e}")
            return False
    
    def check_namespace(self) -> bool:
        """Verifica che il namespace esista"""
        try:
            # Lista namespace disponibili
            response = requests.get(f"{self.base_url}/namespace", timeout=10)
            
            if response.status_code == 200:
                # Blazegraph restituisce XML con lista namespace
                if self.namespace in response.text:
                    self.logger.info(f"✅ Namespace '{self.namespace}' trovato")
                    return True
                else:
                    self.logger.warning(f"⚠️ Namespace '{self.namespace}' non trovato")
                    return self.create_namespace()
            else:
                self.logger.warning(f"⚠️ Impossibile verificare namespace: HTTP {response.status_code}")
                return True  # Procedi comunque
                
        except Exception as e:
            self.logger.warning(f"⚠️ Errore verifica namespace: {e}")
            return True  # Procedi comunque
    
    def create_namespace(self) -> bool:
        """Crea il namespace se non esiste"""
        try:
            self.logger.info(f"🔧 Creazione namespace '{self.namespace}'...")
            
            namespace_config = f"""
com.bigdata.rdf.store.AbstractTripleStore.quads=true
com.bigdata.rdf.store.AbstractTripleStore.statementIdentifiers=false
com.bigdata.rdf.store.AbstractTripleStore.textIndex=true
com.bigdata.rdf.store.AbstractTripleStore.axiomsClass=com.bigdata.rdf.axioms.NoAxioms
com.bigdata.namespace.{self.namespace}.lex.com.bigdata.btree.BTree.branchingFactor=400
com.bigdata.namespace.{self.namespace}.spo.com.bigdata.btree.BTree.branchingFactor=1024
"""
            
            response = requests.post(
                f"{self.base_url}/namespace",
                data=namespace_config,
                headers={'Content-Type': 'text/plain'},
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                self.logger.info(f"✅ Namespace '{self.namespace}' creato")
                return True
            else:
                self.logger.error(f"❌ Creazione namespace fallita: HTTP {response.status_code}")
                self.logger.error(f"   Response: {response.text[:200]}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Errore creazione namespace: {e}")
            return False
    
    def load_nquads_file(self, file_path: Path) -> bool:
        """Carica un file .nq tramite REST API - VERSIONE CORRETTA"""
        if not file_path.exists():
            self.logger.error(f"❌ File non trovato: {file_path}")
            return False
        
        file_size = file_path.stat().st_size
        self.logger.info(f"🔄 Caricamento {file_path.name} ({file_size:,} bytes)...")
        
        start_time = time.time()
        
        try:
            # Leggi file in chunks per file grandi
            with open(file_path, 'rb') as f:
                file_content = f.read()
            
            # ✅ HEADERS CORRETTI PER BLAZEGRAPH
            response = requests.post(
                self.data_upload_url,
                data=file_content,
                headers={
                    'Content-Type': 'application/n-quads; charset=utf-8'  # ✅ CORRETTO
                },
                timeout=3600  # 1 ora timeout per file grandi
            )
            
            duration = time.time() - start_time
            
            if response.status_code in [200, 201]:
                self.logger.info(f"✅ {file_path.name} caricato con successo ({duration:.2f}s)")
                
                # Verifica conteggio triple dopo caricamento
                self._log_triple_count_after_load()
                return True
                
            else:
                self.logger.error(f"❌ Caricamento fallito: HTTP {response.status_code}")
                self.logger.error(f"   Response: {response.text[:500]}")
                return False
                
        except requests.exceptions.Timeout:
            duration = time.time() - start_time
            self.logger.error(f"⏰ Timeout caricamento {file_path.name} dopo {duration:.0f}s")
            return False
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"❌ Errore caricamento {file_path.name}: {e}")
            self.logger.error(f"   Durata prima errore: {duration:.2f}s")
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
                self.logger.info(f"📊 Triple totali nel DB: {count:,}")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Impossibile verificare conteggio triple: {e}")
    
    def load_multiple_files(self, file_paths: List[Path]) -> Tuple[int, int]:
        """Carica multipli file .nq"""
        if not file_paths:
            self.logger.warning("⚠️ Nessun file da caricare")
            return 0, 0
        
        self.logger.info(f"🚀 Inizio caricamento di {len(file_paths)} file via REST API")
        
        successful = 0
        failed = 0
        
        for i, file_path in enumerate(file_paths, 1):
            self.logger.info(f"\n[{i}/{len(file_paths)}] Processando: {file_path.name}")
            
            if self.load_nquads_file(file_path):
                successful += 1
            else:
                failed += 1
                
        self.logger.info(f"\n📊 RISULTATI CARICAMENTO:")
        self.logger.info(f"   ✅ Successi: {successful}")
        self.logger.info(f"   ❌ Fallimenti: {failed}")
        self.logger.info(f"   📊 Totale: {len(file_paths)}")
        
        return successful, len(file_paths)
    
    def clear_namespace(self) -> bool:
        """Pulisce tutti i dati dal namespace (opzionale)"""
        try:
            self.logger.info(f"🧹 Pulizia namespace '{self.namespace}'...")
            
            # SPARQL UPDATE per cancellare tutto
            clear_query = "CLEAR ALL"
            
            response = requests.post(
                self.sparql_update_url,
                data={'update': clear_query},
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=60
            )
            
            if response.status_code == 200:
                self.logger.info("✅ Namespace pulito")
                return True
            else:
                self.logger.error(f"❌ Pulizia fallita: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Errore pulizia namespace: {e}")
            return False

# === AGGIUNTA: VERSIONE CON CHUNKING ===
class BlazegraphRESTLoaderWithChunking(BlazegraphRESTLoader):
    """Versione estesa con supporto per file grandi"""
    
    def __init__(self, base_url: str = "http://localhost:9999/blazegraph", namespace: str = "kb", logger=None):
        super().__init__(base_url, namespace)
        if logger:
            self.logger = logger
        self.large_file_handler = LargeFileHandler(self.logger)
    
    def load_nquads_file_smart(self, file_path: Path) -> bool:
        """Carica file N-Quads con gestione automatica file grandi"""
        if not file_path.exists():
            if self.logger:
                self.logger.error(f"❌ File non trovato: {file_path}")
            return False
        
        file_size = file_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        # Se il file è troppo grande, dividilo
        if self.large_file_handler.should_split_file(file_path):
            if self.logger:
                self.logger.warning(f"⚠️ File molto grande ({file_size_mb:.1f}MB), divisione automatica...")
            
            chunk_files = self.large_file_handler.split_nquads_file(file_path)
            
            if len(chunk_files) == 1:
                # Divisione fallita, prova caricamento normale
                return self.load_nquads_file(file_path)
            
            # Carica ogni chunk
            successful_chunks = 0
            total_chunks = len(chunk_files)
            
            for i, chunk_file in enumerate(chunk_files, 1):
                if self.logger:
                    self.logger.info(f"📥 Caricamento chunk {i}/{total_chunks}: {chunk_file.name}")
                
                if self.load_nquads_file(chunk_file):
                    successful_chunks += 1
                else:
                    if self.logger:
                        self.logger.error(f"❌ Caricamento chunk {i} fallito")
            
            # Cleanup chunk files
            self.large_file_handler.cleanup_chunk_files(chunk_files, file_path)
            
            success = successful_chunks == total_chunks
            if self.logger:
                if success:
                    self.logger.info(f"✅ File grande caricato con successo ({total_chunks} chunk)")
                else:
                    self.logger.error(f"❌ Caricamento parziale: {successful_chunks}/{total_chunks} chunk")
            
            return success
        else:
            # File di dimensioni normali
            return self.load_nquads_file(file_path)
    
    def load_multiple_files_smart(self, file_paths: List[Path]) -> Tuple[int, int]:
        """Carica multipli file con gestione automatica file grandi"""
        if not file_paths:
            if self.logger:
                self.logger.warning("⚠️ Nessun file da caricare")
            return 0, 0
        
        if self.logger:
            self.logger.info(f"🚀 Inizio caricamento di {len(file_paths)} file (con gestione file grandi)")
        
        successful = 0
        failed = 0
        
        for i, file_path in enumerate(file_paths, 1):
            if self.logger:
                self.logger.info(f"\n[{i}/{len(file_paths)}] Processando: {file_path.name}")
            
            if self.load_nquads_file_smart(file_path):
                successful += 1
            else:
                failed += 1
        
        if self.logger:
            self.logger.info(f"\n📊 RISULTATI CARICAMENTO SMART:")
            self.logger.info(f"   ✅ Successi: {successful}")
            self.logger.info(f"   ❌ Fallimenti: {failed}")
            self.logger.info(f"   📊 Totale: {len(file_paths)}")
        
        return successful, len(file_paths)

class BlazegraphJournalLoaderREST:
    """Wrapper per sostituire il BlazegraphJournalLoader originale"""
    
    def __init__(self, logger, working_dir: Path):
        self.logger = logger
        self.working_dir = working_dir
        self.blazegraph_dir = working_dir / "blazegraph_journal"
        
        # Configurazione per REST API
        self.rest_loader = BlazegraphRESTLoader()
        
        # Setup logging del REST loader per usare il logger della pipeline
        self.rest_loader.logger = logger
    
    def generate_blazegraph_journal(self, nq_files: List[Path]) -> bool:
        """Metodo principale che sostituisce il caricamento DataLoader con REST API"""
        self.logger.info(f"🚀 Avvio caricamento Blazegraph via REST API per {len(nq_files)} file")
        
        start_time = time.time()
        
        try:
            # 1. Test connessione server
            if not self.rest_loader.test_connection():
                self.logger.error("❌ Server Blazegraph non raggiungibile")
                self.logger.error("   Avvia il server: cd blazegraph_journal && java -jar blazegraph.jar")
                return False
            
            # 2. Verifica/crea namespace
            if not self.rest_loader.check_namespace():
                self.logger.error("❌ Problemi con namespace")
                return False
            
            # 3. Verifica file .nq esistenti
            existing_files = []
            for nq_file in nq_files:
                if nq_file.exists():
                    existing_files.append(nq_file)
                    self.logger.info(f"✅ File trovato: {nq_file.name} ({nq_file.stat().st_size:,} bytes)")
                else:
                    self.logger.warning(f"⚠️ File non trovato: {nq_file}")
            
            if not existing_files:
                self.logger.error("❌ Nessun file .nq valido da caricare")
                return False
            
            # 4. Caricamento via REST API
            successful, total = self.rest_loader.load_multiple_files(existing_files)
            
            # 5. Report finale
            duration = time.time() - start_time
            success_rate = (successful / total * 100) if total > 0 else 0
            
            self.logger.info(f"\n{'='*70}")
            self.logger.info(f"📊 REPORT FINALE - Caricamento Blazegraph via REST API")
            self.logger.info(f"{'='*70}")
            self.logger.info(f"File processati: {successful}/{total}")
            self.logger.info(f"Tasso successo: {success_rate:.1f}%")
            self.logger.info(f"Tempo totale: {duration:.2f} secondi")
            
            if successful == total:
                self.logger.info("🎉 TUTTI I FILE CARICATI CON SUCCESSO VIA REST API!")
                return True
            else:
                self.logger.error(f"❌ CARICAMENTO PARZIALE: {successful}/{total} file")
                return False
                
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"💥 Errore fatale caricamento REST API: {e}")
            self.logger.error(f"   Durata prima errore: {duration:.2f}s")
            return False

# === AGGIUNTA: VERSIONE CON CHUNKING ===
class BlazegraphJournalGeneratorRESTWithChunking:
    """Versione estesa con supporto file grandi"""
    
    def __init__(self, logger, working_dir: Path):
        self.logger = logger
        self.working_dir = working_dir
        self.blazegraph_dir = working_dir / "blazegraph_journal"
        
        # Configurazione REST API
        self.base_url = "http://localhost:9999/blazegraph"
        self.namespace = "kb"
        
        # ✅ USA IL LOADER CON CHUNKING
        self.rest_loader = BlazegraphRESTLoaderWithChunking(self.base_url, self.namespace, logger)
    
    def _verify_server_running(self) -> bool:
        """Verifica che il server Blazegraph sia in esecuzione"""
        return self.rest_loader.test_connection()
    
    def _check_existing_data(self) -> int:
        """Controlla se ci sono già dati nel server"""
        try:
            import requests
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
        """Metodo principale per caricare dati via REST API con chunking"""
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
            
            # 4. ✅ CARICAMENTO CON CHUNKING AUTOMATICO
            self.logger.info(f"🔄 Caricamento {len(available_files)} file via REST API CON CHUNKING...")
            successful, total = self.rest_loader.load_multiple_files_smart(available_files)
            
            # 5. Report finale
            total_duration = time.time() - start_time
            success_rate = (successful / total * 100) if total > 0 else 0
            
            self.logger.info(f"\n{'='*70}")
            self.logger.info(f"📊 REPORT FINALE - Caricamento Blazegraph REST API CON CHUNKING")
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

def main():
    """Test standalone del loader REST"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Blazegraph REST Loader")
    parser.add_argument('files', nargs='*', help='File .nq da caricare')
    parser.add_argument('--url', default='http://localhost:9999/blazegraph', 
                       help='URL base Blazegraph')
    parser.add_argument('--namespace', default='kb', 
                       help='Namespace da usare')
    parser.add_argument('--clear', action='store_true',
                       help='Pulisci namespace prima del caricamento')
    parser.add_argument('--chunking', action='store_true',
                       help='Usa loader con chunking per file grandi')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    
    # Crea loader (normale o con chunking)
    if args.chunking:
        loader = BlazegraphRESTLoaderWithChunking(args.url, args.namespace)
        print("🔧 Loader con chunking attivato")
    else:
        loader = BlazegraphRESTLoader(args.url, args.namespace)
    
    # Test connessione
    if not loader.test_connection():
        print("❌ Test fallito")
        return 1
    
    # Pulisci se richiesto
    if args.clear:
        loader.clear_namespace()
    
    # Carica file se specificati
    if args.files:
        file_paths = [Path(f) for f in args.files]
        
        if args.chunking:
            successful, total = loader.load_multiple_files_smart(file_paths)
        else:
            successful, total = loader.load_multiple_files(file_paths)
        
        if successful == total:
            print("✅ Test completato con successo")
            return 0
        else:
            print("❌ Test parzialmente fallito")
            return 1
    else:
        print("✅ Test connessione OK - nessun file da caricare")
        return 0

if __name__ == "__main__":
    exit(main())