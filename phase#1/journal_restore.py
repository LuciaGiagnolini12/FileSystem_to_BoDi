import os
import time
import requests
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple

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
        
    def test_server_connection(self) -> bool:
        """Verifica connessione server"""
        try:
            response = requests.get(f"{self.base_url}/status", timeout=5)
            return response.status_code == 200
        except:
            try:
                # Fallback: prova con query semplice
                response = requests.post(
                    self.sparql_endpoint,
                    data={'query': 'SELECT * WHERE { ?s ?p ?o } LIMIT 1'},
                    timeout=5
                )
                return response.status_code == 200
            except:
                return False
    
    def get_current_triple_count(self) -> int:
        """Conta triple attuali nel journal"""
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
    
    def clear_current_journal(self) -> bool:
        """Svuota il journal corrente via SPARQL"""
        print("🧹 Svuotamento journal corrente...")
        
        try:
            response = requests.post(
                self.sparql_endpoint,
                data={'update': 'CLEAR ALL'},
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=120
            )
            
            if response.status_code == 200:
                # Verifica che sia vuoto
                remaining = self.get_current_triple_count()
                if remaining == 0:
                    print("✅ Journal svuotato con successo")
                    return True
                else:
                    print(f"⚠️ Journal non completamente vuoto: {remaining:,} triple rimaste")
                    return False
            else:
                print(f"❌ Svuotamento fallito: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Errore durante svuotamento: {e}")
            return False
    
    def load_backup_to_journal(self, backup_file: Path) -> bool:
        """Carica backup nel journal (sovrascrittivo)"""
        if not backup_file.exists():
            print(f"❌ File backup non trovato: {backup_file}")
            return False
        
        file_size = backup_file.stat().st_size
        print(f"📥 Caricamento backup nel journal...")
        print(f"   📄 File: {backup_file.name}")
        print(f"   📊 Dimensione: {file_size:,} bytes")
        
        start_time = time.time()
        
        try:
            with open(backup_file, 'rb') as f:
                file_content = f.read()
            
            response = requests.post(
                self.data_endpoint,
                data=file_content,
                headers={'Content-Type': 'application/n-quads'},
                timeout=1800  # 30 minuti per file grandi
            )
            
            duration = time.time() - start_time
            
            if response.status_code in [200, 201]:
                loaded_count = self.get_current_triple_count()
                print(f"✅ Backup caricato con successo!")
                print(f"   📊 Triple caricate: {loaded_count:,}")
                print(f"   ⏱️ Durata: {duration:.2f}s")
                return True
            else:
                print(f"❌ Caricamento fallito: HTTP {response.status_code}")
                print(f"   Response: {response.text[:200]}")
                return False
                
        except Exception as e:
            duration = time.time() - start_time
            print(f"❌ Errore durante caricamento: {e}")
            print(f"   Durata prima errore: {duration:.2f}s")
            return False
    
    def restore_journal_from_backup(self, backup_file: Optional[Path] = None, interactive: bool = True) -> bool:
        """
        Procedura completa: ripristina journal da backup
        
        Args:
            backup_file: File backup specifico. Se None, usa il più recente o chiede all'utente
            interactive: Se True, chiede conferma all'utente
        """
        print("🔄 RIPRISTINO JOURNAL BLAZEGRAPH DA BACKUP")
        print("=" * 60)
        
        # 1. Verifica server attivo
        if not self.test_server_connection():
            print("❌ Server Blazegraph non raggiungibile")
            print("   Avvia il server: cd blazegraph_journal && java -jar blazegraph.jar")
            return False
        
        print("✅ Server Blazegraph connesso")
        
        # 2. Stato attuale
        current_count = self.get_current_triple_count()
        print(f"📊 Triple attuali nel journal: {current_count:,}")
        
        # 3. Lista backup disponibili
        available_backups = self.list_available_backups()
        
        if not available_backups:
            print("❌ Nessun backup trovato nella directory backups/")
            return False
        
        print(f"\n📋 Backup disponibili ({len(available_backups)}):")
        for i, (file_path, backup_type, date_part, time_part, size, modified) in enumerate(available_backups, 1):
            print(f"   {i}. {file_path.name}")
            print(f"      Tipo: {backup_type}")
            print(f"      Data: {modified.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"      Dimensione: {size:,} bytes")
            print()
        
        # 4. Selezione backup
        if backup_file is None:
            if interactive:
                try:
                    choice = input(f"Seleziona backup (1-{len(available_backups)}, ENTER per il più recente): ").strip()
                    if choice == "":
                        selected_backup = available_backups[0][0]  # Più recente
                    else:
                        index = int(choice) - 1
                        if 0 <= index < len(available_backups):
                            selected_backup = available_backups[index][0]
                        else:
                            print("❌ Selezione non valida")
                            return False
                except (ValueError, KeyboardInterrupt):
                    print("❌ Operazione annullata")
                    return False
            else:
                selected_backup = available_backups[0][0]  # Automatico: più recente
        else:
            selected_backup = backup_file
        
        print(f"🎯 Backup selezionato: {selected_backup.name}")
        
        # 5. Conferma operazione
        if interactive and current_count > 0:
            print(f"\n⚠️ ATTENZIONE:")
            print(f"   Questa operazione SOSTITUIRÀ le {current_count:,} triple attuali")
            print(f"   con i dati dal backup: {selected_backup.name}")
            print(f"   L'operazione è IRREVERSIBILE!")
            
            confirm = input(f"\n Continuare? (DIGITA 'CONFERMA' per procedere): ").strip()
            if confirm != "CONFERMA":
                print("❌ Operazione annullata")
                return False
        
        # 6. Esecuzione ripristino
        print(f"\n🚀 INIZIO RIPRISTINO JOURNAL")
        print(f"{'='*40}")
        
        total_start = time.time()
        
        # 6a. Svuota journal
        if not self.clear_current_journal():
            print("❌ Impossibile svuotare il journal")
            return False
        
        # 6b. Carica backup
        if not self.load_backup_to_journal(selected_backup):
            print("❌ Impossibile caricare il backup")
            return False
        
        # 7. Verifica finale
        final_count = self.get_current_triple_count()
        total_duration = time.time() - total_start
        
        print(f"\n📊 RIPRISTINO COMPLETATO")
        print(f"{'='*40}")
        print(f"✅ Journal aggiornato con successo!")
        print(f"   📊 Triple prima: {current_count:,}")
        print(f"   📊 Triple dopo: {final_count:,}")
        print(f"   📄 Backup usato: {selected_backup.name}")
        print(f"   ⏱️ Durata totale: {total_duration:.2f}s")
        
        return True
    
    def create_journal_snapshot(self) -> Optional[Path]:
        """Crea snapshot del journal corrente prima del ripristino"""
        if not self.test_server_connection():
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        snapshot_file = self.backup_dir / f"journal_snapshot_before_restore_{timestamp}.nq"
        
        print(f"📸 Creazione snapshot journal corrente...")
        
        try:
            # Query CONSTRUCT per export completo
            construct_query = """
            CONSTRUCT { ?s ?p ?o }
            WHERE { 
                {
                    ?s ?p ?o
                }
                UNION
                {
                    GRAPH ?g { ?s ?p ?o }
                }
            }
            """
            
            response = requests.post(
                self.sparql_endpoint,
                data={'query': construct_query},
                headers={'Accept': 'application/n-quads'},
                timeout=600
            )
            
            if response.status_code == 200:
                with open(snapshot_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                file_size = snapshot_file.stat().st_size
                print(f"✅ Snapshot creato: {snapshot_file.name} ({file_size:,} bytes)")
                return snapshot_file
            else:
                print(f"❌ Creazione snapshot fallita: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Errore creazione snapshot: {e}")
            return None


# ============================================================================
# INTEGRAZIONE CON LA PIPELINE ESISTENTE
# ============================================================================

def add_restore_step_to_pipeline():
    """
    Aggiungi questo metodo alla classe MultiDirectoryPipeline
    
    NOTA: Questo è solo un template - copialo manualmente nella pipeline
    """
    
    def step_restore_from_backup(self, backup_file: Optional[str] = None) -> Dict[str, bool]:
        """Step Restore: Ripristina journal da backup specifico"""
        step_start = time.time()
        self.logger.step_start("RESTORE", "Ripristino Journal da Backup")
        
        # Crea restorer usando la classe di questo file
        restorer = BlazegraphJournalRestorer(self.working_dir)
        
        # Se backup_file specificato, convertilo in Path
        selected_backup = None
        if backup_file:
            selected_backup = Path(backup_file)
            if not selected_backup.exists():
                selected_backup = self.working_dir / "backups" / backup_file
        
        # Esegui ripristino (non interattivo in pipeline)
        restore_success = restorer.restore_journal_from_backup(
            backup_file=selected_backup,
            interactive=False
        )
        
        # Risultati per tutte le directory
        results = {dir_key: restore_success for dir_key in self.directories.keys()}
        
        step_duration = time.time() - step_start
        success_count = sum(1 for success in results.values() if success)
        self.logger.step_complete("RESTORE", "Ripristino Journal", step_duration, success_count, len(results))
        
        return results

    # Restituisci la funzione come template
    return step_restore_from_backup


# ============================================================================
# UTILIZZO STANDALONE
# ============================================================================

def main():
    """Interfaccia a riga di comando per ripristino journal"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ripristina Journal Blazegraph da Backup")
    parser.add_argument('action', choices=['list', 'restore', 'snapshot'], 
                       help='Azione da eseguire')
    parser.add_argument('--backup', help='File backup specifico da ripristinare')
    parser.add_argument('--no-confirm', action='store_true',
                       help='Non chiedere conferma (automatico)')
    parser.add_argument('--working-dir', default='.',
                       help='Directory di lavoro (default: directory corrente)')
    
    args = parser.parse_args()
    
    working_dir = Path(args.working_dir)
    restorer = BlazegraphJournalRestorer(working_dir)
    
    if args.action == 'list':
        backups = restorer.list_available_backups()
        if backups:
            print(f"📋 Backup disponibili ({len(backups)}):")
            for i, (file_path, backup_type, date_part, time_part, size, modified) in enumerate(backups, 1):
                print(f"   {i}. {file_path.name}")
                print(f"      Tipo: {backup_type}")
                print(f"      Data: {modified.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"      Dimensione: {size:,} bytes")
                print()
        else:
            print("❌ Nessun backup trovato")
            return 1
    
    elif args.action == 'restore':
        backup_file = None
        if args.backup:
            backup_file = Path(args.backup)
            if not backup_file.exists():
                backup_file = working_dir / "backups" / args.backup
        
        success = restorer.restore_journal_from_backup(
            backup_file=backup_file,
            interactive=not args.no_confirm
        )
        
        return 0 if success else 1
    
    elif args.action == 'snapshot':
        snapshot = restorer.create_journal_snapshot()
        return 0 if snapshot else 1
    
    return 0


if __name__ == "__main__":
    exit(main())