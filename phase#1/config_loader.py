#!/usr/bin/env python3
"""
Modulo per il caricamento della configurazione unificata dell'Archivio Evangelisti

Gestisce il caricamento del file directory_config.json e fornisce funzioni di utilità
per accedere alle configurazioni da tutti gli script del progetto.

Autore: Sistema Pipeline Evangelisti
Data: 2025
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

# === COSTANTI ===
CONFIG_FILE = "directory_config.json"
CONFIG_VERSION_REQUIRED = "1.0"

class ConfigError(Exception):
    """Eccezione personalizzata per errori di configurazione"""
    pass

class EvangelistiConfig:
    """Classe per gestire la configurazione dell'Archivio Evangelisti"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Inizializza la configurazione
        
        Args:
            config_path: Path opzionale al file di configurazione
        """
        self.config_path = config_path or self._find_config_file()
        self.config = self._load_config()
        self._validate_config()
        # ✅ AGGIUNTO: Validazione consistenza suffix
        self.validate_suffix_consistency()
    
    def _find_config_file(self) -> str:
        """Trova il file di configurazione nella directory corrente o nelle parent"""
        current_dir = Path.cwd()
        
        # Cerca nella directory corrente
        config_file = current_dir / CONFIG_FILE
        if config_file.exists():
            return str(config_file)
        
        # Cerca nelle directory parent (fino a 3 livelli)
        for parent in current_dir.parents[:3]:
            config_file = parent / CONFIG_FILE
            if config_file.exists():
                return str(config_file)
        
        raise ConfigError(f"File di configurazione '{CONFIG_FILE}' non trovato nella directory corrente o nelle parent")
    
    def _load_config(self) -> Dict[str, Any]:
        """Carica il file di configurazione JSON"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            print(f"✅ Configurazione caricata da: {self.config_path}")
            return config
        except FileNotFoundError:
            raise ConfigError(f"File di configurazione non trovato: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ConfigError(f"Errore nel parsing JSON: {e}")
        except Exception as e:
            raise ConfigError(f"Errore nel caricamento configurazione: {e}")
    
    def _validate_config(self):
        """Valida la struttura della configurazione"""
        required_sections = ['directories', 'blazegraph', 'pipeline', 'metadata']
        
        for section in required_sections:
            if section not in self.config:
                raise ConfigError(f"Sezione mancante nella configurazione: {section}")
        
        # Verifica versione
        config_version = self.config.get('metadata', {}).get('version')
        if config_version != CONFIG_VERSION_REQUIRED:
            print(f"⚠️ Versione configurazione: {config_version}, richiesta: {CONFIG_VERSION_REQUIRED}")
        
        # Verifica che ci siano directory configurate
        if not self.config['directories']:
            raise ConfigError("Nessuna directory configurata")
        
        print(f"✅ Configurazione validata - {len(self.config['directories'])} directory disponibili")

    def validate_suffix_consistency(self):
        """
        ✅ NUOVO METODO: Valida che tutti i suffix siano in lowercase e consistenti
        """
        print("🔍 Validazione consistenza suffix...")
        
        inconsistencies = []
        
        for dir_key, config in self.config['directories'].items():
            # Verifica output_suffix
            output_suffix = config.get('structure', {}).get('output_suffix', '')
            if output_suffix != output_suffix.lower():
                inconsistencies.append(f"{dir_key}: output_suffix '{output_suffix}' non è lowercase")
            
            # Verifica metadata.directory
            metadata_directory = config.get('metadata', {}).get('directory', '')
            if metadata_directory != metadata_directory.lower():
                inconsistencies.append(f"{dir_key}: metadata.directory '{metadata_directory}' non è lowercase")
            
            # Verifica consistenza tra output_suffix e metadata.directory
            if output_suffix.lower() != metadata_directory.lower():
                inconsistencies.append(f"{dir_key}: output_suffix '{output_suffix}' non corrisponde a metadata.directory '{metadata_directory}'")
        
        if inconsistencies:
            print("❌ Inconsistenze trovate nei suffix:")
            for issue in inconsistencies:
                print(f"   - {issue}")
            raise ConfigError(f"Trovate {len(inconsistencies)} inconsistenze nei suffix. Correggi il file di configurazione.")
        else:
            print("✅ Tutti i suffix sono consistenti e in lowercase")
    
    # === METODI PER ACCESSO DIRECTORY ===
    
    def get_directories(self) -> Dict[str, Dict[str, Any]]:
        """Restituisce tutte le directory configurate"""
        return self.config['directories']
    
    def get_directory_config(self, directory_key: str) -> Dict[str, Any]:
        """Restituisce la configurazione per una directory specifica"""
        if directory_key not in self.config['directories']:
            available = ', '.join(self.config['directories'].keys())
            raise ConfigError(f"Directory '{directory_key}' non trovata. Disponibili: {available}")
        return self.config['directories'][directory_key]
    
    def get_directory_path(self, directory_key: str) -> str:
        """Restituisce il path per una directory"""
        return self.get_directory_config(directory_key)['path']
    
    def get_directory_description(self, directory_key: str) -> str:
        """Restituisce la descrizione per una directory"""
        return self.get_directory_config(directory_key)['description']
    
    def list_directory_keys(self) -> list:
        """Restituisce la lista delle chiavi directory disponibili"""
        return list(self.config['directories'].keys())
    
    # === METODI PER FILE OUTPUT ===
    
    def get_count_output_file(self, directory_key: str) -> str:
        """Restituisce il nome del file di output per il conteggio"""
        return self.get_directory_config(directory_key)['files']['count_output']
    
    def get_hash_output_file(self, directory_key: str) -> str:
        """Restituisce il nome del file di output per gli hash"""
        return self.get_directory_config(directory_key)['files']['hash_output']
    
    def get_structure_output_file(self, directory_key: str) -> str:
        """Restituisce il nome del file di output per la struttura RDF"""
        return self.get_directory_config(directory_key)['files']['structure_output']
    
    # === METODI PER STRUTTURA RDF ===
    
    def get_structure_config(self, directory_key: str) -> Dict[str, Any]:
        """Restituisce la configurazione struttura per evangelisti_structure_generation.py"""
        return self.get_directory_config(directory_key)['structure']
    
    def get_root_id(self, directory_key: str) -> str:
        """Restituisce il root_id per la generazione struttura"""
        return self.get_structure_config(directory_key)['root_id']
    
    # === METODI PER METADATA EXTRACTION ===
    
    def get_metadata_config(self, directory_key: str) -> Dict[str, Any]:
        """Restituisce la configurazione metadata per evangelisti_metadata_extraction.py"""
        return self.get_directory_config(directory_key)['metadata']
    
    def get_metadata_directory(self, directory_key: str) -> str:
        """Restituisce il parametro directory per metadata extraction"""
        return self.get_metadata_config(directory_key)['directory']
    
    # === METODI PER CHECK SCRIPTS ===
    
    def get_check_type(self, directory_key: str) -> str:
        """Restituisce il check_type per count_check.py e integrity_check.py"""
        return self.get_metadata_config(directory_key)['check_type']
    
    def get_base_path_normalized(self, directory_key: str) -> str:
        """Restituisce il base_path normalizzato per i check scripts"""
        return self.get_directory_config(directory_key)['base_path_normalized']
    
    # === METODI PER BLAZEGRAPH ===
    
    def get_blazegraph_config(self) -> Dict[str, Any]:
        """Restituisce la configurazione Blazegraph"""
        return self.config['blazegraph']
    
    def get_blazegraph_base_url(self) -> str:
        """Restituisce l'URL base di Blazegraph"""
        return self.get_blazegraph_config()['base_url']
    
    def get_blazegraph_namespace(self) -> str:
        """Restituisce il namespace Blazegraph"""
        return self.get_blazegraph_config()['namespace']
    
    def get_blazegraph_endpoints(self) -> list:
        """Restituisce la lista degli endpoint Blazegraph da testare"""
        return self.get_blazegraph_config()['endpoints']
    
    # === METODI PER PIPELINE ===
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """Restituisce la configurazione pipeline"""
        return self.config['pipeline']
    
    def get_blazegraph_reset_config(self) -> Dict[str, Any]:
        """Restituisce la configurazione per il reset Blazegraph"""
        return self.get_pipeline_config()['reset_config']
    
    def get_batch_sizes(self) -> Dict[str, int]:
        """Restituisce le configurazioni dei batch size"""
        return self.get_pipeline_config()['batch_sizes']
    
    # === METODI DI UTILITÀ ===
    
    def print_summary(self):
        """Stampa un riepilogo della configurazione caricata"""
        print(f"\n{'='*60}")
        print(f"CONFIGURAZIONE ARCHIVIO EVANGELISTI")
        print(f"{'='*60}")
        print(f"📄 File: {self.config_path}")
        print(f"📅 Versione: {self.config['metadata']['version']}")
        print(f"📝 Descrizione: {self.config['metadata']['description']}")
        print(f"🔄 Ultimo aggiornamento: {self.config['metadata']['last_updated']}")
        
        print(f"\n📁 DIRECTORY CONFIGURATE ({len(self.config['directories'])}):")
        for key, config in self.config['directories'].items():
            print(f"  {key:<12} : {config['description']}")
            print(f"             Path: {config['path']}")
            # ✅ AGGIUNTO: Mostra suffix per verifica
            output_suffix = config.get('structure', {}).get('output_suffix', 'N/A')
            metadata_dir = config.get('metadata', {}).get('directory', 'N/A')
            print(f"             Suffix: {output_suffix} | Metadata dir: {metadata_dir}")
        
        print(f"\n🔗 BLAZEGRAPH:")
        print(f"  Base URL: {self.get_blazegraph_base_url()}")
        print(f"  Namespace: {self.get_blazegraph_namespace()}")
        print(f"  Endpoint: {len(self.get_blazegraph_endpoints())} configurati")
        
        print(f"\n⚙️ PIPELINE:")
        reset_config = self.get_blazegraph_reset_config()
        print(f"  Reset on start: {reset_config['reset_on_start']}")
        print(f"  Batch sizes: {self.get_batch_sizes()}")
        print(f"{'='*60}")
    
    def validate_paths(self) -> Dict[str, bool]:
        """Valida l'esistenza di tutti i path configurati"""
        print("🔍 Validazione path delle directory...")
        results = {}
        
        for key, config in self.config['directories'].items():
            path = Path(config['path'])
            exists = path.exists() and path.is_dir()
            results[key] = exists
            
            status = "✅" if exists else "❌"
            print(f"  {key:<12} : {status} {config['path']}")
            if not exists:
                print(f"             ⚠️ Path non trovato o non è una directory")
        
        valid_count = sum(results.values())
        total_count = len(results)
        print(f"\n📊 Validazione completata: {valid_count}/{total_count} path validi")
        
        return results
    
    def to_legacy_format(self, script_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Converte la configurazione nel formato legacy per compatibilità retroattiva
        
        Args:
            script_name: Nome dello script per cui generare il formato legacy
        
        Returns:
            Dizionario nel formato legacy appropriato
        """
        legacy_configs = {}
        
        for key, config in self.config['directories'].items():
            if script_name == "file_count":
                legacy_configs[key] = {
                    'path': config['path'],
                    'description': config['description'],
                    'default_output': config['files']['count_output'],
                    'json_file': config['files']['count_output'],
                    'hash_file': config['files']['hash_output'],
                    'base_path': config['base_path_normalized']
                }
            elif script_name == "hash_calc":
                legacy_configs[key] = {
                    'path': config['path'],
                    'hash_output': config['files']['hash_output'],
                    'description': config['description']
                }
            elif script_name == "evangelisti_structure_generation":
                legacy_configs[key] = {
                    'path': config['path'],
                    'root_id': config['structure']['root_id'],
                    'output_suffix': config['structure']['output_suffix'],
                    'log_suffix': config['structure']['log_suffix']
                }
            elif script_name == "evangelisti_metadata_extraction":
                # ✅ FIX CRITICO: Normalizza SEMPRE il suffix in lowercase
                original_suffix = config['structure']['output_suffix']
                normalized_suffix = original_suffix.lower().strip()
                
                legacy_configs[key] = {
                    'root_path': config['path'],
                    'input_nquads': config['files']['structure_output'],
                    'suffix': normalized_suffix  # ✅ SEMPRE lowercase
                }
                
                # Debug per verificare normalizzazione
                if original_suffix != normalized_suffix:
                    print(f"🔄 Normalizzato suffix per {key}: '{original_suffix}' -> '{normalized_suffix}'")
                    
            elif script_name == "evangelisti_pipeline_test":
                # ✅ FIX CRITICO: Normalizza SEMPRE metadata_directory in lowercase
                original_metadata_dir = config['metadata']['directory']
                normalized_metadata_dir = original_metadata_dir.lower().strip()
                
                legacy_configs[key] = {
                    'path': config['path'],
                    'structure_type': key,  # stesso nome della chiave
                    'count_output': config['files']['count_output'],
                    'hash_output': config['files']['hash_output'],
                    'structure_output': config['files']['structure_output'],
                    'metadata_directory': normalized_metadata_dir,  # ✅ SEMPRE lowercase
                    'check_type': config['metadata']['check_type'],
                    'description': config['description']
                }
                
                # Debug per verificare normalizzazione
                if original_metadata_dir != normalized_metadata_dir:
                    print(f"🔄 Normalizzato metadata_directory per {key}: '{original_metadata_dir}' -> '{normalized_metadata_dir}'")
                    
            elif script_name in ["count_check", "integrity_check"]:
                legacy_configs[key] = {
                    'json_file': config['files']['count_output'] if script_name == "count_check" else config['files']['hash_output'],
                    'base_path': config['base_path_normalized'],
                    'description': config['description']
                }
        
        return legacy_configs


# === FUNZIONI DI CONVENIENZA ===

def load_config(config_path: Optional[str] = None) -> EvangelistiConfig:
    """Funzione di convenienza per caricare la configurazione"""
    return EvangelistiConfig(config_path)

def get_directories() -> Dict[str, Dict[str, Any]]:
    """Funzione di convenienza per ottenere tutte le directory"""
    config = load_config()
    return config.get_directories()

def get_directory_config(directory_key: str) -> Dict[str, Any]:
    """Funzione di convenienza per ottenere la configurazione di una directory"""
    config = load_config()
    return config.get_directory_config(directory_key)

# === COMPATIBILITÀ CON SCRIPT ESISTENTI ===

def get_legacy_configs(script_name: str) -> Dict[str, Dict[str, Any]]:
    """
    Restituisce le configurazioni nel formato legacy per compatibilità
    
    Args:
        script_name: Nome dello script (file_count, evangelisti_structure_generation, etc.)
    
    Returns:
        Dizionario nel formato legacy appropriato
    """
    config = load_config()
    return config.to_legacy_format(script_name)

# === MAIN PER TEST ===

def main():
    """Funzione main per testare il caricamento della configurazione"""
    try:
        print("🔄 Test caricamento configurazione...")
        config = load_config()
        
        # Stampa riepilogo
        config.print_summary()
        
        # Valida path
        print(f"\n🔍 Test validazione path...")
        path_results = config.validate_paths()
        
        # Test conversioni legacy
        print(f"\n🔄 Test compatibilità legacy...")
        for script in ["file_count", "evangelisti_structure_generation", "evangelisti_pipeline_test", "evangelisti_metadata_extraction"]:
            legacy = config.to_legacy_format(script)
            print(f"  {script}: {len(legacy)} configurazioni convertite")
            
            # ✅ NUOVO: Mostra suffix per evangelisti_metadata_extraction
            if script == "evangelisti_metadata_extraction":
                print("    📋 Suffix generati per metadata extraction:")
                for dir_key, dir_config in legacy.items():
                    suffix = dir_config.get('suffix', 'N/A')
                    print(f"      {dir_key}: '{suffix}'")
        
        print(f"\n✅ Test completato con successo!")
        
    except ConfigError as e:
        print(f"❌ Errore configurazione: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Errore generico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()