import json
import argparse
import subprocess
import sys
import os
import shlex
from datetime import datetime
from pathlib import Path

# === IMPORT CONFIGURAZIONE CENTRALIZZATA ===
try:
    from config_loader import load_config, ConfigError
    USE_CENTRALIZED_CONFIG = True
    print("✅ Configurazione centralizzata caricata")
except ImportError:
    USE_CENTRALIZED_CONFIG = False
    ConfigError = Exception  # Fallback
    load_config = None  # Fallback
    print("⚠️ Configurazione centralizzata non disponibile, uso configurazione locale")

# === CONFIGURAZIONE LOCALE (FALLBACK) ===
DIRECTORY_CONFIGS_FALLBACK = {
    'floppy': {
        'path': '/media/sdb1/evangelisti/data/FloppyDisks/',
        'description': 'Floppy Disks',
        'default_output': 'FloppyDisks_CNT.json',
        'json_file': 'FloppyDisks_CNT.json',
        'hash_file': 'FloppyDisks_HASH.json',
        'base_path': '/media/sdb1/evangelisti/data/FloppyDisks/'
    },
    'hd': {
        'path': '/media/sdb1/evangelisti/data/HardDiskValerio/',
        'description': 'Hard Disk',
        'default_output': 'HD_CNT.json',
        'json_file': 'HD_CNT.json',
        'hash_file': 'HD_HASH.json',
        'base_path': '/media/sdb1/evangelisti/data/HardDiskValerio/'
    },
    'hdesterno': {
        'path': '/media/sdb1/evangelisti/data/HDEsternoEvangelisti/',
        'description': 'HD Esterno Evangelisti',
        'default_output': 'HDEsterno_CNT.json',
        'json_file': 'HDEsterno_CNT.json',
        'hash_file': 'HDEsterno_HASH.json',
        'base_path': '/media/sdb1/evangelisti/data/HDEsternoEvangelisti/'
    }
}

def get_directory_configs():
    """Ottiene le configurazioni delle directory (centralizzate o fallback)"""
    if USE_CENTRALIZED_CONFIG and load_config is not None:
        try:
            config = load_config()
            return config.to_legacy_format("file_count")
        except ConfigError as e:
            print(f"⚠️ Errore configurazione centralizzata: {e}")
            print("🔄 Fallback alla configurazione locale")
            return DIRECTORY_CONFIGS_FALLBACK
    else:
        return DIRECTORY_CONFIGS_FALLBACK

# Carica le configurazioni (dinamicamente)
DIRECTORY_CONFIGS = get_directory_configs()

def run_command(command, capture_output=True, shell=True):
    """Esegue un comando nel terminale e restituisce il risultato"""
    try:
        result = subprocess.run(command, shell=shell, capture_output=capture_output, text=True)
        if capture_output:
            return result.stdout.strip(), result.stderr.strip(), result.returncode
        return "", "", result.returncode
    except Exception as e:
        print(f"Errore nell'esecuzione del comando: {e}")
        return "", str(e), 1

def usage():
    """Mostra l'aiuto per l'utilizzo dello script - COME IL BASH"""
    print("Uso: python file_count.py [-o output.json] [-r] [-w] [-f] [-e] [-h] [directory]")
    print("  -o: specifica il nome del file di output (default: conteggio_file.json)")
    print("  -r: conta ricorsivamente anche nelle sottocartelle")
    print("  -w: rende la directory read-only dopo l'analisi")
    print("  -f: forza l'operazione read-only senza chiedere conferma (usare con -w)")
    print("  -e: escludi esplicitamente le directory (include file, link, device, ecc.)")
    print("  -h: mostra questo aiuto")
    print("  directory: directory da analizzare (default: directory corrente)")
    print("")
    print("TIPI DI ARCHIVIO PREDEFINITI (opzionale):")
    for tipo, config in DIRECTORY_CONFIGS.items():
        print(f"  {tipo}: {config['description']}")
        print(f"       Path: {config['path']}")
        print("")
    print("ESEMPI:")
    print("  python file_count.py                                    # Analizza directory corrente")
    print("  python file_count.py /path/to/directory                 # Analizza directory specifica")
    print("  python file_count.py floppy                             # Usa configurazione predefinita")
    print("  python file_count.py /path -o custom.json -r -w -f      # Custom con parametri")
    print("")
    print("ATTENZIONE: L'opzione -w rimuove i permessi di scrittura dalla directory e da tutto il suo contenuto!")
    print("           Verrà creato un file di backup per ripristinare i permessi originali.")

def save_permissions(target_dir, perm_file):
    """Salva i permessi originali - COME IL BASH"""
    try:
        print(f"Salvando i permessi originali in: {perm_file}")
        
        # Header del file di backup
        with open(perm_file, 'w') as f:
            f.write(f"#!/bin/bash\n")
            f.write(f"# File di backup permessi generato il {datetime.now()}\n")
            f.write(f"# Per ripristinare i permessi, esegui: bash {perm_file}\n")
            f.write(f"\n")
        
        # Usa find e stat per salvare i permessi (come nel bash) - CON ESCAPE CORRETTO
        escaped_dir = shlex.quote(target_dir)
        find_cmd = f"find {escaped_dir} -exec stat -c \"chmod %a '%n'\" {{}} \\;"
        stdout, stderr, returncode = run_command(find_cmd)
        
        if returncode != 0:
            # Fallback per macOS (stat ha sintassi diversa)
            find_cmd = f"find {escaped_dir} -exec stat -f \"chmod %Lp '%N'\" {{}} \\;"
            stdout, stderr, returncode = run_command(find_cmd)
        
        if returncode == 0:
            with open(perm_file, 'a') as f:
                f.write(stdout)
            
            # Rende il file di backup eseguibile
            os.chmod(perm_file, 0o755)
            return True
        else:
            print(f"Errore nel salvare i permessi: {stderr}")
            return False
            
    except Exception as e:
        print(f"Errore nel salvare i permessi: {e}")
        return False

def make_readonly(target_dir, backup_file):
    """Rende la directory read-only - COME IL BASH"""
    print(f"Salvando i permessi originali in: {backup_file}")
    if not save_permissions(target_dir, backup_file):
        print("Errore: impossibile salvare i permessi originali")
        return False
    
    print(f"Rendendo read-only la directory: {target_dir}")
    escaped_dir = shlex.quote(target_dir)
    chmod_cmd = f"chmod -R -w {escaped_dir}"
    stdout, stderr, returncode = run_command(chmod_cmd)
    
    if returncode == 0:
        print("Directory resa read-only con successo!")
        print(f"Per ripristinare i permessi originali, esegui: bash {backup_file}")
        return True
    else:
        print("Errore: impossibile rendere read-only la directory")
        return False

def count_files_total(directory, exclude_dirs=False):
    """Conta il totale dei file nella directory (sempre ricorsivo) - VERSIONE CORRETTA"""
    escaped_dir = shlex.quote(directory)
    
    if exclude_dirs:
        # Conta tutto eccetto le directory (file, link, device, ecc.)
        find_cmd = f"find {escaped_dir} ! -type d | wc -l"
    else:
        # Comportamento originale del bash (solo file regolari)
        find_cmd = f"find {escaped_dir} -type f | wc -l"
    
    stdout, stderr, returncode = run_command(find_cmd)
    
    if returncode == 0:
        try:
            count = int(stdout.strip())
            print(f"DEBUG: Conteggio totale per '{directory}': {count}")
            return count
        except ValueError:
            print(f"DEBUG: Errore nel convertire output totale in numero: '{stdout}'")
            return 0
    else:
        print(f"Errore nel conteggio totale: {stderr}")
        return 0

def count_files_in_dir(directory, recursive=True, exclude_dirs=False):
    """Conta i file in una directory specifica - VERSIONE CORRETTA"""
    escaped_dir = shlex.quote(directory)
    
    if exclude_dirs:
        if recursive:
            find_cmd = f"find {escaped_dir} ! -type d | wc -l"
        else:
            find_cmd = f"find {escaped_dir} -maxdepth 1 ! -type d | wc -l"
    else:
        if recursive:
            find_cmd = f"find {escaped_dir} -type f | wc -l"
        else:
            find_cmd = f"find {escaped_dir} -maxdepth 1 -type f | wc -l"
    
    stdout, stderr, returncode = run_command(find_cmd)
    
    if returncode == 0:
        try:
            count = int(stdout.strip())
            # Debug: stampa se il conteggio è 0 per directory che dovrebbero avere file
            if count == 0:
                print(f"DEBUG: Directory '{directory}' risulta vuota")
                print(f"DEBUG: Comando eseguito: {find_cmd}")
                # Verifica se la directory esiste
                if not os.path.exists(directory):
                    print(f"DEBUG: Directory non esiste!")
                elif not os.path.isdir(directory):
                    print(f"DEBUG: Path non è una directory!")
                else:
                    # Lista i contenuti per debug
                    try:
                        contents = os.listdir(directory)
                        print(f"DEBUG: Contenuti trovati con os.listdir: {len(contents)} elementi")
                        if len(contents) > 0:
                            print(f"DEBUG: Primi 5 elementi: {contents[:5]}")
                            # Test manuale find
                            test_cmd = f"find {escaped_dir} -type f"
                            test_stdout, test_stderr, test_returncode = run_command(test_cmd)
                            if test_returncode == 0:
                                files = test_stdout.strip().split('\n') if test_stdout.strip() else []
                                print(f"DEBUG: File trovati con find: {len([f for f in files if f])}")
                    except Exception as e:
                        print(f"DEBUG: Errore nel leggere contenuti: {e}")
            return count
        except ValueError:
            print(f"DEBUG: Errore nel convertire output in numero: '{stdout}'")
            return 0
    else:
        print(f"DEBUG: Errore nel conteggio per '{directory}': {stderr}")
        return 0

def count_files_python_fallback(directory, recursive=True, exclude_dirs=False):
    """Versione di fallback che usa Python puro invece di comandi shell"""
    print(f"DEBUG: Usando fallback Python per '{directory}'")
    
    count = 0
    
    try:
        if recursive:
            for root, dirs, files in os.walk(directory):
                if exclude_dirs:
                    # Conta tutto tranne le directory
                    count += len(files)
                    # Aggiungi altri tipi di file se necessario (link, device, ecc.)
                    for item in os.listdir(root):
                        item_path = os.path.join(root, item)
                        if not os.path.isdir(item_path) and not os.path.isfile(item_path):
                            count += 1
                else:
                    # Conta solo file regolari
                    for file in files:
                        filepath = os.path.join(root, file)
                        if os.path.isfile(filepath) and not os.path.islink(filepath):
                            count += 1
        else:
            items = os.listdir(directory)
            for item in items:
                filepath = os.path.join(directory, item)
                if exclude_dirs:
                    # Conta tutto tranne le directory
                    if not os.path.isdir(filepath):
                        count += 1
                else:
                    # Conta solo file regolari
                    if os.path.isfile(filepath) and not os.path.islink(filepath):
                        count += 1
                        
        print(f"DEBUG: Fallback Python ha trovato {count} file")
        return count
        
    except OSError as e:
        print(f"DEBUG: Errore nell'accesso alla directory {directory}: {e}")
        return 0

def get_all_subdirectories(directory):
    """Ottiene tutte le sottodirectory - VERSIONE CORRETTA"""
    escaped_dir = shlex.quote(directory)
    find_cmd = f"find {escaped_dir} -type d -print0"
    stdout, stderr, returncode = run_command(find_cmd)
    
    if returncode != 0:
        print(f"Errore nel trovare le directory: {stderr}")
        # Fallback Python
        try:
            subdirs = []
            for root, dirs, files in os.walk(directory):
                for dir_name in dirs:
                    subdir_path = os.path.join(root, dir_name)
                    subdirs.append(subdir_path)
            return subdirs
        except Exception as e:
            print(f"Errore anche nel fallback Python: {e}")
            return []
    
    # Processa directory usando -print0 per gestire spazi nei nomi
    if stdout.strip():
        directories = stdout.strip('\0').split('\0')
        # Filtra solo le sottodirectory (esclude la directory root)
        subdirs = [d for d in directories if d and d != directory]
        return subdirs
    else:
        return []


def resolve_directory_path(input_path):
    """Risolve il path della directory - COMPATIBILE CON PIPELINE E BASH"""
    
    # Se non è specificato, usa directory corrente (come bash)
    if not input_path:
        current_dir = os.getcwd()
        return current_dir, "conteggio_file.json", None
    
    # Se è un tipo di archivio predefinito
    if input_path in DIRECTORY_CONFIGS:
        config = DIRECTORY_CONFIGS[input_path]
        resolved_path = config['path']
        default_output = config['default_output']  # USA SEMPRE LA CONFIGURAZIONE
        
        print(f"📁 Tipo archivio riconosciuto: {input_path}")
        print(f"📄 Descrizione: {config['description']}")
        print(f"🎯 Path target: {resolved_path}")
        print(f"📋 Output file dalla configurazione: {default_output}")
        
        return resolved_path, default_output, input_path
    
    # Altrimenti verifica se il path corrisponde a una configurazione
    input_path_normalized = os.path.abspath(os.path.expanduser(input_path)).rstrip('/')
    
    for dir_key, config in DIRECTORY_CONFIGS.items():
        config_path_normalized = os.path.abspath(config['path']).rstrip('/')
        
        if input_path_normalized == config_path_normalized:
            print(f"📁 Path riconosciuto come '{dir_key}': {config['description']}")
            print(f"📋 Output file dalla configurazione: {config['default_output']}")
            return config['path'], config['default_output'], dir_key
    
    # Altrimenti è un path generico (espande ~ e rende assoluto)
    expanded_path = os.path.expanduser(input_path)
    absolute_path = os.path.abspath(expanded_path)
    
    if not os.path.exists(absolute_path):
        print(f"Errore: {absolute_path} non è una directory valida")
        return None, None, None
        
    if not os.path.isdir(absolute_path):
        print(f"Errore: {absolute_path} non è una directory")
        return None, None, None
    
    # Genera nome output basato sul path (come bash default)
    dir_name = os.path.basename(absolute_path.rstrip('/'))
    default_output = f"{dir_name}_CNT.json" if dir_name else "conteggio_file.json"
    
    print(f"📁 Path generico non riconosciuto")
    print(f"🔧 Output file generato automaticamente: {default_output}")
    
    return absolute_path, default_output, None

def write_json_like_bash(output_file, directory, total_files, recursive, exclude_dirs, write_protect, 
                        subdirs_data, readonly_applied=False, backup_file="", archive_type=None):
    """Scrive il JSON esattamente come fa il bash"""
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Inizio JSON
            f.write("{\n")
            f.write(f'  "directory_principale": "{directory}",\n')
            f.write(f'  "conteggio_totale": {total_files},\n')
            f.write(f'  "modalita_ricorsiva": {str(recursive).lower()},\n')
            
            # Aggiunge modalità conteggio
            if exclude_dirs:
                f.write(f'  "modalita_conteggio": "esclude_directory",\n')
                f.write(f'  "escludi_directory": true,\n')
            else:
                f.write(f'  "modalita_conteggio": "solo_file_regolari",\n')
                f.write(f'  "escludi_directory": false,\n')
            
            f.write(f'  "write_protect_richiesta": {str(write_protect).lower()},\n')
            f.write(f'  "data_analisi": "{datetime.now().isoformat()}",\n')
            
            # Aggiunge informazioni archivio se è un tipo predefinito
            if archive_type and archive_type in DIRECTORY_CONFIGS:
                config = DIRECTORY_CONFIGS[archive_type]
                f.write(f'  "tipo_archivio": "{archive_type}",\n')
                f.write(f'  "descrizione_archivio": "{config["description"]}",\n')
                f.write(f'  "hash_file_associato": "{config["hash_file"]}",\n')
                f.write(f'  "base_path_count_check": "{config["base_path"]}",\n')
            
            # Sottocartelle
            f.write('  "sottocartelle": [\n')
            
            # Scrive ogni sottocartella
            for i, (subdir, file_count) in enumerate(subdirs_data):
                if i > 0:
                    f.write(',\n')
                # Escape delle virgolette nei path
                escaped_path = subdir.replace('"', '\\"')
                f.write(f'    {{"path": "{escaped_path}", "file_count": {file_count}}}')
            
            f.write('\n  ],\n')
            
            # Status finale
            f.write(f'  "write_protect_applicata": {str(readonly_applied).lower()},\n')
            
            if readonly_applied and backup_file:
                f.write(f'  "backup_permessi": "{backup_file}"\n')
            else:
                f.write('  "backup_permessi": null\n')
                
            f.write('}\n')
            
        return True
    except Exception as e:
        print(f"Errore nel scrivere il file JSON: {e}")
        return False

def main():
    # Parser argomenti - COMPATIBILE CON BASH
    parser = argparse.ArgumentParser(description='Conta i file in una directory e salva i risultati in JSON',
                                   add_help=False)
    parser.add_argument('-o', dest='output_file', default=None,
                       help='specifica il nome del file di output (default: conteggio_file.json)')
    parser.add_argument('-r', dest='recursive', action='store_true',
                       help='conta ricorsivamente anche nelle sottocartelle')
    parser.add_argument('-w', dest='write_protect', action='store_true',
                       help='rende la directory read-only dopo l\'analisi')
    parser.add_argument('-f', dest='force', action='store_true',
                       help='forza l\'operazione read-only senza chiedere conferma (usare con -w)')
    parser.add_argument('-e', dest='exclude_dirs', action='store_true',
                       help='escludi esplicitamente le directory (include file, link, device, ecc.)')
    parser.add_argument('-h', dest='help', action='store_true',
                       help='mostra questo aiuto')
    parser.add_argument('directory', nargs='?', default=None,
                       help='directory da analizzare (default: directory corrente)')
    
    args = parser.parse_args()
    
    if args.help:
        usage()
        return 0
    
    # Risolve directory (può essere None, path, o tipo predefinito)
    directory, default_output, archive_type = resolve_directory_path(args.directory)
    
    if directory is None:
        return 1
    
    # Converte il path in assoluto (come bash con realpath) - CON ESCAPE CORRETTO
    try:
        escaped_dir = shlex.quote(directory)
        realpath_cmd = f"realpath {escaped_dir}"
        stdout, stderr, returncode = run_command(realpath_cmd)
        if returncode == 0:
            directory = stdout.strip()
        else:
            # Fallback
            directory = os.path.abspath(directory)
    except:
        directory = os.path.abspath(directory)
    
    # Determina file di output
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = default_output if default_output else "conteggio_file.json"
    
    print("Analisi completata!")
    print(f"Directory analizzata: {directory}")
    
    # Conta il totale dei file (sempre ricorsivo come nel bash)
    # Per i tipi di archivio predefiniti, usa exclude_dirs=True per includere tutti i tipi di file
    effective_exclude_dirs = args.exclude_dirs or (archive_type is not None)
    total_files = count_files_total(directory, effective_exclude_dirs)
    print(f"Totale file: {total_files}")
    print(f"Modalità: {'Ricorsiva' if args.recursive else 'Non ricorsiva'}")
    print(f"Risultati salvati in: {output_file}")
    
    # Ottiene tutte le sottodirectory
    subdirectories = get_all_subdirectories(directory)
    print(f"DEBUG: Trovate {len(subdirectories)} sottodirectory")
    
    # Calcola conteggi per ogni sottodirectory
    subdirs_data = []
    for subdir in subdirectories:
        # Per le sottodirectory, conta solo i file direttamente nella directory (non ricorsivo)
        # Questo per essere compatibile con il conteggio RDF che conta solo file diretti
        # Per i tipi di archivio predefiniti, usa exclude_dirs=True per includere tutti i tipi di file
        effective_exclude_dirs = args.exclude_dirs or (archive_type is not None)
        file_count = count_files_in_dir(subdir, recursive=True, exclude_dirs=effective_exclude_dirs)
        
        # Se il conteggio è 0 ma la directory sembra avere contenuti, usa fallback
        if file_count == 0 and os.path.exists(subdir):
            try:
                contents = os.listdir(subdir)
                if len(contents) > 0:
                    print(f"DEBUG: Usando fallback per '{subdir}'")
                    file_count = count_files_python_fallback(subdir, recursive=True, exclude_dirs=effective_exclude_dirs)
            except:
                pass
        
        subdirs_data.append((subdir, file_count))
    
    # Gestisce protezione scrittura
    readonly_applied = False
    backup_file = ""
    
    if args.write_protect:
        print("")
        print("=== APPLICAZIONE PROTEZIONE SCRITTURA ===")
        
        if args.force:
            print("Modalità force attivata: applicando protezione senza conferma...")
            backup_file = f"permessi_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sh"
            if make_readonly(directory, backup_file):
                readonly_applied = True
        else:
            confirm = input(f"Sei sicuro di voler rendere read-only '{directory}' e tutto il suo contenuto? (s/N): ")
            if confirm.lower() in ['s', 'si', 'sì', 'y', 'yes']:
                backup_file = f"permessi_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sh"
                if make_readonly(directory, backup_file):
                    readonly_applied = True
            else:
                print("Protezione scrittura annullata.")
    
    # Scrive il JSON esattamente come il bash
    if not write_json_like_bash(output_file, directory, total_files, args.recursive, 
                               effective_exclude_dirs, args.write_protect, subdirs_data, 
                               readonly_applied, backup_file, archive_type):
        return 1
    
    # Mostra il contenuto del JSON (come nel bash)
    print("")
    print("Contenuto del file JSON:")
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            print(f.read())
    except Exception as e:
        print(f"Errore nel leggere il file JSON: {e}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())