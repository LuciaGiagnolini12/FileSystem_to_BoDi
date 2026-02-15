#!/usr/bin/env python3

import concurrent.futures
import datetime
import json
import os
import subprocess
import sys
import time
import platform
from typing import List, Tuple

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

def get_directory_configs():
    """Ottiene le configurazioni delle directory"""
    if USE_CENTRALIZED_CONFIG and load_config is not None:
        try:
            config = load_config()
            return config.to_legacy_format("hash_calc")
        except ConfigError as e:
            print(f"⚠️ Errore configurazione centralizzata: {e}")
            print("🔄 Fallback alla configurazione locale")
        except Exception as e:
            print(f"⚠️ Errore generico configurazione centralizzata: {e}")
            print("🔄 Fallback alla configurazione locale")
    
    # Configurazione locale fallback
    return {
        'floppy': {
            'path': '/media/sdb1/evangelisti/data/FloppyDisks/',
            'hash_output': 'FloppyDisks_HASH.json',
            'description': 'Floppy Disks e CD'
        },
        'hd': {
            'path': '/media/sdb1/evangelisti/data/HardDiskValerio/',
            'hash_output': 'HD_HASH.json',
            'description': 'Hard Disk Valerio'
        },
        'hdesterno': {
            'path': '/media/sdb1/evangelisti/data/HDEsternoEvangelisti/',
            'hash_output': 'HDEsterno_HASH.json',
            'description': 'HD Esterno Evangelisti'
        }
    }

# Carica configurazioni
DIRECTORY_CONFIGS = get_directory_configs()

def detect_hash_command():
    """Rileva automaticamente il comando per calcolare SHA256"""
    if platform.system() == "Darwin":  # macOS
        return ["shasum", "-a", "256"]
    else:  # Linux e altri
        return ["sha256sum"]

def determine_output_filename(directory_path):
    """Determina il nome del file di output basandosi sulla configurazione"""
    # Normalizza il path della directory
    normalized_path = os.path.abspath(directory_path).rstrip('/')
    
    # Cerca nella configurazione
    for dir_key, config in DIRECTORY_CONFIGS.items():
        config_path = os.path.abspath(config['path']).rstrip('/')
        
        if normalized_path == config_path:
            output_file = config['hash_output']
            print(f"📋 Directory riconosciuta come '{dir_key}' -> {output_file}")
            return output_file
    
    # Fallback: genera automaticamente come prima
    base_name = os.path.basename(directory_path.rstrip('/'))
    output_file = f"{base_name}_HASH.json"
    print(f"🔧 Directory non riconosciuta, generazione automatica -> {output_file}")
    return output_file

def elencaFiles(rootDir: str) -> List[Tuple[str, int]]:
    """Ottiene la lista dei file con dimensioni"""
    res = []
    for dirPath, _, fileNames in os.walk(rootDir):
        for fileName in fileNames:
            filePath = os.path.join(dirPath, fileName)
            try:
                st = os.stat(filePath, follow_symlinks=False)
                # Salta i symlink per evitare problemi
                if not os.path.islink(filePath):
                    res.append((filePath, st.st_size))
            except (OSError, FileNotFoundError) as e:
                print(f"⚠️ Saltando file inaccessibile: {filePath} ({e})")
                continue
    return sorted(res, key=lambda x: -x[1])  # Ordina per dimensione decrescente

def calcHash(filePath: str) -> str:
    """Calcola l'hash SHA256 usando il comando di sistema appropriato"""
    hash_cmd = detect_hash_command()
    
    try:
        process = subprocess.Popen(
            hash_cmd + [filePath],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding='utf-8'
        )
        stdout, stderr = process.communicate(timeout=50000)  # Timeout per file grandi
        
        if process.returncode != 0:
            raise RuntimeError(f"Errore comando hash per {filePath}: {stderr}")
        
        # Estrai solo l'hash dalla risposta
        hash_value = stdout.split()[0]
        return hash_value
        
    except subprocess.TimeoutExpired:
        process.kill()
        raise RuntimeError(f"Timeout nel calcolo hash per: {filePath}")
    except Exception as e:
        raise RuntimeError(f"Errore nel calcolo hash per {filePath}: {e}")

def esaminaFile(info: Tuple[str, int]) -> dict:
    """Elabora un singolo file"""
    filePath, fileSize = info
    try:
        st = os.stat(filePath)
        hash_value = calcHash(filePath)
        
        return {
            "path": filePath,
            "sha256": hash_value,  # Chiave principale per compatibilità con integrity_check
            "hash": hash_value,    # Mantieni anche questa per retrocompatibilità
            "size": st.st_size,
            "modified": datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        # Ritorna comunque un oggetto, ma con errore
        return {
            "path": filePath,
            "sha256": None,
            "hash": None,
            "size": fileSize,
            "modified": None,
            "error": str(e)
        }

def adesso() -> str:
    """Restituisce la data/ora corrente formattata"""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def show_progress(current: int, total: int, start_time: float):
    """Mostra progresso personalizzato senza tqdm"""
    elapsed = time.time() - start_time
    if current > 0:
        rate = current / elapsed
        eta = (total - current) / rate if rate > 0 else 0
        eta_str = f", ETA: {eta:.1f}s" if eta > 0 else ""
    else:
        eta_str = ""
    
    percentage = (current / total * 100) if total > 0 else 0
    print(f"\r🔄 Progresso: {current}/{total} ({percentage:.1f}%) - {elapsed:.1f}s{eta_str}", end="", flush=True)

def esaminaDirectory(dirPath: str):
    """Funzione principale per analizzare una directory"""
    if not os.path.isdir(dirPath):
        raise RuntimeError(f"Errore: '{dirPath}' non è una directory valida!")

    # Rileva numero di CPU disponibili
    numJobs = os.cpu_count()
    if numJobs is None:
        numJobs = 1
        print("⚠️ Impossibile determinare numero CPU, uso 1 processo")
    else:
        numJobs = max(numJobs - 2, 1)  # Lascia qualche CPU libera
    
    print(f"🔍 Scansione directory: {dirPath}")
    listaFiles = elencaFiles(dirPath)
    numFiles = len(listaFiles)
    
    if numFiles == 0:
        print("⚠️ Nessun file trovato nella directory")
        return
    
    print(f"📊 Trovati {numFiles} file")
    print(f"🚀 Usando {numJobs} processi paralleli")
    print(f"🔧 Comando hash: {' '.join(detect_hash_command())}")
    
    # ✅ USA LA CONFIGURAZIONE PER DETERMINARE IL NOME FILE
    outputFile = determine_output_filename(dirPath)
    
    # Controlla se esiste già e chiedi conferma (solo in modalità interattiva)
    if os.path.exists(outputFile) and sys.stdout.isatty():
        try:
            response = input(f"⚠️ File {outputFile} già esistente. Sovrascrivere? (s/N): ")
            if response.lower() not in ['s', 'si', 'y', 'yes']:
                print("❌ Operazione annullata")
                return
        except (EOFError, KeyboardInterrupt):
            print("\n❌ Operazione annullata")
            return
    
    # Apri file di output
    with open(outputFile, 'w', encoding='utf-8') as fout:
        # Intestazione JSON strutturata
        fout.write('{\n')
        fout.write(f'  "directory_analizzata": "{dirPath}",\n')
        fout.write(f'  "modalita_ricorsiva": true,\n')
        fout.write(f'  "esclusioni": "",\n')
        fout.write(f'  "data_generazione": "{adesso()}",\n')
        fout.write(f'  "totale_file": {numFiles},\n')
        fout.write(f'  "hash_command": "{" ".join(detect_hash_command())}",\n')
        fout.write(f'  "platform": "{platform.system()}",\n')
        fout.write(f'  "file_hashes": [\n')

        errors_count = 0
        processed_count = 0
        start_time = time.time()
        last_progress_time = start_time
        
        try:
            print("🔄 Calcolo hash in corso...")
            
            with concurrent.futures.ProcessPoolExecutor(max_workers=numJobs) as executor:
                # Sottometti tutti i job
                future_to_info = {executor.submit(esaminaFile, file_info): file_info for file_info in listaFiles}
                
                # Processa i risultati man mano che arrivano
                for future in concurrent.futures.as_completed(future_to_info):
                    result = future.result()
                    processed_count += 1
                    
                    # Controlla se c'è stato un errore
                    if "error" in result:
                        errors_count += 1
                        if errors_count <= 5:  # Mostra solo i primi 5 errori
                            print(f"\n⚠️ Errore: {result['path']}: {result['error']}")
                        elif errors_count == 6:
                            print(f"\n⚠️ Altri errori soppressi (totale: {errors_count})...")
                    
                    # Scrivi risultato nel JSON
                    fout.write("    " + json.dumps(result, ensure_ascii=False, separators=(',', ': ')))
                    if processed_count < numFiles:
                        fout.write(",\n")
                    else:
                        fout.write("\n")
                    
                    # Mostra progresso ogni 10 file o ogni 3 secondi
                    current_time = time.time()
                    if processed_count % 10 == 0 or (current_time - last_progress_time) >= 3:
                        show_progress(processed_count, numFiles, start_time)
                        last_progress_time = current_time
                        
        except KeyboardInterrupt:
            print("\n⚠️ Processo interrotto manualmente!")
            print(f"📊 File elaborati prima dell'interruzione: {processed_count}/{numFiles}")
            return
        except Exception as e:
            print(f"\n❌ Errore durante l'elaborazione: {e}")
            return

        # Mostra progresso finale
        print(f"\r✅ Completato: {processed_count}/{numFiles} (100.0%)")

        # Chiusura JSON con statistiche
        fout.write('  ],\n')
        fout.write(f'  "statistiche": {{\n')
        fout.write(f'    "file_elaborati": {numFiles},\n')
        fout.write(f'    "errori": {errors_count},\n')
        fout.write(f'    "successi": {numFiles - errors_count}\n')
        fout.write(f'  }}\n')
        fout.write('}\n')

    # Report finale
    file_size = os.path.getsize(outputFile)
    print(f"\n✅ File JSON scritto: {outputFile}")
    print(f"📏 Dimensione: {file_size:,} bytes")
    print(f"📊 File elaborati: {numFiles}")
    print(f"✅ Successi: {numFiles - errors_count}")
    if errors_count > 0:
        print(f"⚠️ Errori: {errors_count}")
    print("🎉 Elaborazione completata!")

def test_hash_command():
    """Testa se il comando hash funziona"""
    hash_cmd = detect_hash_command()
    print(f"🧪 Test comando hash: {' '.join(hash_cmd)}")
    
    try:
        # Crea un file temporaneo per test
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_file.write("test")
            temp_path = temp_file.name
        
        # Testa il comando
        result = calcHash(temp_path)
        print(f"✅ Comando hash funziona. Hash di test: {result[:16]}...")
        
        # Pulisci file temporaneo
        os.unlink(temp_path)
        return True
        
    except Exception as e:
        print(f"❌ Comando hash non funziona: {e}")
        return False

def show_usage():
    """Mostra l'uso corretto dello script"""
    print("📋 USO CORRETTO:")
    print(f"  {sys.argv[0]} <directory>")
    print()
    print("📁 DIRECTORY RICONOSCIUTE:")
    for dir_key, config in DIRECTORY_CONFIGS.items():
        print(f"  {config['description']}:")
        print(f"    Path: {config['path']}")
        print(f"    Output: {config['hash_output']}")
        print()
    print("💡 ESEMPIO:")


def main():
    """Entry point principale"""
    if len(sys.argv) < 2:
        print("❌ Errore: specificare la directory da analizzare.")
        show_usage()
        sys.exit(1)
    
    # Test preliminare
    if not test_hash_command():
        print("❌ Comando hash non disponibile sul sistema")
        if platform.system() == "Darwin":
            print("💡 Su macOS, 'shasum' dovrebbe essere disponibile di default")
        else:
            print("💡 Su Linux, installa 'coreutils': sudo apt-get install coreutils")
        sys.exit(1)
    
    directory = sys.argv[1]
    
    # Verifica che la directory esista
    if not os.path.isdir(directory):
        print(f"❌ Errore: '{directory}' non è una directory valida")
        sys.exit(1)
    
    print(f"🚀 Avvio analisi hash per: {directory}")
    print(f"⚙️ Piattaforma: {platform.system()}")
    print("")
    
    try:
        esaminaDirectory(directory)
    except KeyboardInterrupt:
        print("\n⚠️ Processo interrotto dall'utente")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Errore fatale: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()