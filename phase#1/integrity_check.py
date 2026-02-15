import json
import sys
import os
import argparse
import requests
from pathlib import Path
import urllib.parse
import requests

# === IMPORT CONFIGURAZIONE CENTRALIZZATA ===
try:
    from config_loader import load_config, ConfigError
    USE_CENTRALIZED_CONFIG = True
    print("✅ Configurazione centralizzata caricata - integrity_check.py:14")
except ImportError:
    USE_CENTRALIZED_CONFIG = False
    ConfigError = Exception  # Fallback
    load_config = None  # Fallback
    print("⚠️ Configurazione centralizzata non disponibile, uso configurazione locale - integrity_check.py:19")


# ... (import e altre funzioni) ...

def get_device_configs():
    """Ottiene le configurazioni dei device per integrity check"""
    if USE_CENTRALIZED_CONFIG and load_config is not None:
        try:
            config = load_config()
            device_configs = {}
            
            for dir_key, dir_config in config.get_directories().items():
                device_configs[dir_key] = {
                    'json_file': dir_config['files']['hash_output'],
                    'root_id': dir_config['structure']['root_id'], # <-- Assicurati che questo sia corretto
                    'base_path': dir_config['base_path_normalized'],
                    'description': dir_config['description']
                }
            return device_configs
        except ConfigError:
            print("⚠️ Errore configurazione centralizzata, uso fallback locale - integrity_check.py:40")
            pass
    
    # Configurazione fallback aggiornata con path corretti
    # Questa parte dovrebbe essere allineata con i nomi dei file nel directory_config.json
    # per garantire coerenza anche in caso di fallback.
    return {
        'floppy': {
            'json_file': 'FloppyDisks_HASH.json', # Questo dovrebbe corrispondere a directory_config.json
            'base_path': '/media/sdb1/evangelisti/data/FloppyDisks/',
            'description': 'Floppy Disks',
            'root_id': 'RS1_RS3'
        },
        'hd': {
            'json_file': 'HD_HASH.json', # Questo dovrebbe corrispondere
            'base_path': '/media/sdb1/evangelisti/data/HardDiskValerio/',
            'description': 'Hard Disk',
            'root_id': 'RS1_RS1',
        },
        'hdesterno': {
            'json_file': 'HDEsterno_HASH.json', # Questo dovrebbe corrispondere
            'base_path': '/media/sdb1/evangelisti/data/HDEsternoEvangelisti/',
            'description': 'HD Esterno Evangelisti',
            'root_id': 'RS1_RS2'
        }
    }

def get_graph_uri_for_device(root_id):
    return f"http://ficlit.unibo.it/ArchivioEvangelisti/structure/{root_id}"

def normalize_path(path):
    """Normalizza i path per il confronto cross-platform"""
    return path.replace('\\', '/').strip()

def main():
    # Usa la configurazione dinamica
    device_configs = get_device_configs()
    
    # Parsing degli argomenti
    if len(sys.argv) < 2:
        print("Uso: python correct_ <tipo_dispositivo> [debug] - integrity_check.py:80")
        print("Tipi disponibili: - integrity_check.py:81", ", ".join(device_configs.keys()))
        sys.exit(1)
    
    device_type = sys.argv[1].lower()
    debug_mode = '--debug' in sys.argv
    
    if device_type not in device_configs:
        print(f"Tipo dispositivo non riconosciuto: {device_type} - integrity_check.py:88")
        print("Tipi disponibili: - integrity_check.py:89", ", ".join(device_configs.keys()))
        sys.exit(1)
    
    config = device_configs[device_type]
    endpoint_url = "http://localhost:9999/blazegraph/namespace/kb/sparql"  # Usa localhost
    

def normalize_path(path):
    """Normalizza i path per il confronto cross-platform"""
    return path.replace('\\', '/').strip()

def query_sparql_endpoint(endpoint_url, query, timeout=1000):
    """
    Esegue query SPARQL usando requests invece di SPARQLWrapper
    """
    try:
        print(f"🔍 Query SPARQL endpoint: {endpoint_url} - integrity_check.py:105")
        
        headers = {
            'Accept': 'application/sparql-results+json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {
            'query': query,
            'format': 'json'
        }
        
        response = requests.post(
            endpoint_url,
            data=data,
            headers=headers,
            timeout=timeout
        )
        
        response.raise_for_status()
        
        result = response.json()
        print(f"✅ Query completata  Risultati: {len(result.get('results', {}).get('bindings', []))} - integrity_check.py:127")
        return result
        
    except requests.exceptions.Timeout:
        print(f"❌ Timeout connessione dopo {timeout} secondi - integrity_check.py:131")
        raise
    except requests.exceptions.ConnectionError:
        print(f"❌ Impossibile connettersi a {endpoint_url} - integrity_check.py:134")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"❌ Errore HTTP {e.response.status_code}: {e.response.text} - integrity_check.py:137")
        raise
    except json.JSONDecodeError:
        print(f"❌ Risposta non è JSON valido - integrity_check.py:140")
        print(f"Risposta ricevuta: {response.text[:500]} - integrity_check.py:141")
        raise
    except Exception as e:
        print(f"❌ Errore generico: {e} - integrity_check.py:144")
        raise

def get_blazegraph_endpoint(blazegraph_journal=None, blazegraph_config=None):
    """
    Determina l'endpoint Blazegraph da utilizzare.
    AGGIORNATO: Priorità all'endpoint locale funzionante
    """
    # Lista endpoint da testare in ordine di priorità
    endpoints_to_test = [
        "http://localhost:9999/blazegraph/namespace/kb/sparql",
        "http://127.0.0.1:9999/blazegraph/namespace/kb/sparql",
        "http://10.200.10.104:9999/blazegraph/namespace/kb/sparql"  # Endpoint remoto come fallback
    ]
    
    print("🔍 Ricerca endpoint Blazegraph funzionante... - integrity_check.py:159")
    
    for endpoint in endpoints_to_test:
        try:
            print(f"🧪 Test: {endpoint} - integrity_check.py:163")
            
            # Test rapido di connessione
            import requests
            test_query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
            headers = {
                'Accept': 'application/sparql-results+json',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            data = {'query': test_query, 'format': 'json'}
            
            response = requests.post(endpoint, data=data, headers=headers, timeout=1000)
            
            if response.status_code == 200:
                result = response.json()
                count = int(result["results"]["bindings"][0]["count"]["value"])
                print(f"✅ FUNZIONA  {count:,} triple - integrity_check.py:179")
                print(f"🎯 Endpoint selezionato: {endpoint} - integrity_check.py:180")
                return endpoint
            else:
                print(f"❌ HTTP {response.status_code} - integrity_check.py:183")
                
        except requests.exceptions.ConnectionError:
            print(f"❌ Connessione rifiutata - integrity_check.py:186")
        except requests.exceptions.Timeout:
            print(f"❌ Timeout - integrity_check.py:188")
        except Exception as e:
            print(f"❌ Errore: {str(e)[:50]} - integrity_check.py:190")
    
    # Se nessun endpoint funziona, usa quello locale come default
    default_endpoint = "http://localhost:9999/blazegraph/namespace/kb/sparql"
    print(f"⚠️ Nessun endpoint risponde, uso default: {default_endpoint} - integrity_check.py:194")
    print("Assicurati che Blazegraph sia in esecuzione: java jar blazegraph.jar - integrity_check.py:195")
    return default_endpoint

def test_blazegraph_connection(endpoint_url):
    """
    Testa la connessione a Blazegraph
    """
    test_query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
    
    try:
        result = query_sparql_endpoint(endpoint_url, test_query, timeout=1000)
        count = int(result["results"]["bindings"][0]["count"]["value"])
        print(f"✅ Connessione Blazegraph OK  Triple totali: {count:,} - integrity_check.py:207")
        return True
    except Exception as e:
        print(f"❌ Test connessione fallito: {e} - integrity_check.py:210")
        return False

def print_problematic_files(missing_in_sparql, extra_in_sparql, hash_mismatches, corrupted_files_json, max_files=20):
    """
    Stampa tutti i file problematici in modo organizzato
    """
    print("\n - integrity_check.py:217" + "="*80)
    print("🚨 DETTAGLIO FILE PROBLEMATICI - integrity_check.py:218")
    print("= - integrity_check.py:219"*80)
    
    # 1. FILE CON ERRORI NEL JSON
    if corrupted_files_json:
        print(f"\n📄 FILE CON ERRORI NEL JSON ({len(corrupted_files_json)} file): - integrity_check.py:223")
        print("" * 50)
        for i, file_info in enumerate(corrupted_files_json[:max_files], 1):
            print(f"{i:3d}. {file_info['path']} - integrity_check.py:226")
            print(f"Errore: {file_info.get('error', 'Errore sconosciuto')} - integrity_check.py:227")
            if 'size' in file_info:
                print(f"Dimensione: {file_info['size']} bytes - integrity_check.py:229")
        if len(corrupted_files_json) > max_files:
            print(f"... e altri {len(corrupted_files_json) -  max_files} file - integrity_check.py:231")
    
    # 2. FILE MANCANTI IN SPARQL
    if missing_in_sparql:
        print(f"\n🔍 FILE MANCANTI IN SPARQL ({len(missing_in_sparql)} file): - integrity_check.py:235")
        print("" * 50)
        for i, path in enumerate(sorted(missing_in_sparql)[:max_files], 1):
            print(f"{i:3d}. {path} - integrity_check.py:238")
        if len(missing_in_sparql) > max_files:
            print(f"... e altri {len(missing_in_sparql) - max_files} file - integrity_check.py:240")
    
    # 3. FILE EXTRA IN SPARQL
    if extra_in_sparql:
        print(f"\n📊 FILE EXTRA IN SPARQL ({len(extra_in_sparql)} file): - integrity_check.py:244")
        print("" * 50)
        for i, path in enumerate(sorted(extra_in_sparql)[:max_files], 1):
            print(f"{i:3d}. {path} - integrity_check.py:247")
        if len(extra_in_sparql) > max_files:
            print(f"... e altri {(len(extra_in_sparql)  - max_files)} file - integrity_check.py:249")
            
    # 4. FILE CON HASH CORROTTI
    if hash_mismatches:
        print(f"\n🔒 FILE CON HASH CORROTTI ({len(hash_mismatches)} file): - integrity_check.py:253")
        print("" * 50)
        for i, (path, json_hash, sparql_hash) in enumerate(hash_mismatches[:max_files], 1):
            print(f"{i:3d}. {path} - integrity_check.py:256")
            print(f"JSON Hash:   {json_hash} - integrity_check.py:257")
            print(f"SPARQL Hash: {sparql_hash} - integrity_check.py:258")
            print(f"Differenza:  {'✅' if json_hash == sparql_hash else '❌'} - integrity_check.py:259")
        if len(hash_mismatches) > max_files:
            print(f"... e altri {len(hash_mismatches)  -  max_files} file - integrity_check.py:261")
    
    # 5. RIEPILOGO PROBLEMI
    total_problems = len(corrupted_files_json) + len(missing_in_sparql) + len(extra_in_sparql) + len(hash_mismatches)
    print(f"\n📊 RIEPILOGO PROBLEMI: - integrity_check.py:265")
    print(f"File con errori JSON: {len(corrupted_files_json)} - integrity_check.py:266")
    print(f"File mancanti in SPARQL: {len(missing_in_sparql)} - integrity_check.py:267")
    print(f"File extra in SPARQL: {len(extra_in_sparql)} - integrity_check.py:268")
    print(f"File con hash corrotti: {len(hash_mismatches)} - integrity_check.py:269")
    print(f"TOTALE PROBLEMI: {total_problems} - integrity_check.py:270")

def verify_integrity_blazegraph(device_type, blazegraph_journal=None, blazegraph_config=None, debug_mode=False):
    """Funzione principale di verifica integrità via Blazegraph"""
    
    # Ottieni la configurazione del dispositivo usando la funzione centralizzata/fallback
    # Questa è la modifica chiave: non usare più la configurazione hardcoded qui.
    all_device_configs = get_device_configs() 
    
    if device_type not in all_device_configs:
        print(f"❌ Tipo dispositivo non riconosciuto: {device_type} - integrity_check.py:280")
        print(f"Tipi disponibili: {', '.join(all_device_configs.keys())} - integrity_check.py:281")
        return False
    
    config = all_device_configs[device_type] # Usa la configurazione ottenuta
    endpoint_url = get_blazegraph_endpoint(blazegraph_journal, blazegraph_config)
    
    print(f"=== VERIFICA INTEGRITÀ CORRETTA  {config['description'].upper()} === - integrity_check.py:287")
    print(f"📄 File JSON: {config['json_file']} - integrity_check.py:288") # Questo userà il nome dal config
    print(f"📂 Base path: {config['base_path']} - integrity_check.py:289")
    print(f"🔗 Endpoint SPARQL: {endpoint_url} - integrity_check.py:290")
    print()
    
    # Test connessione
    if not test_blazegraph_connection(endpoint_url):
        return False
    
    # 1. CARICAMENTO DATI JSON
    print("📂 1. Caricamento dati JSON... - integrity_check.py:298")
    try:
        json_file_path = Path(config['json_file'])
        if not json_file_path.exists():
            print(f"❌ File JSON non trovato: {config['json_file']} - integrity_check.py:302")
            return False
            
        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Errore: File JSON non valido: {e} - integrity_check.py:308")
        return False
    except Exception as e:
        print(f"❌ Errore nel caricamento JSON: {e} - integrity_check.py:311")
        return False
    
    if "file_hashes" not in json_data:
        print(f"❌ Errore: Struttura JSON non valida  manca 'file_hashes' - integrity_check.py:315")
        return False
    
    # Prepara mappa JSON: path_completo -> hash
    json_map = {}
    corrupted_files_json = []  # NUOVA LISTA PER FILE CORROTTI
    
    for item in json_data["file_hashes"]:
    # Salta i file .DS_Store
        if item.get("path", "").endswith(".DS_Store"):
            continue
        if "error" in item or item.get('sha256') is None:
            corrupted_files_json.append({
                'path': item.get('path', 'unknown'),
                'error': item.get('error', 'SHA256 mancante'),
                'size': item.get('size', 0)
                })
            continue
        
        # Path completo normalizzato
        full_path = normalize_path(item['path'])
        hash_value = item['sha256'].lower().strip()
        
        json_map[full_path] = {
            'hash': hash_value,
            'size': item.get('size', 0),
            'modified': item.get('modified', 'unknown')
        }
        
        if debug_mode and len(json_map) <= 5:
            print(f"🔍 JSON: {full_path} > {hash_value[:16]}... - integrity_check.py:345")
    
    print(f"✅ Caricati {len(json_map)} file validi dal JSON - integrity_check.py:347")
    if corrupted_files_json:
        print(f"❌ Trovati {len(corrupted_files_json)} file con errori nel JSON - integrity_check.py:349")
    
    # 2. QUERY SPARQL CON FILTRO BASE PATH
    print(f"🔍 2. Esecuzione query SPARQL con filtro base path... - integrity_check.py:352")
    
    # Normalizza base path per la query
    base_path_norm = normalize_path(config['base_path'])
    base_path_filter = base_path_norm.rstrip('/')
    



    graph_uri = get_graph_uri_for_device(config['root_id'])

    query = f"""
    PREFIX bodi: <http://w3id.org/bodi#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT DISTINCT ?relative_path ?hash
    WHERE {{
    GRAPH <{graph_uri}> {{
        ?inst prov:atLocation ?loc .
        ?loc rdfs:label ?relative_path .
        ?inst bodi:hasHashCode ?fixity .
        ?fixity rdf:value ?hash .
        FILTER(?relative_path != "")
    }}
    }}
    """

    
    
    if debug_mode:
        print(f"🔍 Query SPARQL: - integrity_check.py:383")
        print(f"Base path per filtro: {base_path_filter} - integrity_check.py:384")
        print(f"Base path per ricostruzione: {base_path_norm} - integrity_check.py:385")
    
    try:
        results = query_sparql_endpoint(endpoint_url, query, timeout=1000)
    except Exception as e:
        print(f"❌ Errore nella query SPARQL: {e} - integrity_check.py:390")
        return False
    
    # 3. ELABORAZIONE RISULTATI SPARQL
    print(f"🔧 3. Elaborazione risultati SPARQL... - integrity_check.py:394")
    
    sparql_map = {}
    total_sparql_records = 0
    
    for i, result in enumerate(results["results"]["bindings"], start=1):
        total_sparql_records += 1
        relative_path = normalize_path(result["relative_path"]["value"]).strip()
        hash_val = result["hash"]["value"].strip().lower()
        if relative_path.endswith(".DS_Store"):
            continue
        # Stampa solo i primi 5 risultati in debug mode
        if debug_mode and i <= 5:
            print(f"{i:2d}. Path: {relative_path} - integrity_check.py:407")
            print(f"Hash: {hash_val[:16]}... - integrity_check.py:408")
        
        if not relative_path:
            continue
        
        # RICOSTRUISCI PATH COMPLETO: base_path + relative_path
        base_path_norm = normalize_path(config['base_path']).rstrip('/')
        if relative_path.startswith('/'):
            relative_path = relative_path[1:]
        full_path = base_path_norm + "/" + relative_path
        sparql_map[normalize_path(full_path)] = hash_val
    
    print(f"📊 Record SPARQL processati: {total_sparql_records} - integrity_check.py:420")
    print(f"📊 Path SPARQL validi: {len(sparql_map)} - integrity_check.py:421")
    
    # 4. CONFRONTO DIRETTO PATH COMPLETI
    print(f"⚖️ 4. Confronto diretto path completi... - integrity_check.py:424")

    json_paths = set(json_map.keys())
    sparql_paths = set(sparql_map.keys())

    if debug_mode:
        print(f"📊 Path JSON (totale): {len(json_paths)} - integrity_check.py:430")
        for path in sorted(json_paths)[:3]:
            print(f"JSON Path: {path} - integrity_check.py:432")

        print(f"📊 Path SPARQL (totale): {len(sparql_paths)} - integrity_check.py:434")
        for path in sorted(sparql_paths)[:3]:
            print(f"SPARQL Path: {path} - integrity_check.py:436")

    # Match esatti sui path completi
    exact_matches = json_paths & sparql_paths
    missing_in_sparql = json_paths - sparql_paths
    extra_in_sparql = sparql_paths - json_paths

    print(f"✅ Match esatti sui path: {len(exact_matches)} - integrity_check.py:443")
    print(f"⚠️ File in JSON ma non in SPARQL: {len(missing_in_sparql)} - integrity_check.py:444")
    print(f"⚠️ File in SPARQL ma non in JSON: {len(extra_in_sparql)} - integrity_check.py:445")

    if debug_mode:
        for path in sorted(exact_matches)[:3]:
            print(f"Match: {path} - integrity_check.py:449")
        for path in sorted(missing_in_sparql)[:3]:
            print(f"Mancante in SPARQL: {path} - integrity_check.py:451")
        for path in sorted(extra_in_sparql)[:3]:
            print(f"Extra in SPARQL: {path} - integrity_check.py:453")
    
    # 5. VERIFICA HASH PER I MATCH ESATTI
    print(f"🔒 5. Verifica integrità hash... - integrity_check.py:456")
    
    hash_matches = 0
    hash_mismatches = []
    
    for path in exact_matches:
        json_hash = json_map[path]['hash']
        sparql_hash = sparql_map[path]
        
        if json_hash == sparql_hash:
            hash_matches += 1
        else:
            hash_mismatches.append((path, json_hash, sparql_hash))
            if debug_mode:
                print(f"❌ HASH MISMATCH: {path} - integrity_check.py:470")
                print(f"JSON:   {json_hash} - integrity_check.py:471")
                print(f"SPARQL: {sparql_hash} - integrity_check.py:472")
    
    print(f"✅ Hash corrispondenti: {hash_matches} - integrity_check.py:474")
    print(f"❌ Hash diversi: {len(hash_mismatches)} - integrity_check.py:475")
    
    # 6. STAMPA DETTAGLIATA DEI FILE PROBLEMATICI
    if corrupted_files_json or missing_in_sparql or extra_in_sparql or hash_mismatches:
        print_problematic_files(
            missing_in_sparql, 
            extra_in_sparql, 
            hash_mismatches, 
            corrupted_files_json,
            max_files=50  # Mostra fino a 50 file per categoria
        )
    
    # 7. REPORT FINALE
    print()
    print("= - integrity_check.py:489"*80)
    print("📊 REPORT FINALE - integrity_check.py:490")
    print("= - integrity_check.py:491"*80)
    print(f"Dispositivo: {config['description']} - integrity_check.py:492")
    print(f"Base path: {config['base_path']} - integrity_check.py:493")
    print(""*80)
    print(f"📂 File totali in JSON: {len(json_map)} - integrity_check.py:495")
    print(f"🗄️ File totali in SPARQL: {len(sparql_map)} - integrity_check.py:496")
    print(f"🎯 Path che corrispondono: {len(exact_matches)} - integrity_check.py:497")
    print(f"✅ Hash integri: {hash_matches} - integrity_check.py:498")
    print(f"❌ Hash corrotti: {len(hash_mismatches)} - integrity_check.py:499")
    print(f"⚠️ File mancanti in SPARQL: {len(missing_in_sparql)} - integrity_check.py:500")
    print(f"⚠️ File extra in SPARQL: {len(extra_in_sparql)} - integrity_check.py:501")
    print(f"🚨 File con errori nel JSON: {len(corrupted_files_json)} - integrity_check.py:502")
    
    # Calcola percentuali
    if len(json_map) > 0:
        path_match_rate = (len(exact_matches) / len(json_map)) * 100
        print(f"📈 Tasso di matching path: {path_match_rate:.2f}% - integrity_check.py:507")
    
    if len(exact_matches) > 0:
        integrity_rate = (hash_matches / len(exact_matches)) * 100
        print(f"🔒 Tasso di integrità hash: {integrity_rate:.2f}% - integrity_check.py:511")
    
    # 8. CONCLUSIONI per la pipeline
    print(f"\n🏁 CONCLUSIONI - integrity_check.py:514")
    print("= - integrity_check.py:515"*40)
    
    success = False
    
    if len(hash_mismatches) == 0 and len(exact_matches) == len(json_map):
        print("🎉 PERFETTO: 100% dei file hanno hash corrispondenti! - integrity_check.py:520")
        print("✅ TUTTI I FILE CORRISPONDONO CON GLI HASH! - integrity_check.py:521")
        print("Sistema perfettamente sincronizzato! - integrity_check.py:522")
        print("All hashes verified via SPARQL - integrity_check.py:523")
        success = True
    elif len(hash_mismatches) == 0:
        print("✅ BUONO: Nessun hash corrotto, ma alcuni file non trovati in SPARQL - integrity_check.py:526")
        print(f"📋 Azione richiesta: Importare {len(missing_in_sparql)} file mancanti - integrity_check.py:527")
        success = True
    else:
        print("❌ ATTENZIONE: Rilevati hash corrotti! - integrity_check.py:530")
        print("Hash mismatch detected - integrity_check.py:531")
        print("Integrity check failed - integrity_check.py:532")
        print(f"🚨 Azione urgente: Verificare {len(hash_mismatches)} file corrotti - integrity_check.py:533")
        success = False
    
    # Messaggi aggiuntivi per la pipeline
    if success:
        print("✅ Blazegraph integrity check passed - integrity_check.py:538")
        print("SPARQL query successful - integrity_check.py:539")
        print(f"📊 SPARQL verified: {hash_matches} files - integrity_check.py:540")
    else:
        print("❌ Blazegraph integrity check failed - integrity_check.py:542")
        print(f"📊 Blazegraph errors: {len(hash_mismatches)} - integrity_check.py:543")
    
    return success

def main():
    """Funzione principale con parsing argomenti"""
    parser = argparse.ArgumentParser(
        description="Verifica integrità hash tramite query SPARQL a Blazegraph",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi di utilizzo:

  # Modalità tradizionale (endpoint remoto)
  python integrity_check.py floppy
  
  # Modalità pipeline con journal locale
  python integrity_check.py floppy --blazegraph-journal blazegraph_journal/blazegraph.jnl --blazegraph-config blazegraph_journal/RWStore.properties
  
  # Con debug dettagliato
  python integrity_check.py floppy --debug
  
Tipi dispositivo supportati: floppy, hd, hdesterno
"""
    )
    
    parser.add_argument(
        'device_type',
        choices=['floppy', 'hd', 'hdesterno'],
        help='Tipo di dispositivo da verificare'
    )
    
    parser.add_argument(
        '--blazegraph-journal',
        help='Path al journal Blazegraph (es: blazegraph_journal/blazegraph.jnl)'
    )
    
    parser.add_argument(
        '--blazegraph-config', 
        help='Path alla configurazione Blazegraph (es: blazegraph_journal/RWStore.properties)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Abilita output debug dettagliato'
    )
    
    args = parser.parse_args()
    
    # Verifica coerenza parametri
    if args.blazegraph_journal and not args.blazegraph_config:
        print("❌ Errore: blazegraphconfig richiesto quando si usa blazegraphjournal - integrity_check.py:594")
        sys.exit(1)
    
    if args.blazegraph_config and not args.blazegraph_journal:
        print("❌ Errore: blazegraphjournal richiesto quando si usa blazegraphconfig - integrity_check.py:598")
        sys.exit(1)
    
    # Esegui verifica
    try:
        success = verify_integrity_blazegraph(
            args.device_type,
            args.blazegraph_journal,
            args.blazegraph_config,
            args.debug
        )
        
        # Codici di uscita:
        # 0 = successo completo
        # 1 = successo parziale (hash OK ma file mancanti)
        # 2 = fallimento (hash corrotti)
        if success:
            sys.exit(0)
        else:
            sys.exit(2)
        
    except KeyboardInterrupt:
        print("\n⚠️ Operazione interrotta dall'utente - integrity_check.py:620")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Errore fatale: {e} - integrity_check.py:623")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()