import json
import sys
import os
import argparse
from collections import defaultdict
from SPARQLWrapper import SPARQLWrapper, JSON
from pathlib import Path
import requests

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

def get_device_configs():
    """Ottiene le configurazioni dei device"""
    if USE_CENTRALIZED_CONFIG and load_config is not None:
        try:
            config = load_config()
            device_configs = {}
            
            for dir_key, dir_config in config.get_directories().items():
                json_file = dir_config['files']['count_output']
                root_id = dir_config['structure']['root_id']
                print(f"🔍 File JSON ricercato per {dir_key}: {json_file}")
                print(f"🆔 Root ID per {dir_key}: {root_id}")
                device_configs[dir_key] = {
                    'json_file': json_file,
                    'base_path': dir_config['base_path_normalized'],
                    'description': dir_config['description'],
                    'root_id': root_id
                }
            return device_configs
        except ConfigError:
            print("⚠️ Errore configurazione centralizzata, uso fallback locale")
            pass
    
    # Configurazione fallback aggiornata con path corretti e root_id
    return {
        'floppy': {
            'json_file': 'FloppyDisks_CNT.json',
            'base_path': '/media/sdb1/evangelisti/data/FloppyDisks/',
            'description': 'Floppy Disks',
            'root_id': 'RS1_RS3'
        },
        'hd': {
            'json_file': 'HD_CNT.json',
            'base_path': '/media/sdb1/evangelisti/data/HardDiskValerio/',
            'description': 'Hard Disk',
            'root_id': 'RS1_RS1'
        },
        'hdesterno': {
            'json_file': 'HDEsterno_CNT.json',
            'base_path': '/media/sdb1/evangelisti/data/HDEsternoEvangelisti/',
            'description': 'HD Esterno Evangelisti',
            'root_id': 'RS1_RS2'
        }
    }

def normalize_path(path, base_path):
    """Normalizza un path rispetto al base_path"""
    base_path = base_path.rstrip('/')
    
    if path.startswith(base_path):
        return path.rstrip('/')
    
    if path.startswith('/'):
        return (base_path + path).rstrip('/')
    else:
        return (base_path + '/' + path).rstrip('/')

def is_true_subpath(record_path, recordset_path):
    """Verifica se record_path è un vero sotto-path di recordset_path"""
    if record_path == recordset_path:
        return True
    if not record_path.startswith(recordset_path):
        return False
    remainder = record_path[len(recordset_path):]
    if not remainder:
        return True
    if remainder.startswith('/'):
        return True
    return False

def get_blazegraph_endpoint(blazegraph_journal=None, blazegraph_config=None):
    """Determina l'endpoint Blazegraph da utilizzare"""
    endpoints_to_test = [
        "http://localhost:9999/blazegraph/namespace/kb/sparql",
        "http://127.0.0.1:9999/blazegraph/namespace/kb/sparql",
        "http://10.200.10.104:9999/blazegraph/namespace/kb/sparql"
    ]
    
    print("🔍 Ricerca endpoint Blazegraph funzionante...")
    
    for endpoint in endpoints_to_test:
        try:
            print(f"🧪 Test: {endpoint}")
            
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
                print(f"  ✅ FUNZIONA - {count:,} triple")
                print(f"🎯 Endpoint selezionato: {endpoint}")
                return endpoint
            else:
                print(f"  ❌ HTTP {response.status_code}")
                
        except requests.exceptions.ConnectionError:
            print(f"  ❌ Connessione rifiutata")
        except requests.exceptions.Timeout:
            print(f"  ❌ Timeout")
        except Exception as e:
            print(f"  ❌ Errore: {str(e)[:50]}")
    
    default_endpoint = "http://localhost:9999/blazegraph/namespace/kb/sparql"
    print(f"⚠️ Nessun endpoint risponde, uso default: {default_endpoint}")
    print("   Assicurati che Blazegraph sia in esecuzione: java -jar blazegraph.jar")
    return default_endpoint

def get_graph_uri_for_device(root_id):
    """Costruisce l'URI del grafo specifico per il device"""
    return f"http://ficlit.unibo.it/ArchivioEvangelisti/structure/{root_id}"

def verify_graph_exists(endpoint, graph_uri):
    """Verifica se il grafo esiste e contiene dati"""
    query = f"""
    SELECT (COUNT(*) as ?count) WHERE {{
        GRAPH <{graph_uri}> {{
            ?s ?p ?o
        }}
    }}
    """
    
    try:
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        
        count = int(result["results"]["bindings"][0]["count"]["value"])
        return count > 0, count
    except Exception as e:
        print(f"❌ Errore verifica grafo: {e}")
        return False, 0

def verify_counts_blazegraph(device_type, blazegraph_journal=None, blazegraph_config=None):
    """Funzione principale di verifica conteggi via Blazegraph"""
    
    all_device_configs = get_device_configs() 
    
    if device_type not in all_device_configs:
        print(f"❌ Tipo dispositivo non riconosciuto: {device_type}")
        print(f"Tipi disponibili: {', '.join(all_device_configs.keys())}")
        return False
    
    config = all_device_configs[device_type]
    root_id = config['root_id']
    graph_uri = get_graph_uri_for_device(root_id)
    
    endpoint_url = get_blazegraph_endpoint(blazegraph_journal, blazegraph_config)
    print(f"\n=== Controllo conteggi da Blazegraph per {config['description']} ===")
    print(f"🔗 Endpoint: {endpoint_url}")
    print(f"🆔 Root ID: {root_id}")
    print(f"📊 Grafo: {graph_uri}")

    # Carica dati JSON di riferimento
    try:
        json_file_path = Path(config['json_file'])
        print(f"🔍 File JSON in lettura: {json_file_path}")
        if not json_file_path.exists():
            print(f"❌ File JSON non trovato: {config['json_file']}")
            return False
            
        with open(json_file_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
    except Exception as e:
        print(f"❌ Errore nel caricamento JSON: {e}")
        return False

    base_path = config['base_path'].rstrip('/')
    print(f"📂 Path base normalizzato: '{base_path}'")
    
    # Prepara i conteggi attesi
    attesi = {}
    if "conteggio_totale" in json_data:
        attesi[base_path] = json_data["conteggio_totale"]
    
    for item in json_data.get("sottocartelle", []):
        full_path = normalize_path(item["path"], base_path)
        attesi[full_path] = item["file_count"]
    
    print(f"📊 Totale directory attese: {len(attesi)}")
    print(f"📊 Conteggi attesi: {dict(list(attesi.items())[:5])}{'...' if len(attesi) > 5 else ''}")

    # Inizializza SPARQL endpoint
    try:
        endpoint = SPARQLWrapper(endpoint_url)
        endpoint.setReturnFormat(JSON)
        
        # Test connessione
        test_query = "SELECT (COUNT(*) as ?count) WHERE { GRAPH ?g { ?s ?p ?o } }"
        endpoint.setQuery(test_query)
        test_result = endpoint.query().convert()
        total_triples = int(test_result["results"]["bindings"][0]["count"]["value"])
        print(f"✅ Connessione Blazegraph OK - Triple totali: {total_triples:,}")
        
    except Exception as e:
        print(f"❌ Errore connessione Blazegraph: {e}")
        return False

    # Verifica se il grafo specifico esiste
    graph_exists, graph_triples = verify_graph_exists(endpoint_url, graph_uri)
    if not graph_exists:
        print(f"❌ Grafo non trovato o vuoto: {graph_uri}")
        print(f"⚠️ Verifica che i dati per {device_type} siano stati caricati in Blazegraph")
        return False
    
    print(f"✅ Grafo trovato con {graph_triples:,} triple")

    # Query unificata per il grafo specifico
    print(f"\n🔍 Analizzando grafo specifico: {graph_uri}")
    
    unified_query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rico: <https://www.ica.org/standards/RiC/ontology#>
        PREFIX prov: <http://www.w3.org/ns/prov#>

        SELECT DISTINCT ?recordset_path ?record_path ?record
        WHERE {{
            GRAPH <{graph_uri}> {{
                # RecordSet
                ?recordset rdf:type rico:RecordSet .
                ?recordset rico:hasOrHadInstantiation ?instantiation_rs .
                ?instantiation_rs prov:atLocation ?location_rs .
                ?location_rs rdfs:label ?recordset_path .
                
                # Record (opzionale)
                OPTIONAL {{
                    ?record rico:isOrWasIncludedIn ?recordset .
                    ?record rdf:type rico:Record .
                    ?record rico:hasOrHadInstantiation ?instantiation .
                    ?instantiation prov:atLocation ?location .
                    ?location rdfs:label ?record_path .
                }}
            }}
        }}
        ORDER BY ?recordset_path ?record_path
    """
    
    try:
        endpoint.setQuery(unified_query)
        results = endpoint.query().convert()
        
        all_recordsets = set()
        all_records_by_path = defaultdict(list)
        total_records = 0
        
        for res in results["results"]["bindings"]:
            recordset_path = res["recordset_path"]["value"]
            full_recordset_path = normalize_path(recordset_path, base_path)
            all_recordsets.add(full_recordset_path)
            
            if "record_path" in res and "record" in res:
                record_path = res["record_path"]["value"]
                record = res["record"]["value"]
                full_record_path = normalize_path(record_path, base_path)
                all_records_by_path[full_record_path].append((record, full_recordset_path))
                total_records += 1
        
        print(f"📊 RecordSet trovati: {len(all_recordsets)}")
        print(f"📊 Record trovati: {total_records}")
        
    except Exception as e:
        print(f"❌ Errore query per grafo {graph_uri}: {e}")
        return False

    # Calcolo ricorsivo dei file per RecordSet
    print("\n📊 Calcolo ricorsivo dei file per RecordSet...")
    
    rilevati_file = {}
    false_positives_avoided = 0
    
    for recordset_path in sorted(all_recordsets):
        recursive_count = 0
        for record_path, records in all_records_by_path.items():
            if is_true_subpath(record_path, recordset_path):
                recursive_count += len(records)
            elif record_path.startswith(recordset_path) and not is_true_subpath(record_path, recordset_path):
                false_positives_avoided += len(records)
        
        rilevati_file[recordset_path] = recursive_count
        print(f"  📁 {os.path.basename(recordset_path)}: {recursive_count} file")

    # Confronto finale - FIXED VERSION
    print(f"\n{'='*100}")
    print(f"=== CONFRONTO RISULTATI ===")
    print(f"{'='*100}")
    print(f"{'STATO':<15} {'JSON':<8} {'SPARQL':<8} {'DIFF':<8} {'PATH'}")
    print("-" * 100)

    discrepanze = 0
    exact_matches = 0
    mancanti_in_rdf = []
    non_trovati_in_json = []

    # Create a set of all paths to compare (both from JSON and SPARQL)
    all_paths_to_compare = set(attesi.keys()) | set(rilevati_file.keys())
    
    # Debug: Show what paths we're comparing
    print(f"🔍 Debug - Paths da JSON: {list(attesi.keys())}")
    print(f"🔍 Debug - Paths da SPARQL: {list(rilevati_file.keys())}")
    print(f"🔍 Debug - Tutti i paths da confrontare: {len(all_paths_to_compare)}")
    
    for path_key in sorted(all_paths_to_compare):
        count_json = attesi.get(path_key, 0)
        count_rdf = rilevati_file.get(path_key, 0)
        diff = count_rdf - count_json
        
        # Skip if both counts are 0 (shouldn't happen, but just in case)
        if count_json == 0 and count_rdf == 0:
            continue
            
        if count_json == count_rdf:
            stato = "✅ MATCH"
            exact_matches += 1
        elif count_json == 0:
            stato = "🆕 SOLO SPARQL"
            non_trovati_in_json.append((path_key, count_rdf))
        elif count_rdf == 0:
            stato = "❌ MANCANTE RDF"
            discrepanze += 1
            mancanti_in_rdf.append(path_key)
        else:
            stato = "⚠️ DISCREPANZA"
            discrepanze += 1
        
        path_display = os.path.basename(path_key) if len(path_key) > 50 else path_key
        print(f"{stato:<15} {count_json:<8} {count_rdf:<8} {diff:+<8} {path_display}")

    # Report finale
    print(f"\n{'='*70}")
    print(f"📊 REPORT FINALE VERIFICA CONTEGGI")
    print(f"{'='*70}")
    print(f"✅ Verifiche OK: {exact_matches}")
    print(f"❌ Discrepanze: {discrepanze}")
    print(f"🔍 Falsi positivi evitati: {false_positives_avoided}")
    print(f"🆕 Path SPARQL non attesi: {len(non_trovati_in_json)}")
    print(f"❌ Path mancanti in RDF: {len(mancanti_in_rdf)}")

    # Dettagli per il debug
    if mancanti_in_rdf:
        print(f"\n❌ Path mancanti in RDF (primi 10):")
        for path in mancanti_in_rdf[:10]:
            print(f"  - {os.path.basename(path)}")

    if non_trovati_in_json:
        print(f"\n🆕 Path non attesi in SPARQL (primi 10):")
        for path, count in non_trovati_in_json[:10]:
            print(f"  - {os.path.basename(path)}: {count} file")

    # Determinazione del successo
    success = discrepanze == 0
    
    if success:
        print(f"\n🎉 SUCCESSO: Tutti i conteggi corrispondono perfettamente!")
        print(f"✅ {exact_matches} directory verificate con successo")
    else:
        print(f"\n❌ FALLIMENTO: Trovate {discrepanze} discrepanze")
        print(f"⚠️ Verifica i dati in Blazegraph e il file JSON")
    
    return success

def main():
    """Funzione principale con parsing argomenti"""
    parser = argparse.ArgumentParser(
        description="Verifica conteggi file tramite query SPARQL a Blazegraph",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi di utilizzo:

  # Modalità tradizionale (endpoint remoto)
  python count_check.py floppy
  
  # Modalità pipeline con journal locale
  python count_check.py floppy --blazegraph-journal blazegraph_journal/blazegraph.jnl --blazegraph-config blazegraph_journal/RWStore.properties
  
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
    
    args = parser.parse_args()
    
    # Verifica coerenza parametri
    if args.blazegraph_journal and not args.blazegraph_config:
        print("❌ Errore: --blazegraph-config richiesto quando si usa --blazegraph-journal")
        sys.exit(1)
    
    if args.blazegraph_config and not args.blazegraph_journal:
        print("❌ Errore: --blazegraph-journal richiesto quando si usa --blazegraph-config")
        sys.exit(1)
    
    # Esegui verifica
    try:
        success = verify_counts_blazegraph(
            args.device_type,
            args.blazegraph_journal,
            args.blazegraph_config
        )
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n⚠️ Operazione interrotta dall'utente")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Errore fatale: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()