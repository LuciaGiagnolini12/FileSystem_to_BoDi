#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Evangelisti Works Modeler - LRMoo F1 Work Relations
ENHANCED: Con propagazione dai RecordSet ai Record figli
"""

import re
import requests
import sys
import os
from urllib.parse import quote
from rdflib import Dataset, Graph, URIRef, Literal, Namespace, ConjunctiveGraph
from rdflib.namespace import RDF, RDFS
import unicodedata
import pandas as pd
from pathlib import Path

# === CONFIGURAZIONE FILE XLSX ===
XLSX_FILE_PATH = "opere_evangelisti.xlsx"

# === NAMESPACE DEFINITIONS ===
LRMOO = Namespace("http://iflastandards.info/ns/lrm/lrmoo/")
RIC = Namespace("https://www.ica.org/standards/RiC/ontology#")
BASE_URI_WORKS = "http://ficlit.unibo.it/ArchivioEvangelisti/works/"
BASE_URI_RECORDS = "http://ficlit.unibo.it/ArchivioEvangelisti/"
GRAPH_URI = URIRef("http://ficlit.unibo.it/ArchivioEvangelisti/works")

# Blazegraph endpoint configuration
BLAZEGRAPH_ENDPOINT = "http://localhost:10214/blazegraph/sparql"
BLAZEGRAPH_UPDATE_ENDPOINT = "http://localhost:10214/blazegraph/sparql"

def normalize_title_for_uri(title):
    """Normalizza un titolo per creare un URI valido in LOWERCASE"""
    normalized = unicodedata.normalize('NFD', title)
    ascii_title = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    
    replacements = {
        ' ': '_', '.': '', ',': '', ':': '', ';': '', '!': '', '?': '', '"': '', "'": '',
        '(': '', ')': '', '[': '', ']': '', '/': '_', '\\': '_', '-': '_',
        'Ã ': 'a', 'Ã¨': 'e', 'Ã©': 'e', 'Ã¬': 'i', 'Ã²': 'o', 'Ã¹': 'u'
    }
    
    for old, new in replacements.items():
        ascii_title = ascii_title.replace(old, new)
    
    ascii_title = ascii_title.lower()
    ascii_title = re.sub(r'_+', '_', ascii_title)
    ascii_title = ascii_title.strip('_')
    
    if len(ascii_title) > 100:
        ascii_title = ascii_title[:100].rstrip('_')
    
    return ascii_title if ascii_title else "untitled"

def create_work_uri(title):
    """Crea URI per un'opera"""
    normalized = normalize_title_for_uri(title)
    return URIRef(f"{BASE_URI_WORKS}{normalized}")

def create_record_uri(record_identifier):
    """Crea URI per un Record o RecordSet"""
    clean_id = record_identifier.strip()
    
    if clean_id.startswith('<') and clean_id.endswith('>'):
        uri_string = clean_id[1:-1]
        return URIRef(uri_string)
    
    return URIRef(f"{BASE_URI_RECORDS}{clean_id}")

def detect_record_type(record_uri_string):
    """Determina se si tratta di un Record o RecordSet basandosi sull'URI"""
    uri_lower = record_uri_string.lower()
    
    if 'rs' in uri_lower or 'recordset' in uri_lower:
        return RIC.RecordSet
    
    if 'record' in uri_lower and 'recordset' not in uri_lower:
        return RIC.Record
    
    segments = uri_lower.split('_')
    if len(segments) > 5:
        return RIC.RecordSet
    
    return RIC.Record

def test_blazegraph_connection():
    """Testa la connessione a Blazegraph"""
    try:
        response = requests.get(f"{BLAZEGRAPH_ENDPOINT}?query=SELECT * WHERE {{ ?s ?p ?o }} LIMIT 1", 
                              timeout=10)
        if response.status_code == 200:
            print("✅ Connessione a Blazegraph OK - works_evangelisti.py:93")
            return True
        else:
            print(f"⚠️ Blazegraph risponde con status {response.status_code} - works_evangelisti.py:96")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Errore connessione Blazegraph: {e} - works_evangelisti.py:99")
        return False

def clear_existing_graph():
    """Pulisce il grafo esistente se presente"""
    clear_query = f"DROP GRAPH <{GRAPH_URI}>"
    
    try:
        response = requests.post(
            BLAZEGRAPH_UPDATE_ENDPOINT,
            data=clear_query,
            headers={'Content-Type': 'application/sparql-update', 'Accept': 'application/json'},
            timeout=30
        )
        
        if response.status_code in [200, 204]:
            print("✅ Grafo esistente pulito - works_evangelisti.py:115")
        else:
            print(f"⚠️ Pulizia grafo: status {response.status_code} - works_evangelisti.py:117")
            
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Errore pulizia grafo (potrebbe non esistere): {e} - works_evangelisti.py:120")

# === NUOVE FUNZIONI PER PROPAGAZIONE RECORDSET→RECORD ===

def find_child_records(recordset_uri):
    """
    NUOVA: Trova i Record figli di un RecordSet interrogando Blazegraph
    Usa diverse strategie per identificare i figli
    """
    child_records = []
    
    query1 = f"""
    SELECT DISTINCT ?child_record WHERE {{
        ?child_record <{RIC.isOrWasIncludedIn}> <{recordset_uri}> .
        ?child_record a <{RIC.Record}> .
    }}
    """
    
    # Strategia 3: Pattern URI (se seguono convenzione tipo RS_123 → R_123_001)
    recordset_str = str(recordset_uri)
    if "RS_" in recordset_str:
        base_id = recordset_str.replace("RS_", "R_").rstrip("/")
        query3 = f"""
        SELECT DISTINCT ?child_record WHERE {{
            ?child_record a <{RIC.Record}> .
            FILTER(STRSTARTS(STR(?child_record), "{base_id}"))
        }}
        """
    else:
        query3 = None
    
    queries_to_try = [query1]
    if query3:
        queries_to_try.append(query3)
    
    try:
        for i, query in enumerate(queries_to_try, 1):
            response = requests.get(
                BLAZEGRAPH_ENDPOINT,
                params={'query': query},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=15
            )
            
            if response.status_code == 200:
                result = response.json()
                for binding in result['results']['bindings']:
                    child_uri = URIRef(binding['child_record']['value'])
                    if child_uri not in child_records:
                        child_records.append(child_uri)
                        
                if child_records:
                    print(f"🔍 Strategia {i} trovata: {len(child_records)} figli per {recordset_uri} - works_evangelisti.py:172")
                    break
            else:
                print(f"⚠️ Strategia {i} fallita: HTTP {response.status_code} - works_evangelisti.py:175")
                
    except Exception as e:
        print(f"❌ Errore ricerca figli per {recordset_uri}: {e} - works_evangelisti.py:178")
    
    return child_records

def is_recordset(record_uri):
    """
    NUOVA: Verifica se un URI rappresenta un RecordSet interrogando Blazegraph
    """
    check_query = f"""
    ASK {{
        <{record_uri}> a <{RIC.RecordSet}> .
    }}
    """
    
    try:
        response = requests.get(
            BLAZEGRAPH_ENDPOINT,
            params={'query': check_query},
            headers={'Accept': 'application/sparql-results+json'},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            return result['boolean']
        else:
            # Fallback: usa euristica basata su URI
            return detect_record_type(str(record_uri)) == RIC.RecordSet
            
    except Exception as e:
        print(f"⚠️ Errore verifica RecordSet per {record_uri}: {e} - works_evangelisti.py:208")
        # Fallback: usa euristica basata su URI
        return detect_record_type(str(record_uri)) == RIC.RecordSet

def propagate_to_child_records(work_uri, recordset_uri, dataset, stats_counter):
    """
    NUOVA: Propaga il collegamento Work↔RecordSet anche ai Record figli del RecordSet
    """
    print(f"🔄 Propagazione da RecordSet {recordset_uri} ai figli... - works_evangelisti.py:216")
    
    child_records = find_child_records(recordset_uri)
    
    if not child_records:
        print(f"ℹ️ Nessun figlio trovato per RecordSet {recordset_uri} - works_evangelisti.py:221")
        return 0
    
    propagated_count = 0
    
    for child_record in child_records:
        # Work → Child Record
        if add_triple_to_named_graph(dataset, work_uri, RIC.isRelatedTo, child_record, GRAPH_URI,
                                   f"Work→Child: {work_uri} ↔ {child_record} (via {recordset_uri})"):
            propagated_count += 1
        
        # Child Record → Work  
        if add_triple_to_named_graph(dataset, child_record, RIC.isRelatedTo, work_uri, GRAPH_URI,
                                   f"Child→Work: {child_record} ↔ {work_uri} (via {recordset_uri})"):
            propagated_count += 1
    
    stats_counter['recordset_propagations'] += propagated_count
    print(f"✅ Propagati {propagated_count} collegamenti a {len(child_records)} Record figli - works_evangelisti.py:238")
    
    return propagated_count

# === FINE NUOVE FUNZIONI ===

def upload_to_blazegraph_v2(dataset):
    """
    METODO AGGIORNATO: Carica il dataset RDF in Blazegraph usando SPARQL UPDATE
    Questo assicura che le triple finiscano nel named graph corretto
    """
    print("🔥 Caricamento dati in Blazegraph nel named graph... - works_evangelisti.py:249")
    
    try:
        # Serializza il dataset in TriG (formato che supporta named graphs)
        trig_data = dataset.serialize(format='trig')
        
        # Crea query INSERT DATA
        insert_query = f"""
        INSERT DATA {{
            {trig_data}
        }}
        """
        
        response = requests.post(
            BLAZEGRAPH_UPDATE_ENDPOINT,
            data=insert_query,
            headers={
                'Content-Type': 'application/sparql-update',
                'Accept': 'application/json'
            },
            timeout=120
        )
        
        if response.status_code not in [200, 204]:
            print(f"❌ Errore caricamento: HTTP {response.status_code} - works_evangelisti.py:273")
            print(f"Response: {response.text[:500]} - works_evangelisti.py:274")
            return False
        
        print("✅ Dati caricati con successo nel named graph - works_evangelisti.py:277")
        return True
        
    except Exception as e:
        print(f"❌ Errore nel caricamento: {e} - works_evangelisti.py:281")
        import traceback
        traceback.print_exc()
        return False

def check_triple_exists_in_graph(subject, predicate, obj):
    """Controlla se una tripla esiste già nel named graph specificato"""
    check_query = f"""
    ASK {{
        GRAPH <{GRAPH_URI}> {{
            <{subject}> <{predicate}> <{obj}> .
        }}
    }}
    """
    
    try:
        response = requests.get(
            BLAZEGRAPH_ENDPOINT,
            params={'query': check_query},
            headers={'Accept': 'application/sparql-results+json'},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            return result['boolean']
        else:
            print(f"⚠️ Errore controllo tripla: HTTP {response.status_code} - works_evangelisti.py:308")
            return False
            
    except Exception as e:
        print(f"❌ Errore controllo tripla: {e} - works_evangelisti.py:312")
        return False

def add_triple_to_named_graph(dataset, subject, predicate, obj, graph_uri, description=""):
    """
    Aggiunge una tripla al named graph specificato nel dataset
    NUOVO: Usa il dataset con named graph context
    """
    # Ottieni il grafo per il named graph URI
    graph = dataset.graph(graph_uri)
    
    # Controlla se la tripla esiste già in Blazegraph
    if not check_triple_exists_in_graph(subject, predicate, obj):
        graph.add((subject, predicate, obj))
        if description:
            print(f"✅ Aggiunta al grafo <{graph_uri}>: {description} - works_evangelisti.py:327")
        return True
    else:
        if description:
            print(f"⚠️ Già esistente nel grafo: {description} - works_evangelisti.py:331")
        return False

def verify_upload_in_named_graph():
    """Verifica che i dati siano stati caricati nel named graph corretto"""
    verify_query = f"""
    SELECT (COUNT(*) as ?count) 
    WHERE {{ 
        GRAPH <{GRAPH_URI}> {{ 
            ?s ?p ?o 
        }} 
    }}
    """
    
    try:
        response = requests.get(
            BLAZEGRAPH_ENDPOINT,
            params={'query': verify_query},
            headers={'Accept': 'application/sparql-results+json'},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            count = int(result['results']['bindings'][0]['count']['value'])
            print(f"✅ Verifica caricamento nel named graph <{GRAPH_URI}>: {count} triple - works_evangelisti.py:356")
            return count > 0
        else:
            print(f"⚠️ Errore verifica: HTTP {response.status_code} - works_evangelisti.py:359")
            return False
            
    except Exception as e:
        print(f"❌ Errore verifica: {e} - works_evangelisti.py:363")
        return False

def read_works_xlsx(xlsx_file_path):
    """Legge il file XLSX e restituisce lista di tuple (work_title, record_identifier)"""
    if not os.path.exists(xlsx_file_path):
        raise FileNotFoundError(f"File XLSX non trovato: {xlsx_file_path}")
    
    print(f"📖 Lettura file XLSX: {xlsx_file_path} - works_evangelisti.py:371")
    
    try:
        df = pd.read_excel(xlsx_file_path, engine='openpyxl')
        
        if df.shape[1] < 2:
            raise ValueError("Il file XLSX deve avere almeno 2 colonne")
        
        df_works = df.iloc[:, [0, 1]]
        df_works.columns = ['work_title', 'record_identifier']
        df_works = df_works.dropna()
        
        works_records = []
        for _, row in df_works.iterrows():
            work_title = str(row['work_title']).strip()
            record_id = str(row['record_identifier']).strip()
            
            if work_title and record_id:
                works_records.append((work_title, record_id))
        
        print(f"✅ Lette {len(works_records)} relazioni workrecord dal file XLSX - works_evangelisti.py:391")
        
        return works_records
        
    except Exception as e:
        raise Exception(f"Errore nella lettura del file XLSX: {e}")

def find_parent_works(work_title, existing_relations):
    """Trova i work "padre" (cicli/trilogie) che contengono il work specificato"""
    parent_works = []
    
    for cycle_title, novel_title in existing_relations:
        if novel_title.strip().lower() == work_title.strip().lower():
            parent_works.append(cycle_title)
    
    return parent_works

def create_evangelisti_works_dataset(xlsx_file_path=None):
    """
    METODO AGGIORNATO: Crea un Dataset RDF con named graph per le relazioni tra opere di Evangelisti
    ENHANCED: Con propagazione RecordSet→Record
    """
    
    # Dati delle relazioni parte-tutto
    works_relations = [
        ("Ciclo di Eymerich", "Nicolas Eymerich, inquisitore"),
        ("Ciclo di Eymerich", "Le catene di Eymerich"),
        ("Ciclo di Eymerich", "Il mistero dell'inquisitore Eymerich"),
        ("Ciclo di Eymerich", "Il corpo e il sangue di Eymerich"),
        ("Ciclo di Eymerich", "Cherudek"),
        ("Ciclo di Eymerich", "Picatrix. La scala per l'inferno"),
        ("Ciclo di Eymerich", "Metallo urlante"),
        ("Ciclo di Eymerich", "Il castello di Eymerich"),
        ("Ciclo di Eymerich", "Mater Terribilis"),
        ("Ciclo di Eymerich", "La Sala dei Giganti"),
        ("Ciclo di Eymerich", "La luce di Orione"),
        ("Ciclo di Eymerich", "Rex tremendae maiestatis"),
        ("Ciclo di Eymerich", "Eymerich risorge"),
        ("Ciclo di Eymerich", "Il fantasma di Eymerich"),
        ("Ciclo di Pantera", "Metallo urlante"),
        ("Ciclo di Eymerich", "La furia di Eymerich"),
        ("Ciclo di Pantera", "Black Flag"),
        ("Ciclo di Pantera", "Antracite"),
        ("Trilogia di Magus", "Magus. Il romanzo di Nostradamus. Vol. 1: Il Presagio"),
        ("Trilogia di Magus", "Magus. Il romanzo di Nostradamus. Vol. 2: L'Inganno"),
        ("Trilogia di Magus", "Magus. Il romanzo di Nostradamus. Vol. 3: L'Abisso"),
        ("Trilogia americana", "Antracite"),
        ("Trilogia americana", "Noi saremo tutto"),
        ("Trilogia americana", "One Big Union"),
        ("Trilogia americana", "Trilogia americana"),
        ("Ciclo messicano", "Il collare di fuoco"),
        ("Ciclo messicano", "Il collare spezzato"),
        ("Ciclo dei pirati", "Tortuga"),
        ("Ciclo dei pirati", "Veracruz"),
        ("Ciclo dei pirati", "Cartagena"),
        ("Ciclo Il sole dell'avvenire", "Il sole dell'avvenire. Vivere lavorando o morire combattendo"),
        ("Ciclo Il sole dell'avvenire", "Il sole dell'avvenire. Chi ha del ferro ha del pane"),
        ("Ciclo Il sole dell'avvenire", "Il sole dell'avvenire. Nella notte ci guidano le stelle"),
    ]
    
    # CAMBIAMENTO PRINCIPALE: Usa Dataset invece di Graph
    dataset = Dataset()
    
    # Bind dei namespace per il dataset
    dataset.bind("lrmoo", LRMOO)
    dataset.bind("ric", RIC)
    dataset.bind("works", Namespace(BASE_URI_WORKS))
    dataset.bind("records", Namespace(BASE_URI_RECORDS))
    
    # Ottieni il grafo per il named graph specificato
    target_graph = dataset.graph(GRAPH_URI)
    
    # Set per tenere traccia delle opere già dichiarate
    declared_works = set()
    declared_records = set()
    
    # NUOVO: Contatori per statistiche enhanced
    stats = {
        'cycles_created': set(),
        'novels_created': set(),
        'relations_created': 0,
        'work_record_relations': 0,
        'propagated_relations': 0,
        'recordset_propagations': 0  # NUOVO
    }
    
    print("🔥 Creazione dataset RDF con named graph ENHANCED... - works_evangelisti.py:477")
    print(f"📚 Processando {len(works_relations)} relazioni workwork... - works_evangelisti.py:478")
    print(f"🎯 Target Named Graph: {GRAPH_URI} - works_evangelisti.py:479")
    print("🆕 FUNZIONALITÀ: Propagazione RecordSet→Record attiva - works_evangelisti.py:480")
    
    # === FASE 1: Crea relazioni Work-Work (cicli e romanzi) ===
    for cycle_title, novel_title in works_relations:
        
        cycle_uri = create_work_uri(cycle_title)
        novel_uri = create_work_uri(novel_title)
        
        # Dichiara il ciclo come F1 Work nel named graph
        if cycle_uri not in declared_works:
            if add_triple_to_named_graph(dataset, cycle_uri, RDF.type, LRMOO.F1_Work, GRAPH_URI,
                                       f"Ciclo: '{cycle_title}' -> {cycle_uri}"):
                target_graph.add((cycle_uri, RDFS.label, Literal(cycle_title)))
            declared_works.add(cycle_uri)
            stats['cycles_created'].add(cycle_title)
        
        # Dichiara il romanzo come F1 Work nel named graph
        if novel_uri not in declared_works:
            if add_triple_to_named_graph(dataset, novel_uri, RDF.type, LRMOO.F1_Work, GRAPH_URI,
                                       f"Romanzo: '{novel_title}' -> {novel_uri}"):
                target_graph.add((novel_uri, RDFS.label, Literal(novel_title)))
            declared_works.add(novel_uri)
            stats['novels_created'].add(novel_title)
        
        # Aggiungi relazioni parte-tutto nel named graph
        if add_triple_to_named_graph(dataset, cycle_uri, LRMOO.R67_has_part, novel_uri, GRAPH_URI,
                                   f"{cycle_title} R67_has_part {novel_title}"):
            stats['relations_created'] += 1
        
        if add_triple_to_named_graph(dataset, novel_uri, LRMOO.R67i_forms_part_of, cycle_uri, GRAPH_URI,
                                   f"{novel_title} R67i_forms_part_of {cycle_title}"):
            stats['relations_created'] += 1
    
    # === FASE 2: Leggi file XLSX e crea collegamenti Work-Record CON PROPAGAZIONE ===
    works_records = []
    if xlsx_file_path and os.path.exists(xlsx_file_path):
        try:
            works_records = read_works_xlsx(xlsx_file_path)
            print(f"\n📊 Processando {len(works_records)} collegamenti workrecord... - works_evangelisti.py:518")
            
            records_created = set()
            
            for work_title, record_identifier in works_records:
                
                # Crea Work se non esiste già
                work_uri = create_work_uri(work_title)
                if work_uri not in declared_works:
                    if add_triple_to_named_graph(dataset, work_uri, RDF.type, LRMOO.F1_Work, GRAPH_URI,
                                               f"Nuovo Work da XLSX: '{work_title}' -> {work_uri}"):
                        target_graph.add((work_uri, RDFS.label, Literal(work_title)))
                    declared_works.add(work_uri)
                
                # Crea Record/RecordSet nel named graph
                record_uri = create_record_uri(record_identifier)
                
                # Collegamento Work ↔ Record/RecordSet nel named graph
                if add_triple_to_named_graph(dataset, work_uri, RIC.isRelatedTo, record_uri, GRAPH_URI,
                                           f"Work->Record: '{work_title}' ↔ {record_uri}"):
                    stats['work_record_relations'] += 1
                
                if add_triple_to_named_graph(dataset, record_uri, RIC.isRelatedTo, work_uri, GRAPH_URI,
                                           f"Record->Work: {record_uri} ↔ '{work_title}'"):
                    stats['work_record_relations'] += 1
                
                # === NUOVA PROPAGAZIONE RecordSet→Record ===
                if is_recordset(record_uri):
                    print(f"\n🔄 PROPAGAZIONE RecordSet rilevato: {record_uri} - works_evangelisti.py:546")
                    propagate_to_child_records(work_uri, record_uri, dataset, stats)
                
                # === PROPAGAZIONE GERARCHICA (Work→Work esistente) ===
                parent_works = find_parent_works(work_title, works_relations)
                
                for parent_title in parent_works:
                    parent_uri = create_work_uri(parent_title)
                    
                    if parent_uri in declared_works:
                        if add_triple_to_named_graph(dataset, parent_uri, RIC.isRelatedTo, record_uri, GRAPH_URI,
                                                   f"Parent->Record: '{parent_title}' ↔ {record_uri}"):
                            stats['propagated_relations'] += 1
                        
                        if add_triple_to_named_graph(dataset, record_uri, RIC.isRelatedTo, parent_uri, GRAPH_URI,
                                                   f"Record->Parent: {record_uri} ↔ '{parent_title}'"):
                            stats['propagated_relations'] += 1
                        
                        # === NUOVA PROPAGAZIONE: Anche il parent work ai figli del RecordSet ===
                        if is_recordset(record_uri):
                            print(f"🔄 Propagazione parent work '{parent_title}' ai figli di {record_uri} - works_evangelisti.py:566")
                            propagate_to_child_records(parent_uri, record_uri, dataset, stats)
            
            print(f"\n📊 STATISTICHE COLLEGAMENTI XLSX ENHANCED: - works_evangelisti.py:569")
            print(f"Record/RecordSet creati: {len(records_created)} - works_evangelisti.py:570")
            print(f"Collegamenti workrecord aggiunti: {stats['work_record_relations']} - works_evangelisti.py:571")
            print(f"Collegamenti propagati (work→work): {stats['propagated_relations']} - works_evangelisti.py:572")
            print(f"🆕 Collegamenti propagati (recordset→record): {stats['recordset_propagations']} - works_evangelisti.py:573")
                
        except Exception as e:
            print(f"⚠️ Errore nel processamento file XLSX: {e} - works_evangelisti.py:576")
            print("Continuo con solo le relazioni workwork... - works_evangelisti.py:577")
    
    # Calcola statistiche del dataset
    total_triples = sum(len(graph) for graph in dataset.graphs())
    named_graph_triples = len(target_graph)
    
    print(f"\n📊 STATISTICHE DATASET FINALE ENHANCED: - works_evangelisti.py:583")
    print(f"Cicli/Trilogie: {len(stats['cycles_created'])} - works_evangelisti.py:584")
    print(f"Romanzi: {len(stats['novels_created'])} - works_evangelisti.py:585")
    print(f"Works totali: {len(declared_works)} - works_evangelisti.py:586")
    print(f"Records/RecordSets: {len(declared_records)} - works_evangelisti.py:587")
    print(f"Relazioni workwork aggiunte: {stats['relations_created']} - works_evangelisti.py:588")
    if works_records:
        total_work_record = stats['work_record_relations'] + stats['propagated_relations'] + stats['recordset_propagations']
        print(f"Relazioni workrecord totali: {total_work_record} - works_evangelisti.py:591")
        print(f"├─ Dirette: {stats['work_record_relations']} - works_evangelisti.py:592")
        print(f"├─ Propagate work→work: {stats['propagated_relations']} - works_evangelisti.py:593")
        print(f"└─ 🆕 Propagate recordset→record: {stats['recordset_propagations']} - works_evangelisti.py:594")
    print(f"Triple nel named graph <{GRAPH_URI}>: {named_graph_triples} - works_evangelisti.py:595")
    print(f"Triple totali nel dataset: {total_triples} - works_evangelisti.py:596")
    print(f"✅ Controllo duplicati attivo - works_evangelisti.py:597")
    print(f"🆕 Propagazione RecordSet→Record attiva - works_evangelisti.py:598")
    
    return dataset

def main():
    """Funzione principale ENHANCED"""
    
    print("= - works_evangelisti.py:605" * 70)
    print("🎭 EVANGELISTI WORKS MODELER  ENHANCED RecordSet Propagation - works_evangelisti.py:606")
    print("Target Named Graph: {} - works_evangelisti.py:607".format(GRAPH_URI))
    print("🆕 NUOVA FUNZIONALITÀ: Propagazione dai RecordSet ai Record figli - works_evangelisti.py:608")
    print("= - works_evangelisti.py:609" * 70)
    
    xlsx_file_path = XLSX_FILE_PATH
    
    if xlsx_file_path and not os.path.exists(xlsx_file_path):
        print(f"⚠️ File XLSX configurato non trovato: {xlsx_file_path} - works_evangelisti.py:614")
        print("Continuo senza file XLSX... - works_evangelisti.py:615")
        xlsx_file_path = None
    
    # Test connessione Blazegraph
    if not test_blazegraph_connection():
        print("❌ Impossibile connettersi a Blazegraph. - works_evangelisti.py:620")
        sys.exit(1)
    
    try:
        # Pulisci grafo esistente
        clear_existing_graph()
        
        # Crea il dataset con named graph ENHANCED
        works_dataset = create_evangelisti_works_dataset(xlsx_file_path)
        
        if sum(len(graph) for graph in works_dataset.graphs()) == 0:
            print("❌ Errore: dataset vuoto generato - works_evangelisti.py:631")
            sys.exit(1)
        
        # Carica in Blazegraph usando il nuovo metodo
        if upload_to_blazegraph_v2(works_dataset):
            
            # Verifica caricamento nel named graph
            if verify_upload_in_named_graph():
                print(f"\n✅ SUCCESSO! Dati caricati nel named graph <{GRAPH_URI}> - works_evangelisti.py:639")
                
                # Salva copia locale
                local_file = "evangelisti_works_enhanced_named_graph.nq"
                works_dataset.serialize(destination=local_file, format='nquads')
                print(f"💾 Copia locale (NQuads) salvata: {local_file} - works_evangelisti.py:644")
                
                print(f"\n🚀 VERIFICA CARICAMENTO: - works_evangelisti.py:646")
                print(f"Blazegraph UI: http://localhost:10214/blazegraph/#query - works_evangelisti.py:647")
                print(f"Named Graph: {GRAPH_URI} - works_evangelisti.py:648")
                
                print(f"\n🔍 QUERY DI VERIFICA: - works_evangelisti.py:650")
                print(f"""
SELECT (COUNT(*) as ?total_triples) 
WHERE {{ 
    GRAPH <{GRAPH_URI}> {{ 
        ?s ?p ?o 
    }} 
}}
""")
                
                print(f"\n🔍 QUERY VERIFICA PROPAGAZIONE RecordSet→Record:")
                print(f"""
SELECT ?work ?recordset ?child_record WHERE {{
    GRAPH <{GRAPH_URI}> {{
        ?work <{RIC.isRelatedTo}> ?recordset .
        ?work <{RIC.isRelatedTo}> ?child_record .
        ?recordset a <{RIC.RecordSet}> .
        ?child_record a <{RIC.Record}> .
        ?child_record <{RIC.isOrWasIncludedIn}> ?recordset .
    }}
}}
""")
                
                print(f"\n📋 STRUTTURA ENHANCED:")
                print(f"   ✅ Tutte le triple sono nel named graph specificato")
                print(f"   ✅ Non più nel grafo default")
                print(f"   ✅ URI Work in lowercase, rdfs:label originali")
                print(f"   ✅ Controllo duplicati attivo")
                print(f"   🆕 Propagazione Work→Work (bottom-up)")
                print(f"   🆕 Propagazione RecordSet→Record (top-down)")
                print(f"   🆕 Triple strategie per identificare Record figli")
                
                print(f"\n🎯 FUNZIONALITÀ AGGIUNTE:")
                print(f"   🔍 find_child_records() - Trova Record figli di un RecordSet")
                print(f"   🔄 propagate_to_child_records() - Propaga collegamenti ai figli")
                print(f"   ❓ is_recordset() - Identifica RecordSet con query SPARQL")
                print(f"   📊 Statistiche dettagliate per tipo di propagazione")
                
            else:
                print("❌ Errore nella verifica del caricamento")
                sys.exit(1)
        else:
            print("❌ Errore nel caricamento in Blazegraph")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Errore durante l'esecuzione: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
