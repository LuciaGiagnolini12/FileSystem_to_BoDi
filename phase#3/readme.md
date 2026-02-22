# Personal Archive Pipeline — Phase 3

Phase 3 adds two layers of interpretation to the graph built in Phases 1 and 2: a bibliographic model of the works documented in the archive, and AI-generated natural-language descriptions of the physical files.

---

## How it works

**Step 1 — Works modelling (`step_1_works_evangelisti.py`)** reads a spreadsheet listing which archive records correspond to which intellectual works, then generates `lrmoo:F1_Work` entities and links them to the Records and RecordSets already in the graph. Work-to-work hierarchical relationships (e.g. a cycle containing individual novels) are also modelled. When a work is linked to a RecordSet, the link is automatically propagated down to all its child Records.

**Step 2 — AI descriptions (`step_2_ai_generated_descriptions.py`)** queries Blazegraph for all Instantiations then sends the technical metadata of each file to a locally running [Ollama](https://ollama.com/) model. The generated description is stored as a `bodi:TechnicalDescription` entity, linked to a `rico:Activity` that records the model used and the generation timestamp.

Both scripts write output both to a **N-Quads file** and to a **dedicated named graph** in Blazegraph.

---

## Requirements

**Step 1:**
```bash
pip install requests rdflib pandas openpyxl
```

**Step 2:**
```bash
pip install requests ollama
```

Step 2 also requires a running [Ollama](https://ollama.com/) instance with at least one model pulled:
```bash
# Install Ollama, then pull the default model
ollama pull llama3.2
```

---

## Adapting the scripts to a different archive

### Step 1 — Works modelling

Three things to update in `step_1_works_evangelisti.py`:

**1. Base URIs:**
```python
BASE_URI_WORKS   = "http://your-institution.org/YourArchive/works/"
BASE_URI_RECORDS = "http://your-institution.org/YourArchive/"
GRAPH_URI        = URIRef("http://your-institution.org/YourArchive/works")
```

**2. Work-to-work relationships** — the list of hierarchical relations between works (cycles, trilogies, series) is hardcoded in `create_evangelisti_works_dataset()`. Replace it with the structure relevant to your archive:
```python
works_relations = [
    ("Series Title", "Volume 1 Title"),
    ("Series Title", "Volume 2 Title"),
    ...
]
```

**3. XLSX file** — the spreadsheet must have two columns: work title and record identifier (URI or plain ID). Update the path:
```python
XLSX_FILE_PATH = "your_works_file.xlsx"
```

### Step 2 — AI descriptions

Update the `BASE_URIS` dictionary at the top of `step_2_ai_generated_descriptions.py`:
```python
BASE_URIS = {
    'ai_generated_desc':         "http://your-institution.org/YourArchive/ai_generated_desc_",
    'ai_text_generation_activity': "http://your-institution.org/YourArchive/ai_textgen_activity_",
    'software':                  "http://your-institution.org/YourArchive/software_",
    'date':                      "http://your-institution.org/YourArchive/date_",
}
```

The target graph can be set at runtime via `--target-graph` (see below).


**Filtering by file type** — To generate descriptions only for certain file types, edit the SPARQL query inside `get_instantiations_with_metadata()` in `step_2_ai_generated_descriptions.py` and add a filter on the `rico:type` property assigned by Phase 2 Step 2's MIME classification. For example, to process only image and document files:

```sparql
SELECT DISTINCT ?instantiation ?relatedRecord WHERE {
    ?instantiation rdf:type rico:Instantiation .
    ?relatedRecord rico:hasOrHadInstantiation ?instantiation .
    ?relatedRecord a rico:Record .

    # Add this block to restrict by file type
    ?instantiation rico:type ?fileType .
    FILTER(?fileType IN (
        "Image (JPEG)", "Image (PNG)", "Image (TIFF)",
        "Document (PDF)", "Document (Word)", "Document (Text)"
    ))

    FILTER NOT EXISTS {
        ?relatedRecord bodi:redactedInformation "yes" .
    }
}
```

The values to use in the filter are the category labels assigned by `MIME_TYPE_CATEGORY_MAPPING` in Phase 2 Step 2 (e.g. `"Video (MP4)"`, `"Audio (MP3)"`, `"Spreadsheet (Excel)"`). Instantiations that were not classified (no `rico:type`) will be excluded by this filter; remove the `rico:type` triple pattern to include them again.

---

## Running the scripts

### Step 1 — Works modelling

```bash
# Standard run (reads opere_evangelisti.xlsx, inserts into Blazegraph, saves .nq)
python step_1_works_evangelisti.py
```

There are no CLI arguments: all configuration is done by editing the constants at the top of the file.

### Step 2 — AI descriptions

```bash
# Full run (all non-redacted instantiations, default model llama3.2)
python step_2_ai_generated_descriptions.py

# Limit to a subset for testing
python step_2_ai_generated_descriptions.py --limit 50

# Use a different Ollama model
python step_2_ai_generated_descriptions.py --ollama-model mistral

# Dry-run: queries data and builds prompts without calling Ollama
python step_2_ai_generated_descriptions.py --dry-run

# Export .nq only, without inserting into Blazegraph
python step_2_ai_generated_descriptions.py --export-nquads

# Custom target graph
python step_2_ai_generated_descriptions.py --target-graph http://your-institution.org/YourArchive/ai_descriptions

# Custom endpoints
python step_2_ai_generated_descriptions.py \
  --blazegraph-endpoint http://localhost:9999/blazegraph/namespace/kb/sparql \
  --ollama-endpoint http://localhost:11434

# Tune pagination and batch size
python step_2_ai_generated_descriptions.py --page-size 200 --batch-size 20

# Connection test only
python step_2_ai_generated_descriptions.py --test-only
```

---

## What Step 1 produces

| Entity / Relationship | RDF |
|----------------------|-----|
| Work entity | `lrmoo:F1_Work` with `rdfs:label` |
| Work-to-work hierarchy (cycle → novel) | `lrmoo:R67_has_part` / `lrmoo:R67i_forms_part_of` |
| Work ↔ Record/RecordSet | `rico:isRelatedTo` (bidirectional) |
| Work ↔ child Records (propagated from RecordSet) | `rico:isRelatedTo` (bidirectional) |

Output files:
```
evangelisti_works_enhanced_named_graph.nq    # N-Quads backup
```

---

## What Step 2 produces

For each processed Instantiation:

| Entity / Relationship | RDF |
|----------------------|-----|
| Description entity | `bodi:TechnicalDescription` with generated text |
| Instantiation ↔ description | `bodi:hasTechnicalDescription` / `bodi:isTechnicalDescriptionOf` |
| Generation activity | `rico:Activity` with timestamp |
| Description ↔ activity | `bodi:generatedBy` / `bodi:hasGenerated` |
| Activity ↔ software | `rico:isOrWasPerformedBy` / `rico:performsOrPerformed` |
| Human validation flag | `bodi:hasHumanValidation "false"` |

Output files:
```
ai_descriptions_*.nq                    # N-Quads file
ai_descriptions_report_*.json           # processing report
ai_descriptions_checkpoint.json         # resumption checkpoint
ai_descriptions_uri_counters.json       # persistent URI counters
```

---

## Notes

- **Resumability**: Step 2 saves a checkpoint after every batch. If interrupted, re-running it will skip already-processed Instantiations automatically.
- **Incremental insertion**: by default, new triples are inserted into Blazegraph every 100 descriptions (`--incremental-every`). Lower this value if memory is limited.
- **Model choice**: any model available in your Ollama installation can be used. The Software entity in the graph records the model name and links to its documentation page on `ollama.com`.
- **Run time**: generating descriptions for a large archive is slow. With the default model on modest hardware, expect roughly 3 seconds per file. Use `--limit` to run a test batch first.
