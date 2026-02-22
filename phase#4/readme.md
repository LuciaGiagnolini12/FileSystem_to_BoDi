# Born-Digital Archive Pipeline — Phase 4

Phase 4 generates natural-language descriptions of the physical files in the archive using a locally running AI model. Each description is stored in the graph as a citable entity linked to the file it describes, the model that produced it, and the generation activity.

---

## How it works

`ai_generated_descriptions.py` queries Blazegraph for all Instantiations, then sends the technical metadata of each file to a locally running [Ollama](https://ollama.com/) model. The generated description is stored as a `bodi:TechnicalDescription` entity, linked to a `rico:Activity` that records the model used and the generation timestamp.

Output is written both to a **N-Quads file** and to a **dedicated named graph** in Blazegraph.

---

## Requirements

```bash
pip install requests ollama
```

A running [Ollama](https://ollama.com/) instance with at least one model pulled:
```bash
ollama pull llama3.2
```

---

## Adapting the script to a different archive

Update the `BASE_URIS` dictionary at the top of `ai_generated_descriptions.py`:
```python
BASE_URIS = {
    'ai_generated_desc':           "http://your-institution.org/YourArchive/ai_generated_desc_",
    'ai_text_generation_activity': "http://your-institution.org/YourArchive/ai_textgen_activity_",
    'software':                    "http://your-institution.org/YourArchive/software_",
    'date':                        "http://your-institution.org/YourArchive/date_",
}
```

The target graph can be set at runtime via `--target-graph` (see below).

**Filtering by file type** — To generate descriptions only for certain file types, edit the SPARQL query inside `get_instantiations_with_metadata()` and add a filter on the `rico:type` property assigned by Phase 2 Step 2's MIME classification. For example, to process only image and document files:

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

## Running the script

```bash
# Full run (all non-redacted instantiations, default model llama3.2)
python ai_generated_descriptions.py

# Limit to a subset for testing
python ai_generated_descriptions.py --limit 50

# Use a different Ollama model
python ai_generated_descriptions.py --ollama-model mistral

# Dry-run: queries data and builds prompts without calling Ollama
python ai_generated_descriptions.py --dry-run

# Export .nq only, without inserting into Blazegraph
python ai_generated_descriptions.py --export-nquads

# Custom target graph
python ai_generated_descriptions.py --target-graph http://your-institution.org/YourArchive/ai_descriptions

# Custom endpoints
python ai_generated_descriptions.py \
  --blazegraph-endpoint http://localhost:9999/blazegraph/namespace/kb/sparql \
  --ollama-endpoint http://localhost:11434

# Tune pagination and batch size
python ai_generated_descriptions.py --page-size 200 --batch-size 20

# Connection test only
python ai_generated_descriptions.py --test-only
```

---

## What it produces

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
ai_descriptions_*.nq                  # N-Quads file
ai_descriptions_report_*.json         # processing report
ai_descriptions_checkpoint.json       # resumption checkpoint
ai_descriptions_uri_counters.json     # persistent URI counters
```

---

## Notes

- **Resumability**: the script saves a checkpoint after every batch. If interrupted, re-running it will skip already-processed Instantiations automatically.
- **Incremental insertion**: by default, new triples are inserted into Blazegraph every 100 descriptions (`--incremental-every`). Lower this value if memory is limited.
- **Model choice**: any model available in your Ollama installation can be used. The Software entity in the graph records the model name and links to its documentation page on `ollama.com`.
- **Run time**: generating descriptions for a large archive is slow. With the default model on modest hardware, expect roughly 3 seconds per file. Use `--limit` to run a test batch first.
