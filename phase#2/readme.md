# Born-Digital Archive Pipeline — Phase 2

Phase 2 operates on the RDF graph produced by Phase 1. It consists of two scripts that run sequentially: first the graph is validated, then it is enriched with new relationships and entities.

---

## How it works

**Step 1 — Validation (`step_1_validation_queries.py`)** runs a suite of SPARQL queries against Blazegraph to verify that the graph produced by Phase 1 is structurally sound and internally consistent before any enrichment takes place.

**Step 2 — Graph enrichment (`step_2_relations_update.py`)** reads the validated graph and generates new RDF triples in two passes:

- **Group A — query-based enrichment**: derives new relationships from data already in the graph (duplicate files, dates, titles)
- **Group B — knowledge-based enrichment**: applies external classification logic (metadata type grouping, MIME type categories, semantic alignments)

All new triples are written both to a **N-Quads file** (`.nq`) and loaded into a **dedicated named graph** in Blazegraph, keeping the enrichment layer separate from the Phase 1 data.

---

## Requirements

**Python ≥ 3.8:**
```bash
pip install requests
```

Both scripts communicate with Blazegraph via HTTP — no additional software is required beyond a running Blazegraph instance (see Phase 1 README).

---

## Configuration

### URIs and target graph

Both scripts contain hardcoded URIs that must be updated for a different archive.

**`step_2_relations_update.py`** — two places to update:

```python
# Base URIs used to mint new entities (dates, titles, metadata sets)
BASE_URIS = {
    'date': "http://your-institution.org/YourArchive/date_",
    'technical_metadata_set': "http://your-institution.org/YourArchive/technical_metadata_set_",
    'title': "http://your-institution.org/YourArchive/title_"
}

# Named graph where all new triples are loaded
target_graph = "http://your-institution.org/YourArchive/updated_relations"
```

The `target_graph` can also be set at runtime via `--target-graph` (see below).

**`step_1_validation_queries.py`** — no URI changes needed; it only reads the graph, it does not write to it.

### SPARQL endpoint

Both scripts default to `http://localhost:10214/blazegraph/namespace/kb/sparql`. A different endpoint can be passed via `--endpoint`.

---

## Running the scripts

### Step 1 — Validation

```bash
# Full validation (all categories)
python step_1_validation_queries.py

# Basic validation only (statistics + structural integrity)
python step_1_validation_queries.py --level basic

# Custom endpoint
python step_1_validation_queries.py --endpoint http://localhost:9999/blazegraph/namespace/kb/sparql

# Connection test only
python step_1_validation_queries.py --test-only

# Skip CSV exports
python step_1_validation_queries.py --skip-csv

# Custom CSV filenames
python step_1_validation_queries.py --csv-file metadata_types.csv --mime-csv-file mime_distribution.csv
```

### Step 2 — Enrichment

```bash
# Full enrichment (both groups) — inserts into Blazegraph + saves .nq file
python step_2_relations_update.py

# Dry-run: analyses data and generates the .nq file without inserting anything
python step_2_relations_update.py --dry-run

# Export .nq only, without inserting into Blazegraph
python step_2_relations_update.py --export-nquads

# Run only Group A (query-based: hashes, dates, titles)
python step_2_relations_update.py --only-queries

# Run only Group B (knowledge-based: sameAs, metadata sets, MIME classification)
python step_2_relations_update.py --only-mappings

# Custom target graph
python step_2_relations_update.py --target-graph http://your-institution.org/YourArchive/updated_relations
```

---

## Validation categories (Step 1)

| Category | What it checks |
|----------|----------------|
| 1. General statistics | Triple count, entity type distribution, missing labels |
| 2. Archival structural integrity | Hierarchy cycles, orphan records/instantiations, depth inconsistencies, duplicate locations |
| 3. Technical metadata validation | Coverage by tool (Tika, ExifTool, FileSystem), orphan metadata, missing types or activities |
| 4. Hash validation | SHA-256 format, missing algorithm links, duplicate hashes, multiple hashes per file |
| 5. Advanced consistency | Path format, multiple locations per instantiation |
| 6. CSV export | Metadata types per tool (`.csv`) + MIME type distribution (`.csv`) |

The validator is optimised for large datasets (16GB+): queries run with a 3-second throttle between them and an 8-second pause between categories to avoid overloading Blazegraph.

---

## Enrichment operations (Step 2)

### Group A — query-based

| Operation | What it does | RDF property |
|-----------|-------------|--------------|
| Hash duplicates | Links instantiations that share the same SHA-256 hash | `bodi:hasSameHashCodeAs` (bidirectional) |
| Creation dates | Extracts `dcterms:created` from metadata → creates `rico:Date` entities linked to Records | `rico:hasCreationDate` / `rico:isCreationDateOf` |
| Modification dates (Records) | Extracts `dcterms:modified`, falling back to `st_mtime` for Records without it | `rico:hasModificationDate` / `rico:isModificationDateOf` |
| Modification dates (RecordSets) | Extracts `st_mtime` from RecordSet instantiations | `rico:hasModificationDate` / `rico:isModificationDateOf` |
| Modification dates (Instantiations) | Extracts `st_mtime` directly on Instantiations | `rico:hasModificationDate` / `rico:isModificationDateOf` |
| Title generation | Creates `rico:Title` entities from the `rdfs:label` of every Record and RecordSet | `rico:hasOrHadTitle` / `rico:isTitleOf` |

### Group B — knowledge-based

| Operation | What it does | RDF property |
|-----------|-------------|--------------|
| `owl:sameAs` alignment | Links equivalent `TechnicalMetadataType` entities from different tools (e.g. `CreateDate` ↔ `dcterms:created`) | `owl:sameAs` |
| TechnicalMetadataTypeSet | Creates typed grouping entities (FileSystem, Document, Image, Audio, Video, Email, Executable, CompressedFile, Other) | `bodi:TechnicalMetadataTypeSet` |
| MetadataType → Set | Links each `TechnicalMetadataType` to its set based on field name classification | `rico:isOrWasPartOf` / `rico:hasOrHadPart` |
| MIME type classification | Reads the `Content-Type` metadata value of each Instantiation and assigns a human-readable category | `rico:type` |

---

## Expected output

```
relations_update_*.nq                 # all new triples as N-Quads
enhanced_relationship_report_*.json  # enrichment report
dataset_validation_report_*.json     # validation report
metadata_types_report_*.csv          # metadata types per tool
mime_types_distribution_*.csv        # MIME type counts
```

The `.nq` file and the named graph in Blazegraph contain identical data. The named graph URI defaults to `<base_uri>/updated_relations`.

---

## Notes

- **Idempotency**: both scripts check for existing relationships before inserting — re-running them is safe and will skip already-processed entities.
- **Dry-run recommended**: run Step 2 with `--dry-run` first to inspect the `.nq` output before committing changes to Blazegraph.
- **Date normalisation**: dates are accepted in any common format (ISO 8601, Unix timestamps, DD/MM/YYYY, etc.) and normalised to `YYYY-MM-DD` before being written to the graph.
- **`owl:sameAs` equivalences**: the list of equivalent metadata field names (e.g. `FileSize` ↔ `Content-Length` ↔ `st_size`) is hardcoded in the `METADATA_EQUIVALENCES` list in `step_2_relations_update.py`. Extend or modify it to match the metadata fields present in your archive.
- **MIME classification**: the mapping from MIME type to human-readable category is hardcoded in `MIME_TYPE_CATEGORY_MAPPING`. Add entries for any MIME types not already covered.
