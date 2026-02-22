# Born-Digital Archive Pipeline - Phase 1

Automated pipeline for indexing, verifying, and loading born-digital personal archives into a knowledge graph modeled with Records in Contexts-Ontology (RiC-O) and the Born-Digital Ontology (BoDi).

Developed and tested on the Valerio Evangelisti Archive, it is designed to be reused with any personal digital directory: it is sufficient to configure the paths in the configuration file.

---

## How it works

The pipeline takes one or more directories as input and produces a **knowledge graph** representing the archive both in its hierarchical structure and in the technical metadata of each file.

The graph is built incrementally across several stages:

1. the **directory tree** is traversed and its structure is modelled as RDF
2. **SHA-256 hashes** are computed for every file and used to verify integrity throughout the process
3. **technical metadata** is extracted for each file via Python Os library, Apache Tika and ExifTool (format, size, encoding, EXIF data, etc.)
4. all data is serialised as **N-Quads (`.nq`) files** and loaded into a **Blazegraph triplestore**, queryable via SPARQL — both formats are retained as output
5. **automatic backups** are created at each critical phase

---

## Requirements

**External software:**
- Java ≥ 8
- [Blazegraph](https://blazegraph.com/) (`blazegraph.jar`)
- [Apache Tika](https://tika.apache.org/) (`tika-app.jar`)
- [ExifTool](https://exiftool.org/) (available in `PATH`)
- `sha256sum` (Linux) or `shasum` (macOS, preinstalled)

**Python ≥ 3.8:**
```bash
pip install requests rdflib SPARQLWrapper psutil
```

---

## Adapting the pipeline to your archive

All configuration is centralised in `directory_config.json`, to be created in the working directory. No code changes are required.

```json
{
  "version": "1.0",
  "directories": {
    "medium1": {
      "path": "/path/to/your/directory/",
      "description": "Human-readable description of the medium",
      "files": {
        "count_output": "Medium1_CNT.json",
        "hash_output": "Medium1_HASH.json"
      },
      "structure": {
        "root_id": "RS1_RS1",
        "output_suffix": "medium1"
      }
    },
    "medium2": { ... }
  },
  "blazegraph": { ... },
  "pipeline": { ... },
  "metadata": { ... }
}
```

You can define as many directories as needed, each corresponding to a physical medium or a logical partition of the archive (e.g. floppy disks, hard drives, tapes, etc.).

If `directory_config.json` is not present, each script falls back to its own hardcoded local configuration (the Evangelisti Archive settings, used as a reference).


### URIs

All RDF resources in the graph are minted using a base URI that must be updated to reflect your institution and archive.

**`structure_generation.py`** — single variable in the `Config` dataclass:
```python
BASE_URL: str = "http://your-institution.org/YourArchive/"
```

**`pipeline_test.py`** — inside the `NGRegistryGenerator` class:
```python
self.base_uri = "http://your-institution.org/YourArchive"

Two values are hardcoded in the `NGRegistryGenerator` class inside `pipeline.py` and must be updated for a different archive:

```python
# Root URI of the archive
self.base_uri = "http://your-institution.org/YourArchive"

# Also update the mapping from directory keys to archival structure identifiers
self.directory_structure_mapping = {
    'medium1': 'RS1_RS1',
    'medium2': 'RS1_RS2',
}
```
**`metadata_extraction.py`** — the base URI appears in multiple places throughout the file, both in a `BASE_URIS` dictionary and in individual `URIRef` calls. 

### required code change

In `pipeline.py`, update `PYTHON_INTERPRETER` with your Python environment path:

```python
PYTHON_INTERPRETER = '/usr/bin/python3'        # Linux
PYTHON_INTERPRETER = '/opt/conda/bin/python3'  # Anaconda
```

---

## Running the pipeline

### 1. Start Blazegraph

```bash
cd blazegraph_journal/
java -jar blazegraph.jar
```

The server listens by default on `http://localhost:9999/blazegraph`.

### 2. Run the pipeline

```bash
# All configured directories
python pipeline.py

# Specific directories only
python pipeline.py --directories medium1 medium2

# Dry-run: shows what would be executed without running it
python pipeline.py --dry-run

# Custom log file
python pipeline.py --log my_archive.log
```

---


## Pipeline steps

| Step | What it does | Script | Output |
|------|-------------|--------|--------|
| 1 | Counts files in each directory | `file_count.py` | `*_CNT.json` |
| 2 | Calculates SHA-256 hashes | `hash_calc.py` | `*_HASH.json` |
| 3 | Generates the RDF structure | `structure_generation.py` | `structure_*.nq` |
| 4 | Loads the structure into Blazegraph | `blazegraph_loader.py` | — |
| 5 | Post-structure backup | `pipeline.py` | `backups/*.nq` |
| 6| Verifies counts via SPARQL | `count_check.py` | — |
| 7 | Verifies hash integrity via SPARQL | `integrity_check.py` | — |
| 8 | Extracts technical metadata (Tika, ExifTool) | `metadata_extraction.py` | `*_TechMeta_*.nq` |
| 9 | Recalculates hashes to verify files were not modified | `hash_calc.py` | — |
| 10 | Loads metadata into Blazegraph | `blazegraph_loader.py` | — |
| 11 | Post-metadata backup | `pipeline.py` | `backups/*.nq` |
| 12 | Generates the Named Graph Registry (index of loaded graphs) | `pipeline.py` | `NGRegistry.nq` |
| 13 | Final backup | `pipeline.py` | `backups/*.nq` |

`pipeline.py` orchestrates all steps: it invokes `file_count.py`, `hash_calc.py`, `structure_generation.py`, `count_check.py`, `integrity_check.py` and `metadata_extraction.py` as subprocesses, imports `blazegraph_loader.py` directly, and handles backups and Named Graph Registry generation within its own code.

---

## Expected output

At the end of the pipeline, the working directory will contain:

```
*_CNT.json                        # file counts per directory
*_HASH.json                       # SHA-256 hashes per directory
structure_*.nq                    # RDF structure
*_TechMeta_*.nq                   # technical metadata (FileSystem, Tika, ExifTool)
NGRegistry.nq                     # named graph index
pipeline_report_complete_*.json   # final report
pipeline_evangelisti.log          # full execution log
backups/                          # automatic database backups
```

---

## Notes

- **Automatic reset**: at startup the pipeline clears the Blazegraph database. To disable this, set `'reset_on_start': False` in `BLAZEGRAPH_RESET_CONFIG` in `pipeline.py`.
- **Backups**: the 10 most recent backups are kept automatically; older ones are deleted.
- **Logs**: in case of errors, `pipeline_evangelisti.log` contains the full output of every subprocess.
- **`journal_restore.py`**: standalone utility to restore the database from a `.nq` backup file; not called by the pipeline.
