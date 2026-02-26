# Born-Digital Archive Pipeline - Phase 1

Automated pipeline for indexing, verifying, and loading born-digital personal archives into a knowledge graph modeled with [Records in Contexts-Ontology (RiC-O)(https://www.ica.org/standards/RiC/ontology)] and the [Born-Digital Ontology (BoDi)(http://w3id.org/bodi#)].

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

### System
- Java ≥ 8
- ExifTool — [download](https://exiftool.org/) or:
  - macOS: `brew install exiftool`
  - Linux: `sudo apt install libimage-exiftool-perl`
- `sha256sum` (Linux, preinstalled) / `shasum` (macOS, preinstalled)

### Java tools (download JARs manually)
- [Blazegraph](https://github.com/blazegraph/database/releases) (`blazegraph.jar`)
- [Apache Tika](https://tika.apache.org/download.html) (`tika-app.jar`)

### Python ≥ 3.8
pip install requests>=2.28 rdflib>=6.0 SPARQLWrapper>=2.0 psutil>=5.9

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

The keys under `directories` (e.g. `medium1`, `medium2`) identify each medium and become the names usable with `--directories` on the command line. They should be adapted to reflect your archive's physical media or logical partitions — the defaults in the code correspond to the Evangelisti Archive (`floppy`, `hd`, `hdesterno`).

> **Note:** `output_suffix`, `log_suffix`, and `metadata.directory` must be **lowercase** and must all match each other within the same directory entry.

If `directory_config.json` is not present, each script falls back to its own hardcoded local configuration (the Evangelisti Archive settings, used as a reference).

### URIs

All RDF resources in the graph are minted using a base URI that must be updated to reflect your institution and archive.

**`structure_generation.py`** — single variable in the `Config` dataclass:
```python
BASE_URL: str = "http://your-institution.org/YourArchive/"
```

**`pipeline.py`** — inside the `NGRegistryGenerator` class:

```python
# Root URI of the archive
self.base_uri = "http://your-institution.org/YourArchive"

# Mapping from directory keys to archival structure identifiers
self.directory_structure_mapping = {
    'medium1': 'RS1_RS1',
    'medium2': 'RS1_RS2',
}
```

**`metadata_extraction.py`** — the base URI appears in multiple places throughout the file, both in a `BASE_URIS` dictionary and in individual `URIRef` calls.

### Required code change

In `pipeline.py`, update `PYTHON_INTERPRETER` with your Python environment path:

```python
PYTHON_INTERPRETER = '/usr/bin/python3'        # Linux
PYTHON_INTERPRETER = '/opt/conda/bin/python3'  # Anaconda
```

---

## Running the pipeline

Blazegraph and Apache Tika are started automatically — no manual setup is required before launching the pipeline.

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
