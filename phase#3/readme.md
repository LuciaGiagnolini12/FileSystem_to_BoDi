# Born-Digital Archive Pipeline — Phase 3

Phase 3 adds a bibliographic layer to the graph built in Phases 1 and 2: intellectual works are modelled as `lrmoo:F1_Work` entities and linked to the Records and RecordSets that document them.

---

## How it works

`works_evangelisti.py` reads a spreadsheet listing which archive records correspond to which intellectual works, then generates `lrmoo:F1_Work` entities and links them to the Records and RecordSets already in the graph. Work-to-work hierarchical relationships (e.g. a cycle containing individual novels) are also modelled. When a work is linked to a RecordSet, the link is automatically propagated down to all its child Records.

Output is written both to a **N-Quads file** and to a **dedicated named graph** in Blazegraph.

---

## Requirements

```bash
pip install requests rdflib pandas openpyxl
```

---

## Adapting the script to a different archive

Three things to update in `works_evangelisti.py`:

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

---

## Running the script

```bash
python works_evangelisti.py
```

There are no CLI arguments: all configuration is done by editing the constants at the top of the file.

---

## What it produces

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
