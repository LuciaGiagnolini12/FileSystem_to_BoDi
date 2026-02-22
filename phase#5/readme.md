# Born-Digital Archive Pipeline — Phase 5

Phase 5 applies privacy protection to the graph. It classifies every Record and RecordSet as either *to be anonymised* or *to be protected*, then rewrites the relevant triples in place across all named graphs.

---

## How it works

`step_1_privacy_protection.py` runs six sequential phases:

1. **Entity retrieval** — loads all Records and RecordSets from the three structure graphs
2. **Classification** — decides what to anonymise and what to protect (see logic below)
3. **Anonymisation** — rewrites labels, titles, and author metadata for sensitive entities
4. **Consistency check — titles** — verifies that every Title entity linked to an anonymised Record/RecordSet has also been anonymised; fixes any gaps
5. **Author check** — scans protected entities for author metadata fields that contain unauthorised names and anonymises them selectively
6. **Consistency check — protected metadata** — verifies that technical metadata fields in the `PROTECTED_TECH_METADATA_TYPES` list have not been accidentally anonymised

A Blazegraph journal backup is created automatically before any writes unless `--skip-backup` is passed.

---

## Classification logic

| Condition | Decision |
|-----------|----------|
| Entity is linked to an `lrmoo:F1_Work` | **Protected** |
| Entity URI is in `whitelist.xlsx` | **Protected** |
| Entity URI is in `blacklist.xlsx` | **Anonymised** |
| Entity is a hierarchical child of a blacklisted entity (via `rico:isOrWasIncludedIn`) | **Anonymised** |
| Entity is both blacklisted and work-linked / whitelisted | **Protected** (takes precedence) |
| Everything else | **Protected** |

In other words the default is to protect: only explicit blacklist membership (or hierarchy descent from a blacklisted node) triggers anonymisation.

---

## What anonymisation does

**For Records and RecordSets marked for anonymisation:**
- `rdfs:label` → `"Anonymized information"`
- `rico:title` → `"Anonymized information"`
- `bodi:redactedInformation` → `"yes"`
- All linked `rico:Title` entities → `rdfs:label` replaced in the `updated_relations` graph

**For their Instantiations:**
- `rdfs:label` → `"Anonymized information"`

**For their technical metadata (whitelist approach — only author fields):**
The following metadata types are anonymised, and only these:
`Creator`, `dc:creator`, `Author`, `LastModifiedBy`, `meta:last-author`

All other technical metadata (file size, MIME type, dates, filesystem attributes, etc.) is **never touched** regardless of the Record's privacy status.

**For protected entities:**
- `bodi:redactedInformation` → `"no"`
- Author metadata fields are checked independently: if the value is not in the `AUTHORIZED_AUTHORS` or `NEUTRAL_AUTHOR_PATTERNS` lists, it is anonymised even on a protected Record.

---

## Requirements

```bash
pip install requests pandas openpyxl aiohttp tqdm psutil
```

---

## Input files

Two spreadsheets must be present in the working directory (missing files are tolerated — the corresponding list is treated as empty):

| File | Content |
|------|---------|
| `blacklist.xlsx` | One URI per row (first column). Entities to anonymise. |
| `whitelist.xlsx` | One URI per row (first column). Entities to force-protect even if they would otherwise be anonymised. |

URIs may be written bare (`http://...`) or wrapped in angle brackets (`<http://...>`).

---

## Adapting the script to a different archive

Several constants at the top of the file must be updated:

**Named graphs:**
```python
NAMED_GRAPH_URIS = {
    "RS1_RS1": "http://your-institution.org/YourArchive/structure/RS1_RS1",
    ...
}

TECHNICAL_METADATA_GRAPH_URIS = [
    "http://your-institution.org/YourArchive/AT_TechMeta_...",
    ...
]

UPDATED_RELATIONS_GRAPH = "http://your-institution.org/YourArchive/updated_relations"
```

**Authorised author values** — names that should never be anonymised even in author metadata fields:
```python
AUTHORIZED_AUTHORS = {
    "your author", "author surname", ...
}
```

**Neutral author patterns** — software names, generic usernames, and other non-personal values that can be left visible:
```python
NEUTRAL_AUTHOR_PATTERNS = {
    'admin', 'microsoft', 'system', ...
}
```

**Protected technical metadata types** — fields that must never be anonymised regardless of context:
```python
PROTECTED_TECH_METADATA_TYPES = {
    "FileSize", "MIMEType", "CreateDate", "st_mtime", ...
}
```

**Backup path** — the script tries a list of known paths to find the Blazegraph journal. Add the correct path for your installation at the top of `BLAZEGRAPH_POSSIBLE_PATHS`.

---

## Running the script

```bash
# Standard run (creates backup first)
python step_1_privacy_protection.py

# Skip backup (faster, use only if a recent backup already exists)
python step_1_privacy_protection.py --skip-backup
```

There is no dry-run mode. Run the script on a test Blazegraph instance or ensure a backup exists before proceeding on production data.

---

## Output

The script modifies triples **in place** across the structure graphs, the `updated_relations` graph, and all technical metadata graphs. There is no separate output file.

```
fast_title_anonymization.log    # full execution log
```

The exit code is `0` if all consistency checks pass, `1` if any anomaly is detected (protected metadata anonymised, or works not correctly protected).

---

## Notes

- **Idempotency**: re-running the script is safe. Entities already marked `bodi:redactedInformation "yes"` or `"no"` will be overwritten to their correct value, and the consistency checks will catch any residual mismatches.
- **Performance**: the script uses an async SPARQL client with a connection pool. Large archives with tens of thousands of entities are processed in parallel batches. Expected throughput depends on Blazegraph performance.
- **Backup**: the journal backup verifies MD5 integrity after copying. If the hash does not match, the backup is discarded and the script aborts. Up to 5 backup copies are kept; older ones are deleted automatically.
- **Interruption**: unlike Phase 4 there is no checkpoint system. If the process is interrupted mid-run, re-execute from the beginning — the writes already performed are harmless to repeat.
