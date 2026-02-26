# FileSystem_to_BoDi

Python workflow for generating RDF knowledge graphs from born-digital archives according to the BoDi ontology.

## Overview

Developed as part of Lucia Giagnolini's PhD research, this workflow translates the structure and metadata of born-digital archives into knowledge graphs, enabling structured querying and visualization through semantic web technologies. It aims to automate portions of the descriptive process for born-digital resources, thereby supporting curatorial and archival management activities.

The system takes a directory (or a set of directories) as input and produces its RDF representation compliant with the **BoDi (Born-Digital Archive Ontology)** model. The resulting graph allows a semantic representation of the archive's hierarchy, provenance, and relationships among digital objects.

For each phase of the workflow, a dedicated folder containing the corresponding Python scripts is included in this repository.

## Methodological Objectives

The workflow has been designed to pursue specific methodological goals:

- **Integrity preservation**: Every operation maintains the physical and logical integrity of original documents through cryptographic verification mechanisms and preventive backup systems that ensure complete traceability of transformations.

- **Reversibility and transparency**: All transformations are documented and reversible, maintaining a chain-of-custody that links physical documents to their semantic representations through verifiable and reproducible processes.

- **Standards compliance**: Generated representations adhere to documented representation models to ensure portability, interoperability, and reusability.

## Workflow Structure

The workflow is organized into five main phases, each with its own folder containing the Python code:

### Phase 1: Systematization of Existing Information

This foundational phase produces a formalized representation of the archive's content structure through ten progressive steps:

- Implementation of protection measures to ensure archive immutability
- Complete hierarchical census of files and directories
- Generation of SHA-256 cryptographic fingerprints for each file
- Systematic metadata extraction using three specialized tools:
  - Python `os` library for filesystem metadata
  - Apache Tika for content-based metadata
  - ExifTool for format-specific technical metadata
- Semantic transformation of captured information into RDF entities compliant with BoDi ontology
- Final integrity checks to ensure no operations have compromised the authenticity of original materials

**Output**: RDF files of the archive structure with comprehensive technical metadata.

### Phase 2: Rule-Based Validation and Enrichment

This phase operates exclusively in the semantic domain to validate logical coherence and make implicit knowledge explicit through controlled inference:

- **Validation**: SPARQL queries verify structural consistency, metadata presence, and relationship coherence
- **Semantic enrichment** through six operations:
  - Correlation of files with identical content via hash analysis
  - Alignment of metadata types extracted from different tools
  - Subdivision of metadata types into nine functional categories
  - Assignment of document types based on media types
  - Attribution of titles and dates (creation and modification) according to RiC-O model

**Output**: A validated and enriched knowledge base with explicit relationships and normalized metadata.

### Phase 3: Semantic Enrichment Through Domain Knowledge and Generative Models

This phase extends the information perimeter through integration of specialized knowledge sources and AI technologies:

- **Semi-automatic mapping**: Connection of archival documents to reference works using the LRMoo model (IFLA Library Reference Model)
- **Automated system**: AI (Llama) for producing natural language descriptions derived from metadata analysis

**Output**: Contextually enriched descriptions of files and folders.

### Phase 4: Provenance Documentation

This phase reconstructs the contexts of origin and documents the chain of custody:

- Census of different digital environments
- Reconstruction of their characteristics and temporal/processual relationships
- Documentation of connections between original media and working copies
- Representation of provenance data according to BoDi model

**Output**: A knowledge graph enriched with information about provenance contexts and custodial history.

### Phase 5: Anonymization for Publication

The final phase implements selective anonymization strategies that preserve structure and informational value while protecting sensitive information:

- Distinction between different types of archival resources
- Application of differentiated anonymization logic
- Customizable strategies based on specific needs and legal requirements

**Output**: A publication-ready RDF knowledge base compliant with General Data Protection Regulation (Regulation (EU) 2016/679) .

## Technical Stack

- **Metadata extraction tools**: Apache Tika, ExifTool
- **RDF serialization formats**: NQuads (.nq)
- **Query language**: SPARQL
- **Ontologies**: BoDi, RiC-O, LRMoo, CIDOC-CRM

## Output

The final output is an RDF knowledge base that mirrors the hierarchical and contextual structure of the original digital archive. It can be:

- Exported in standard formats 
- Queried through a SPARQL endpoint
- Integrated into semantic visualization platforms
- Published as Linked Open Data

## Case Study

The workflow has been experimentally applied to the **Valerio Evangelisti Archive**, using as test cases:

- Copy of the main computer hard disk
- Copy of the external hard disk
- Extracted contents from floppy disks

The output will be soon published in a dedicated ResearchSpace App.

## License

This work is licensed under a [Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/).

© 2025 Lucia Giagnolini

You are free to share and adapt this work, provided appropriate credit is given to the author.

## Contact

For questions or collaborations:
- **Lucia Giagnolini**: lucia.giagnolini2[at]unibo.it

