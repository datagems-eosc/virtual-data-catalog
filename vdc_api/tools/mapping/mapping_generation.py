import logging
from pathlib import Path

from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef  # noqa: F811

logger = logging.getLogger(__name__)

RR = Namespace("http://www.w3.org/ns/r2rml#")
EX = Namespace("http://example.com/")

INPUT_DIR = Path(__file__).parent.parent / "ontop/input"
MAPPINGS_DIR = INPUT_DIR / "mappings"
ONTOLOGIES_DIR = INPUT_DIR / "ontologies"
MAPPING_FILE = INPUT_DIR / "mapping.ttl"
ONTOLOGY_FILE = INPUT_DIR / "ontology.ttl"


def merge_mapping_files() -> None:
    """Concatenate all per-dataset .ttl files from mappings/ into mapping.ttl."""
    ttl_files = sorted(MAPPINGS_DIR.glob("*.ttl"))
    if not ttl_files:
        logger.warning("No .ttl files found in %s, skipping merge", MAPPINGS_DIR)
        return

    merged = Graph()
    merged.bind("rr", RR)
    merged.bind("ex", EX)
    for ttl_file in ttl_files:
        merged.parse(ttl_file, format="turtle")
        logger.info("Merged mapping file: %s", ttl_file.name)

    with open(MAPPING_FILE, "w") as f:
        f.write(merged.serialize(format="turtle"))
    logger.info(
        "Wrote merged mapping.ttl (%d triple maps across %d file(s))",
        len(merged),
        len(ttl_files),
    )


def merge_ontology_files() -> None:
    """Concatenate all per-dataset .ttl files from ontologies/ into ontology.ttl."""
    ttl_files = sorted(ONTOLOGIES_DIR.glob("*.ttl"))
    if not ttl_files:
        logger.warning("No .ttl files found in %s, skipping merge", ONTOLOGIES_DIR)
        return

    merged = Graph()
    for ttl_file in ttl_files:
        merged.parse(ttl_file, format="turtle")
        logger.info("Merged ontology file: %s", ttl_file.name)

    with open(ONTOLOGY_FILE, "w") as f:
        f.write(merged.serialize(format="turtle"))
    logger.info(
        "Wrote merged ontology.ttl (%d triples across %d file(s))",
        len(merged),
        len(ttl_files),
    )


def _has_type(node: dict, expected_type: str) -> bool:
    props = node.get("properties", {})
    if props.get("type") == expected_type:
        return True
    return expected_type in node.get("labels", [])


def _edge_endpoints(edge: dict) -> tuple[str | None, str | None]:
    return (
        edge.get("source") or edge.get("from") or edge.get("start"),
        edge.get("target") or edge.get("to") or edge.get("end"),
    )


def _build_subject_template(recordset_name: str, field_names: list[str]) -> str:
    id_col = next((name for name in field_names if name.lower() == "id"), None)
    if id_col:
        return f"http://example.com/{recordset_name}/{{{id_col}}}"

    preferred_columns = [
        name
        for name in ("latitude", "longitude", "type_code", "value", "time")
        if name in field_names
    ]
    subject_columns = preferred_columns or field_names
    placeholder_suffix = "/".join(f"{{{name}}}" for name in subject_columns)
    return f"http://example.com/{recordset_name}/{placeholder_suffix}"


def _append_unique_field(recordset: dict, field: dict) -> None:
    field_name = field.get("fieldName")
    if not field_name:
        return
    if any(existing.get("fieldName") == field_name for existing in recordset["fields"]):
        return
    recordset["fields"].append(field)


def generate_mappings(dataset_info, source_name: str, schema_name: str = "public"):
    mapping_file = MAPPINGS_DIR / f"{source_name}.ttl"
    ontology_file = ONTOLOGIES_DIR / f"{source_name}.ttl"
    record_set_nodes = {}
    record_set_ids = {}
    field_nodes = {}
    for node in dataset_info.get("nodes", []):
        props = node.get("properties", {})
        if _has_type(node, "cr:RecordSet"):
            name = props.get("name", "")
            if not name:
                continue
            record_set_ids[node["id"]] = name
            record_set_nodes.setdefault(name, {"name": name, "fields": []})
        elif _has_type(node, "cr:Field"):
            field_name = props.get("name", "")
            data_type = props.get("dataType")
            field_nodes[node["id"]] = {"fieldName": field_name, "dataType": data_type}

    edges = dataset_info.get("edges", [])
    linked_fields = 0
    if edges:
        for edge in edges:
            src, dst = _edge_endpoints(edge)
            src_recordset = record_set_ids.get(src)
            dst_recordset = record_set_ids.get(dst)
            if src_recordset and dst in field_nodes:
                _append_unique_field(record_set_nodes[src_recordset], field_nodes[dst])
                linked_fields += 1
            elif dst_recordset and src in field_nodes:
                _append_unique_field(record_set_nodes[dst_recordset], field_nodes[src])
                linked_fields += 1

    if not edges or linked_fields == 0:
        for rs in record_set_nodes.values():
            for field in field_nodes.values():
                _append_unique_field(rs, field)

    recordSets = record_set_nodes
    logger.info(
        "Found %d RecordSets with fields: %s",
        len(recordSets),
        {v["name"]: [f["fieldName"] for f in v["fields"]] for v in recordSets.values()},
    )

    ontology = generate_ontology(recordSets)
    mappings = generate_mapping(recordSets, source_name, schema_name)

    MAPPINGS_DIR.mkdir(parents=True, exist_ok=True)
    ONTOLOGIES_DIR.mkdir(parents=True, exist_ok=True)

    with open(mapping_file, "w") as f:
        f.write(mappings)

    with open(ontology_file, "w") as f:
        f.write(ontology)


def generate_ontology(recordSets):
    ontology = Graph()
    for recordSet, details in recordSets.items():
        ontology.add(
            (
                URIRef(f"http://example.com/{details['name']}"),
                RDF.type,
                URIRef("http://www.w3.org/2002/07/owl#Class"),
            )
        )
        for field in details["fields"]:
            ontology.add(
                (
                    URIRef(
                        f"http://example.com/{details['name']}#{field['fieldName']}"
                    ),
                    URIRef("http://www.w3.org/2000/01/rdf-schema#domain"),
                    URIRef(f"http://example.com/{details['name']}"),
                )
            )
            ontology.add(
                (
                    URIRef(
                        f"http://example.com/{details['name']}#{field['fieldName']}"
                    ),
                    RDF.type,
                    URIRef("http://www.w3.org/2002/07/owl#DatatypeProperty"),
                )
            )
            if field["dataType"] and field["dataType"].startswith(
                "http://www.w3.org/2001/XMLSchema#"
            ):
                ontology.add(
                    (
                        URIRef(
                            f"http://example.com/{details['name']}#{field['fieldName']}"
                        ),
                        URIRef("http://www.w3.org/2000/01/rdf-schema#range"),
                        URIRef(field["dataType"]),
                    )
                )
    return ontology.serialize(format="turtle")


def generate_mapping(recordSets, source_name: str, schema_name: str = "public"):
    mappings = Graph()
    mappings.bind("rr", RR)
    mappings.bind("ex", EX)

    for i, (recordSet, details) in enumerate(recordSets.items()):
        triples_map = URIRef(f"#TripleMap{i + 1}")
        mappings.add((triples_map, RDF.type, RR.TriplesMap))

        # Create LogicalTable
        logical_table = BNode()
        mappings.add((triples_map, RR.logicalTable, logical_table))
        mappings.add((logical_table, RDF.type, RR.LogicalTable))
        sql_query = f"SELECT * FROM {source_name}.public.{details['name']}"
        mappings.add((logical_table, RR.sqlQuery, Literal(sql_query)))

        # Build a concrete subject template from available columns.
        field_names = [
            f.get("fieldName", "") for f in details["fields"] if f.get("fieldName")
        ]
        subject_template = _build_subject_template(details["name"], field_names)

        # Create SubjectMap
        subject_map = BNode()
        mappings.add((triples_map, RR.subjectMap, subject_map))
        mappings.add((subject_map, RDF.type, RR.SubjectMap))
        mappings.add((subject_map, RR.template, Literal(subject_template)))
        mappings.add(
            (subject_map, RR["class"], URIRef(f"http://example.com/{details['name']}"))
        )

        # Create PredicateObjectMaps for each field
        for field in details["fields"]:
            field_name = field.get("fieldName")
            if not field_name:
                continue

            predicate_object_map = BNode()
            mappings.add((triples_map, RR.predicateObjectMap, predicate_object_map))
            mappings.add((predicate_object_map, RDF.type, RR.PredicateObjectMap))
            mappings.add(
                (
                    predicate_object_map,
                    RR.predicate,
                    URIRef(f"http://example.com/{details['name']}#{field_name}"),
                )
            )

            # ObjectMap
            object_map = BNode()
            mappings.add((predicate_object_map, RR.objectMap, object_map))
            mappings.add((object_map, RDF.type, RR.ObjectMap))
            mappings.add((object_map, RR.column, Literal(field_name)))

    return mappings.serialize(format="turtle")
