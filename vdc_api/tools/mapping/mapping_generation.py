import logging
import json
from pathlib import Path
from urllib.parse import urlparse

from rdflib import Graph, Namespace, BNode, Literal, RDF, URIRef


logger = logging.getLogger(__name__)

RR = Namespace("http://www.w3.org/ns/r2rml#")
EX = Namespace("http://example.com/")

INPUT_DIR = Path(__file__).parent.parent / "ontop/input"
MAPPINGS_DIR = INPUT_DIR / "mappings"
ONTOLOGIES_DIR = INPUT_DIR / "ontologies"
MAPPING_FILE = INPUT_DIR / "mapping.ttl"
ONTOLOGY_FILE = INPUT_DIR / "ontology.ttl"


SCHEMA_TO_XSD = {
    "boolean": "http://www.w3.org/2001/XMLSchema#boolean",
    "date": "http://www.w3.org/2001/XMLSchema#date",
    "datetime": "http://www.w3.org/2001/XMLSchema#dateTime",
    "float": "http://www.w3.org/2001/XMLSchema#decimal",
    "integer": "http://www.w3.org/2001/XMLSchema#integer",
    "number": "http://www.w3.org/2001/XMLSchema#decimal",
    "text": "http://www.w3.org/2001/XMLSchema#string",
    "time": "http://www.w3.org/2001/XMLSchema#time",
}


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _normalize_datatype_token(data_type: str | None) -> str | None:
    if not data_type:
        return None

    token = data_type.strip()
    if not token:
        return None

    if token.startswith("http://") or token.startswith("https://"):
        path = urlparse(token).path.rsplit("/", 1)[-1]
        return path.lower() if path else None

    if ":" in token:
        return token.rsplit(":", 1)[-1].lower()

    return token.lower()


def _sample_value_is_complex(value) -> bool:
    if isinstance(value, (list, dict)):
        return True

    if not isinstance(value, str):
        return False

    text = value.strip()
    if not text:
        return False

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return text.startswith("[[") or text.startswith("{{") or text.startswith("[")

    return isinstance(parsed, (list, dict))


def _infer_field_strategy(field_name: str, field_info: dict) -> dict:
    normalized_type = _normalize_datatype_token(field_info.get("data_type"))
    xsd_type = SCHEMA_TO_XSD.get(normalized_type)

    if any(
        _sample_value_is_complex(sample) for sample in field_info.get("samples", [])
    ):
        return {"mode": "serialized", "datatype": SCHEMA_TO_XSD["text"]}

    if xsd_type:
        return {"mode": "direct", "datatype": xsd_type}

    if field_name.lower() in {"latitude", "longitude"}:
        return {"mode": "direct", "datatype": SCHEMA_TO_XSD["number"]}

    return {"mode": "direct", "datatype": None}


def _build_projection_sql(source_column: str, alias: str, mode: str) -> str:
    source_expr = _quote_identifier(source_column)
    alias_expr = _quote_identifier(alias)

    if mode == "serialized":
        return f"CAST({source_expr} AS VARCHAR) AS {alias_expr}"

    return f"{source_expr} AS {alias_expr}"


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


def generate_mappings(croissant_dict, source_id: str, schema_name: str = "public"):
    mapping_file = MAPPINGS_DIR / f"{source_id}.ttl"
    ontology_file = ONTOLOGIES_DIR / f"{source_id}.ttl"
    ontology = generate_ontology(croissant_dict, source_id, schema_name)
    mappings = generate_mappings_file(croissant_dict, source_id, schema_name)

    MAPPINGS_DIR.mkdir(parents=True, exist_ok=True)
    ONTOLOGIES_DIR.mkdir(parents=True, exist_ok=True)

    with open(mapping_file, "w") as f:
        f.write(mappings.serialize(format="turtle"))

    with open(ontology_file, "w") as f:
        f.write(ontology.serialize(format="turtle"))


def generate_mappings_file(croissant_dict, source_id: str, schema_name: str = "public"):
    dataset_id = croissant_dict.get("@id", "unknown_dataset")

    mappings = Graph()
    mappings.bind("rr", RR)
    extracted_schema = extract_schema(croissant_dict)
    for index, (table, details) in enumerate(extracted_schema.items(), start=1):
        table_name = details.get("recordset_name", table)
        field_specs = []
        projection_sql = []

        for field in details["columns"]:
            field_name = details.get("column_names", {}).get(field, field)
            field_info = details.get("field_metadata", {}).get(field, {})
            source_column = field_info.get("source_column") or field_name
            strategy = _infer_field_strategy(field_name, field_info)

            field_specs.append(
                {
                    "field": field,
                    "field_name": field_name,
                    "source_column": source_column,
                    "strategy": strategy,
                }
            )
            projection_sql.append(
                _build_projection_sql(source_column, field_name, strategy["mode"])
            )

        triples_map = URIRef(f"#TripleMapping{index}")
        mappings.add((triples_map, RDF.type, RR.TriplesMap))
        logical_table = BNode()
        mappings.add((triples_map, RR.logicalTable, logical_table))
        mappings.add((logical_table, RDF.type, RR.LogicalTable))
        sql_query = (
            f"SELECT {', '.join(projection_sql)}, UUID() AS uuid "
            f"FROM {_quote_identifier("ds_" + source_id)}.{schema_name}.{_quote_identifier(table_name)}"
        )
        mappings.add((logical_table, RR.sqlQuery, Literal(sql_query)))

        subject_map = BNode()
        mappings.add((triples_map, RR.subjectMap, subject_map))
        mappings.add((subject_map, RDF.type, RR.SubjectMap))
        mappings.add(
            (
                subject_map,
                RR.template,
                Literal(f"http://example.com/{dataset_id}/{table_name}/{{uuid}}"),
            )
        )

        for field_spec in field_specs:
            field_name = field_spec["field_name"]
            predicate_object_map = BNode()
            mappings.add((triples_map, RR.predicateObjectMap, predicate_object_map))
            mappings.add((predicate_object_map, RDF.type, RR.PredicateObjectMap))
            mappings.add(
                (
                    predicate_object_map,
                    RR.predicate,
                    URIRef(f"http://example.com/{dataset_id}/{table}#{field_name}"),
                )
            )

            object_map = BNode()
            mappings.add((predicate_object_map, RR.objectMap, object_map))
            mappings.add((object_map, RDF.type, RR.ObjectMap))
            mappings.add((object_map, RR.column, Literal(field_name)))
            datatype = field_spec["strategy"].get("datatype")
            if datatype:
                mappings.add((object_map, RR.datatype, URIRef(datatype)))

    return mappings


def generate_ontology(croissant_dict, source_id: str, schema_name: str = "public"):
    dataset_id = croissant_dict.get("@id", "unknown_dataset")
    croissant_graph = Graph()
    croissant_graph.parse(data=json.dumps(croissant_dict), format="json-ld")
    ontology = Graph()
    ontology.bind("ex", EX)

    extracted_schema = extract_schema(croissant_dict)
    print(json.dumps(extracted_schema, indent=2))
    # Generation of the classes in the ontology
    for table, details in extracted_schema.items():
        recordset_name = details.get("recordset_name", table)
        if not isBinaryTable(table, details):
            ontology.add(
                (
                    URIRef(f"http://example.com/{dataset_id}/{recordset_name}"),
                    RDF.type,
                    URIRef("http://www.w3.org/2002/07/owl#Class"),
                )
            )
            ontology.add(
                (
                    URIRef(f"http://example.com/{dataset_id}/{recordset_name}"),
                    URIRef("http://www.w3.org/ns/prov#wasDerivedFrom"),
                    URIRef(f"http://example.com/{source_id}/{table}"),
                )
            )

    # Generation of the properties in the ontology
    for table, details in extracted_schema.items():
        recordset_name = details.get("recordset_name", table)
        for field in details["columns"]:
            field_name = details.get("column_names", {}).get(field, field)

            if not isBinaryTable(table, details):
                ontology.add(
                    (
                        URIRef(f"http://example.com/{dataset_id}/{table}#{field_name}"),
                        URIRef("http://www.w3.org/2000/01/rdf-schema#domain"),
                        URIRef(f"http://example.com/{dataset_id}/{recordset_name}"),
                    )
                )
                ontology.add(
                    (
                        URIRef(f"http://example.com/{dataset_id}/{table}#{field_name}"),
                        URIRef("http://www.w3.org/ns/prov#wasDerivedFrom"),
                        URIRef(f"http://example.com/{source_id}/{field}"),
                    )
                )
                for fk in details["foreign_keys"]:
                    if fk["column"] == field:
                        target_table, target_pk = fk["references"]
                        ontology.add(
                            (
                                URIRef(
                                    f"http://example.com/{dataset_id}/{table}#{field_name}"
                                ),
                                RDF.type,
                                URIRef("http://www.w3.org/2002/07/owl#ObjectProperty"),
                            )
                        )
                        ontology.add(
                            (
                                URIRef(
                                    f"http://example.com/{dataset_id}/{table}#{field_name}"
                                ),
                                URIRef("http://www.w3.org/2000/01/rdf-schema#range"),
                                URIRef(
                                    f"http://example.com/{dataset_id}/{target_table}"
                                ),
                            )
                        )
                        break
                else:
                    ontology.add(
                        (
                            URIRef(
                                f"http://example.com/{dataset_id}/{table}#{field_name}"
                            ),
                            RDF.type,
                            URIRef("http://www.w3.org/2002/07/owl#DatatypeProperty"),
                        )
                    )

                    query = f"""PREFIX cr: <http://mlcommons.org/croissant/>
                            SELECT ?dataType WHERE {{ <file:///Users/zoech/Documents/projects/datagems/code/mapping-generation/{field}> cr:dataType ?dataType. }}"""
                    # print(query)
                    results = croissant_graph.query(query)
                    for row in results:
                        dataType = (
                            row.dataType.value
                            if hasattr(row.dataType, "value")
                            else str(row.dataType)
                        )
                        if dataType != "None":
                            ontology.add(
                                (
                                    URIRef(
                                        f"http://example.com/{dataset_id}/{table}#{field_name}"
                                    ),
                                    URIRef(
                                        "http://www.w3.org/2000/01/rdf-schema#range"
                                    ),
                                    URIRef(dataType),
                                )
                            )

            # Binary tables
            else:
                ontology.add(
                    (
                        URIRef(f"http://example.com/{dataset_id}/{recordset_name}"),
                        RDF.type,
                        URIRef("http://www.w3.org/2002/07/owl#ObjectProperty"),
                    )
                )
                domain_table, range_table = None, None
                for fk in details["foreign_keys"]:
                    if domain_table is None:
                        domain_table, domain_pk = fk["references"]
                    elif range_table is None:
                        range_table, range_pk = fk["references"]
                ontology.add(
                    (
                        URIRef(f"http://example.com/{dataset_id}/{recordset_name}"),
                        URIRef("http://www.w3.org/2000/01/rdf-schema#domain"),
                        URIRef(f"http://example.com/{dataset_id}/{domain_table}"),
                    )
                )
                ontology.add(
                    (
                        URIRef(f"http://example.com/{dataset_id}/{recordset_name}"),
                        URIRef("http://www.w3.org/2000/01/rdf-schema#range"),
                        URIRef(f"http://example.com/{dataset_id}/{range_table}"),
                    )
                )
    return ontology


def extract_schema(croissant_data):
    def uuid_tail(term):
        value = str(term)
        if not value:
            return value
        return value.rstrip("/").split("/")[-1]

    schema = {}
    graph = Graph()
    if isinstance(croissant_data, dict):
        croissant_data = json.dumps(croissant_data)
    graph.parse(data=croissant_data, format="json-ld")

    # Query 1: extract UUID IDs and optional human-readable names.
    core_query = """PREFIX cr: <http://mlcommons.org/croissant/>
        PREFIX sc: <https://schema.org/>
        SELECT DISTINCT ?recordSet ?recordSetName ?field ?fieldName ?sourceColumn ?dataType ?sample ?pKey ?pKeyName
        WHERE {
            ?recordSet a cr:RecordSet ;
                       cr:field ?field .
            OPTIONAL { ?recordSet sc:name ?recordSetName . }
            ?field a cr:Field .
            OPTIONAL { ?field sc:name ?fieldName . }
            OPTIONAL { ?field cr:dataType ?dataType . }
            OPTIONAL { ?field cr:sample ?sample . }
            OPTIONAL {
                ?field cr:source ?source .
                ?source cr:extract ?extract .
                ?extract cr:column ?sourceColumn .
            }
            OPTIONAL {
                ?recordSet cr:key ?pKey .
                OPTIONAL { ?pKey sc:name ?pKeyName . }
            }
        }
    """

    for row in graph.query(core_query):
        table_name = uuid_tail(row.recordSet)
        table_label = str(row.recordSetName) if row.recordSetName else table_name
        column_name = uuid_tail(row.field)
        column_label = str(row.fieldName) if row.fieldName else column_name
        source_column = str(row.sourceColumn) if row.sourceColumn else column_label
        data_type = str(row.dataType) if row.dataType else None
        sample = str(row.sample) if row.sample else None
        primary_key = uuid_tail(row.pKey) if row.pKey else None
        primary_key_label = (
            str(row.pKeyName)
            if row.pKeyName
            else (primary_key if primary_key else None)
        )

        if table_name not in schema:
            schema[table_name] = {
                "recordset_name": table_label,
                "columns": [],
                "primary_key": [],
                "foreign_keys": [],
                "column_names": {},
                "primary_key_names": {},
                "field_metadata": {},
            }

        if column_name not in schema[table_name]["columns"]:
            schema[table_name]["columns"].append(column_name)

        schema[table_name]["column_names"][column_name] = column_label
        field_metadata = schema[table_name]["field_metadata"].setdefault(
            column_name,
            {
                "source_column": source_column,
                "data_type": data_type,
                "samples": [],
            },
        )
        if data_type and not field_metadata.get("data_type"):
            field_metadata["data_type"] = data_type
        if source_column and not field_metadata.get("source_column"):
            field_metadata["source_column"] = source_column
        if sample and sample not in field_metadata["samples"]:
            field_metadata["samples"].append(sample)

        if primary_key and primary_key not in schema[table_name]["primary_key"]:
            schema[table_name]["primary_key"].append(primary_key)

        if primary_key:
            schema[table_name]["primary_key_names"][primary_key] = primary_key_label

    # Query 2: extract FK relationships and names.
    fk_query = """PREFIX cr: <http://mlcommons.org/croissant/>
        PREFIX sc: <https://schema.org/>
        SELECT DISTINCT ?recordSet ?recordSetName ?field ?fieldName ?targetRecordSet ?targetRecordSetName ?targetKeyField ?targetKeyName
        WHERE {
            ?recordSet a cr:RecordSet ;
                       cr:field ?field .
            OPTIONAL { ?recordSet sc:name ?recordSetName . }
            ?field a cr:Field ;
                   cr:references ?targetKeyField .
            OPTIONAL { ?field sc:name ?fieldName . }

            ?targetRecordSet a cr:RecordSet ;
                             cr:key ?targetKeyField .
            OPTIONAL { ?targetRecordSet sc:name ?targetRecordSetName . }
            OPTIONAL { ?targetKeyField sc:name ?targetKeyName . }
        }
    """

    seen_fks = set()
    for row in graph.query(fk_query):
        table_name = uuid_tail(row.recordSet)
        table_label = str(row.recordSetName) if row.recordSetName else table_name
        column_name = uuid_tail(row.field)
        column_label = str(row.fieldName) if row.fieldName else column_name
        target_table = uuid_tail(row.targetRecordSet)
        target_table_label = (
            str(row.targetRecordSetName) if row.targetRecordSetName else target_table
        )
        target_pk = uuid_tail(row.targetKeyField)
        target_pk_label = str(row.targetKeyName) if row.targetKeyName else target_pk

        if table_name not in schema:
            schema[table_name] = {
                "recordset_name": table_label,
                "columns": [],
                "primary_key": [],
                "foreign_keys": [],
                "column_names": {},
                "primary_key_names": {},
                "field_metadata": {},
            }

        fk_key = (table_name, column_name, target_table, target_pk)
        if fk_key in seen_fks:
            continue
        seen_fks.add(fk_key)

        schema[table_name]["foreign_keys"].append(
            {
                "column": column_name,
                "column_name": column_label,
                "references": (target_table, target_pk),
                "references_name": (target_table_label, target_pk_label),
            }
        )

    return schema


def isBinaryTable(table, details):
    foreign_keys = details.get("foreign_keys", [])
    fk_columns = [
        fk.get("column")
        for fk in foreign_keys
        if isinstance(fk, dict) and fk.get("column")
    ]
    primary_keys = details.get("primary_key", [])
    columns = details.get("columns", [])

    if len(fk_columns) != 2:
        return False
    return set(fk_columns) == set(primary_keys) and set(fk_columns) == set(columns)
