import csv
import json
from pathlib import Path
from typing import Dict, Iterable, List
import xml.etree.ElementTree as ET

import pandas as pd

def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def export_json(records: List[Dict], path: Path) -> None:
    _ensure_parent(path)
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

def export_csv(records: List[Dict], path: Path) -> None:
    _ensure_parent(path)
    fieldnames = sorted({k for r in records for k in r.keys()}) if records else []
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

def export_excel(records: List[Dict], path: Path) -> None:
    _ensure_parent(path)
    df = pd.DataFrame.from_records(records)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="results")

def export_xml(records: Iterable[Dict], path: Path) -> None:
    _ensure_parent(path)
    root = ET.Element("records")
    for r in records:
        rec = ET.SubElement(root, "record")
        for k, v in r.items():
            child = ET.SubElement(rec, k)
            child.text = "" if v is None else str(v)
    tree = ET.ElementTree(root)
    tree.write(path, encoding="utf-8", xml_declaration=True)

def export_records(records: List[Dict], path: Path, fmt: str) -> None:
    fmt = fmt.lower().strip()
    if fmt == "json":
        export_json(records, path)
    elif fmt == "csv":
        export_csv(records, path)
    elif fmt in {"excel", "xlsx"}:
        export_excel(records, path)
    elif fmt == "xml":
        export_xml(records, path)
    else:
        raise ValueError(f"Unsupported export format: {fmt}")