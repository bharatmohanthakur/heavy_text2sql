"""Build operator CSVs that match data/edfi/sample_demo.sqlite.

Two artifacts (gitignored under data/edfi/):
  * sample_demo_schema.csv        — Q1 format
  * sample_demo_relationships.csv — Q2 format

Together with sample_demo.sqlite these are the zero-infra demo input
for the operator-CSV pivot path: `text2sql ingest-csvs` validates them,
`build-table-catalog-cmd --from-csvs` builds the catalog from CSV +
live SQLite, no Ed-Fi GitHub touch.
"""

from __future__ import annotations

import csv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = REPO_ROOT / "data" / "edfi" / "sample_demo_schema.csv"
REL_PATH = REPO_ROOT / "data" / "edfi" / "sample_demo_relationships.csv"


# (Ranking, Domain, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, Populated)
_SCHEMA_ROWS = [
    # School
    (1, "EducationOrganization", "edfi", "School", "SchoolId",          "Yes"),
    (1, "EducationOrganization", "edfi", "School", "NameOfInstitution", "Yes"),
    (1, "EducationOrganization", "edfi", "School", "City",              "Yes"),
    (1, "EducationOrganization", "edfi", "School", "StateAbbreviation", "Yes"),

    # Student
    (0, "Student", "edfi", "Student", "StudentUSI",      "Yes"),
    (0, "Student", "edfi", "Student", "StudentUniqueId", "Yes"),
    (0, "Student", "edfi", "Student", "FirstName",       "Yes"),
    (0, "Student", "edfi", "Student", "LastSurname",     "Yes"),
    (0, "Student", "edfi", "Student", "BirthDate",       "Yes"),

    # Staff
    (0, "Staff", "edfi", "Staff", "StaffUSI",      "Yes"),
    (0, "Staff", "edfi", "Staff", "StaffUniqueId", "Yes"),
    (0, "Staff", "edfi", "Staff", "FirstName",     "Yes"),
    (0, "Staff", "edfi", "Staff", "LastSurname",   "Yes"),
    (0, "Staff", "edfi", "Staff", "HireDate",      "Yes"),

    # Course
    (1, "Course", "edfi", "Course", "CourseCode",      "Yes"),
    (1, "Course", "edfi", "Course", "CourseTitle",     "Yes"),
    (1, "Course", "edfi", "Course", "NumberOfCredits", "Yes"),

    # StudentSchoolAssociation
    (0, "Enrollment", "edfi", "StudentSchoolAssociation", "StudentUSI",          "Yes"),
    (0, "Enrollment", "edfi", "StudentSchoolAssociation", "SchoolId",            "Yes"),
    (0, "Enrollment", "edfi", "StudentSchoolAssociation", "EntryDate",           "Yes"),
    (0, "Enrollment", "edfi", "StudentSchoolAssociation", "GradeLevelDescriptor","Yes"),

    # CourseOffering
    (1, "Course", "edfi", "CourseOffering", "CourseCode",        "Yes"),
    (1, "Course", "edfi", "CourseOffering", "SchoolId",          "Yes"),
    (1, "Course", "edfi", "CourseOffering", "SchoolYear",        "Yes"),
    (1, "Course", "edfi", "CourseOffering", "SectionIdentifier", "Yes"),
]


# (FK_Name, Parent_Table, Parent_Column, Referenced_Table, Referenced_Column,
#  Parent_Schema, Referenced_Schema)
_REL_ROWS = [
    ("FK_SSA_Student",   "StudentSchoolAssociation", "StudentUSI", "Student", "StudentUSI", "edfi", "edfi"),
    ("FK_SSA_School",    "StudentSchoolAssociation", "SchoolId",   "School",  "SchoolId",   "edfi", "edfi"),
    ("FK_CO_Course",     "CourseOffering",           "CourseCode", "Course",  "CourseCode", "edfi", "edfi"),
    ("FK_CO_School",     "CourseOffering",           "SchoolId",   "School",  "SchoolId",   "edfi", "edfi"),
]


_SCHEMA_HEADERS = [
    "Ranking", "Domain", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "Populated",
]
_REL_HEADERS = [
    "FK_Name", "Parent_Table", "Parent_Column",
    "Referenced_Table", "Referenced_Column",
    "Parent_Schema", "Referenced_Schema",
]


def build() -> tuple[Path, Path]:
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SCHEMA_PATH.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(_SCHEMA_HEADERS)
        w.writerows(_SCHEMA_ROWS)
    with REL_PATH.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(_REL_HEADERS)
        w.writerows(_REL_ROWS)
    return SCHEMA_PATH, REL_PATH


if __name__ == "__main__":
    sp, rp = build()
    print(f"Wrote {sp}  ({len(_SCHEMA_ROWS)} column rows)")
    print(f"Wrote {rp}  ({len(_REL_ROWS)} FK rows)")
