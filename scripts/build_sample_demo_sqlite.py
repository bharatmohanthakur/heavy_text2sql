"""Build data/edfi/sample_demo.sqlite — the zero-infra demo target.

Creates a small Ed-Fi-shaped SQLite database (~50 rows across 6 tables
with realistic FK relationships) so the `sqlite-demo` target_db
provider resolves to a usable file out of the box. The demo target is
what `text2sql serve` ships with when no operator-supplied DB is
configured, and what the architecture deck and the onboarding banner
walk a new user through on their first run.

This is NOT a real Ed-Fi populated template — it's a minimal stand-in
that exercises the parts of the pipeline that don't depend on the
full ApiModel:
  * reflect_unknown_tables (P7) walks the live DB and finds these
    tables with no ApiModel entry, tagging them domains=["Other"]
  * schema linking + JOIN expansion need real FK relationships
  * sample_rows + row_count populate the per-table semantic blob

Run this script once to (re)generate the file. Idempotent: drops and
recreates each table so re-running produces the same DB. The output
path is gitignored data; the script itself is committed so anyone
checking out the repo can regenerate it.

Schema mirrors a tiny slice of Ed-Fi DS 6.1.0:

  Student
    StudentUSI            INTEGER PK
    StudentUniqueId       TEXT UNIQUE
    FirstName / LastSurname
    BirthDate

  School
    SchoolId              INTEGER PK
    NameOfInstitution
    City / State

  Staff
    StaffUSI              INTEGER PK
    FirstName / LastSurname
    HireDate

  Course
    CourseCode            TEXT PK
    CourseTitle
    NumberOfCredits

  StudentSchoolAssociation       (composite PK; Student × School × EntryDate)
  CourseOffering                 (Course × School)

Approximate row counts: 20 students, 4 schools, 6 staff, 8 courses,
20 enrollments, 12 course offerings → ~70 rows total.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# Repo root is two levels up from scripts/.
REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "edfi" / "sample_demo.sqlite"


_SCHEMA_SQL = """
DROP TABLE IF EXISTS CourseOffering;
DROP TABLE IF EXISTS StudentSchoolAssociation;
DROP TABLE IF EXISTS Course;
DROP TABLE IF EXISTS Staff;
DROP TABLE IF EXISTS Student;
DROP TABLE IF EXISTS School;

CREATE TABLE School (
    SchoolId            INTEGER PRIMARY KEY,
    NameOfInstitution   TEXT    NOT NULL,
    City                TEXT,
    StateAbbreviation   TEXT
);

CREATE TABLE Student (
    StudentUSI          INTEGER PRIMARY KEY,
    StudentUniqueId     TEXT    NOT NULL UNIQUE,
    FirstName           TEXT    NOT NULL,
    LastSurname         TEXT    NOT NULL,
    BirthDate           TEXT
);

CREATE TABLE Staff (
    StaffUSI            INTEGER PRIMARY KEY,
    StaffUniqueId       TEXT    NOT NULL UNIQUE,
    FirstName           TEXT    NOT NULL,
    LastSurname         TEXT    NOT NULL,
    HireDate            TEXT
);

CREATE TABLE Course (
    CourseCode          TEXT    PRIMARY KEY,
    CourseTitle         TEXT    NOT NULL,
    NumberOfCredits     REAL    NOT NULL DEFAULT 1.0
);

CREATE TABLE StudentSchoolAssociation (
    StudentUSI          INTEGER NOT NULL,
    SchoolId            INTEGER NOT NULL,
    EntryDate           TEXT    NOT NULL,
    GradeLevelDescriptor TEXT,
    PRIMARY KEY (StudentUSI, SchoolId, EntryDate),
    FOREIGN KEY (StudentUSI) REFERENCES Student(StudentUSI),
    FOREIGN KEY (SchoolId) REFERENCES School(SchoolId)
);

CREATE TABLE CourseOffering (
    CourseCode          TEXT    NOT NULL,
    SchoolId            INTEGER NOT NULL,
    SchoolYear          INTEGER NOT NULL,
    SectionIdentifier   TEXT    NOT NULL,
    PRIMARY KEY (CourseCode, SchoolId, SchoolYear, SectionIdentifier),
    FOREIGN KEY (CourseCode) REFERENCES Course(CourseCode),
    FOREIGN KEY (SchoolId) REFERENCES School(SchoolId)
);
"""


_SCHOOLS = [
    (1, "Northridge High School",     "Northridge", "TX"),
    (2, "Northridge Middle School",   "Northridge", "TX"),
    (3, "Westwood Elementary",        "Westwood",   "TX"),
    (4, "Eastside Charter Academy",   "Eastside",   "TX"),
]

_STUDENTS = [
    (101, "S00101", "Ana",    "García",     "2008-04-12"),
    (102, "S00102", "Bilal",  "Khan",       "2007-11-23"),
    (103, "S00103", "Camila", "López",      "2009-02-05"),
    (104, "S00104", "Daniel", "Smith",      "2008-08-30"),
    (105, "S00105", "Elena",  "Petrov",     "2007-12-14"),
    (106, "S00106", "Farouk", "Adekunle",   "2008-06-19"),
    (107, "S00107", "Grace",  "O'Connor",   "2009-09-03"),
    (108, "S00108", "Hiroshi","Tanaka",     "2008-03-27"),
    (109, "S00109", "Imani",  "Williams",   "2007-10-08"),
    (110, "S00110", "Jorge",  "Rodríguez",  "2009-01-15"),
    (111, "S00111", "Kenji",  "Yamada",     "2008-05-22"),
    (112, "S00112", "Lina",   "Ibrahim",    "2007-07-04"),
    (113, "S00113", "Mateo",  "Fernández",  "2008-11-11"),
    (114, "S00114", "Nadia",  "Petrov",     "2009-04-19"),
    (115, "S00115", "Omar",   "Hassan",     "2008-09-25"),
    (116, "S00116", "Priya",  "Patel",      "2007-06-13"),
    (117, "S00117", "Quinn",  "Murphy",     "2008-12-31"),
    (118, "S00118", "Ravi",   "Sharma",     "2009-03-08"),
    (119, "S00119", "Sofia",  "Torres",     "2008-10-02"),
    (120, "S00120", "Tomás",  "Vásquez",    "2009-08-17"),
]

_STAFF = [
    (201, "T0201", "Alice",   "Henderson", "2018-08-15"),
    (202, "T0202", "Bernard", "Okafor",    "2015-08-15"),
    (203, "T0203", "Catherine","Liu",      "2020-08-15"),
    (204, "T0204", "Devang",  "Mehta",     "2017-08-15"),
    (205, "T0205", "Esther",  "Brown",     "2014-08-15"),
    (206, "T0206", "Felipe",  "Souza",     "2021-08-15"),
]

_COURSES = [
    ("ALG-1",   "Algebra I",                   1.0),
    ("ALG-2",   "Algebra II",                  1.0),
    ("BIO-1",   "Biology I",                   1.0),
    ("CHEM-1",  "Chemistry I",                 1.0),
    ("ENG-1",   "English Language Arts I",     1.0),
    ("HIST-1",  "U.S. History",                1.0),
    ("PE-1",    "Physical Education",          0.5),
    ("ART-1",   "Visual Arts",                 0.5),
]

# Each row: (StudentUSI, SchoolId, EntryDate, GradeLevel)
_ENROLLMENTS = [
    (101, 1, "2023-08-21", "Ninth grade"),
    (102, 1, "2023-08-21", "Tenth grade"),
    (103, 2, "2023-08-21", "Seventh grade"),
    (104, 1, "2023-08-21", "Ninth grade"),
    (105, 1, "2022-08-22", "Tenth grade"),
    (106, 4, "2023-08-21", "Ninth grade"),
    (107, 3, "2023-08-21", "Fifth grade"),
    (108, 1, "2023-08-21", "Ninth grade"),
    (109, 2, "2023-08-21", "Eighth grade"),
    (110, 3, "2023-08-21", "Fourth grade"),
    (111, 1, "2023-08-21", "Tenth grade"),
    (112, 4, "2022-08-22", "Tenth grade"),
    (113, 1, "2023-08-21", "Ninth grade"),
    (114, 3, "2023-08-21", "Fourth grade"),
    (115, 4, "2023-08-21", "Ninth grade"),
    (116, 2, "2023-08-21", "Seventh grade"),
    (117, 1, "2023-08-21", "Tenth grade"),
    (118, 1, "2023-08-21", "Ninth grade"),
    (119, 2, "2023-08-21", "Eighth grade"),
    (120, 4, "2023-08-21", "Ninth grade"),
]

# Each row: (CourseCode, SchoolId, SchoolYear, SectionIdentifier)
_COURSE_OFFERINGS = [
    ("ALG-1",  1, 2024, "ALG1-A"),
    ("ALG-1",  1, 2024, "ALG1-B"),
    ("ALG-2",  1, 2024, "ALG2-A"),
    ("BIO-1",  1, 2024, "BIO1-A"),
    ("CHEM-1", 1, 2024, "CHEM1-A"),
    ("ENG-1",  1, 2024, "ENG1-A"),
    ("ENG-1",  1, 2024, "ENG1-B"),
    ("HIST-1", 1, 2024, "HIST1-A"),
    ("PE-1",   2, 2024, "PE-MIDDLE"),
    ("ART-1",  3, 2024, "ART-ELEM"),
    ("ENG-1",  4, 2024, "ENG-CHARTER"),
    ("BIO-1",  4, 2024, "BIO-CHARTER"),
]


def build() -> Path:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(_SCHEMA_SQL)
        conn.executemany(
            "INSERT INTO School VALUES (?, ?, ?, ?)", _SCHOOLS,
        )
        conn.executemany(
            "INSERT INTO Student VALUES (?, ?, ?, ?, ?)", _STUDENTS,
        )
        conn.executemany(
            "INSERT INTO Staff VALUES (?, ?, ?, ?, ?)", _STAFF,
        )
        conn.executemany(
            "INSERT INTO Course VALUES (?, ?, ?)", _COURSES,
        )
        conn.executemany(
            "INSERT INTO StudentSchoolAssociation VALUES (?, ?, ?, ?)",
            _ENROLLMENTS,
        )
        conn.executemany(
            "INSERT INTO CourseOffering VALUES (?, ?, ?, ?)",
            _COURSE_OFFERINGS,
        )
        conn.commit()
    finally:
        conn.close()
    return DB_PATH


if __name__ == "__main__":
    path = build()
    print(f"Wrote {path}")
    # Sanity-print row counts so a regenerator can eyeball the totals.
    conn = sqlite3.connect(path)
    try:
        for table in ("School", "Student", "Staff", "Course",
                      "StudentSchoolAssociation", "CourseOffering"):
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:30s} {n:4d} rows")
    finally:
        conn.close()
