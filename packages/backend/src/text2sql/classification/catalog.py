"""Candidate domain catalog — what the LLM is allowed to choose from.

By default, derive the catalog from the ApiModel.json that Component 1
ingested (every distinct value in `domains[]` across entityDefinitions, plus
auto-generated descriptions from common entity name fragments). For non-Ed-Fi
schemas, point the catalog loader at a YAML file with explicit (name, description)
pairs.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from text2sql.ingestion.edfi_fetcher import IngestionManifest


@dataclass(frozen=True)
class Domain:
    name: str
    description: str
    seed_entities: tuple[str, ...] = ()           # representative tables, helps LLM


@dataclass
class DomainCatalog:
    domains: list[Domain] = field(default_factory=list)

    def names(self) -> list[str]:
        return [d.name for d in self.domains]

    def by_name(self) -> dict[str, Domain]:
        return {d.name: d for d in self.domains}


# Curated descriptions for the 35 DS 6.1.0 domains. The LLM gets these verbatim,
# so they steer the classification decisions. Concise + concrete.
_DS610_DOMAIN_DESCRIPTIONS: dict[str, str] = {
    "AlternativeAndSupplementalServices": "Programs and services beyond the standard curriculum — Title I, migrant, homeless, neglected/delinquent.",
    "Assessment": "Assessment definitions, assessment items, performance levels (test blueprint, not the student-result side).",
    "AssessmentMetadata": "Metadata describing assessments — sections, sub-tests, scoring rules.",
    "AssessmentRegistration": "Student registrations and accommodations for upcoming assessment administrations.",
    "BellSchedule": "Daily bell schedules, periods, and class period offerings.",
    "CourseCatalog": "Course definitions, prerequisites, identification codes, levels.",
    "Credential": "Credentials issued to staff (teaching licenses, endorsements, certificates).",
    "Discipline": "Disciplinary incidents, actions, behaviors, and weapons.",
    "EducationOrganization": "Education organization hierarchy: SEAs, LEAs, schools, postsecondary, and their addresses/contacts.",
    "EducatorPreparationProgram": "Educator preparation programs — program enrollments, completers, requirements.",
    "Enrollment": "Student enrollment in schools and education organizations: entry/exit, attendance status.",
    "Finance": "Charts of accounts, balance sheets, ledgers — district financial data.",
    "Gradebook": "Gradebook entries, learning standards, gradebook scores tied to sections.",
    "Graduation": "Graduation plans, alternative graduation plans, completion requirements.",
    "Intervention": "Intervention prescriptions, intervention studies, populations served.",
    "Path": "Pathway/program-of-study sequences and stage completions.",
    "PerformanceEvaluation": "Staff performance evaluations, ratings, evaluation rubrics.",
    "RecruitingAndStaffing": "Job postings, applicants, hiring sources, recruitment events.",
    "ReportCard": "Student report card grades, marks, and term-level academic summaries.",
    "SchoolCalendar": "School year, calendar dates, sessions, and grading periods.",
    "SectionsAndPrograms": "Course sections, program associations, section attendance.",
    "SpecialEducation": "IEPs, special-education program associations, services, and disability.",
    "SpecialEducationDataModel": "Extended IEP data model — service provider details, present-level descriptions.",
    "Staff": "Staff identity, demographics, employment, and education-organization assignments.",
    "Standards": "Academic content standards, learning standards, and standards alignment.",
    "StudentAcademicRecord": "Student transcripts, grades, course taken, GPAs, academic histories.",
    "StudentAssessment": "Student assessment results — scores, performance levels, accommodations used.",
    "StudentAttendance": "Daily and section attendance events.",
    "StudentCohort": "Student cohorts and cohort-based program associations.",
    "StudentHealth": "Student immunizations, health screenings, medical conditions.",
    "StudentIdentificationAndDemographics": "Student identity, USIs, demographics, race, ethnicity, languages.",
    "StudentProgramEvaluation": "Student-level program evaluation results.",
    "StudentTranscript": "Course transcripts and credits earned.",
    "Survey": "Survey instruments, sections, questions, and responses captured from students or staff.",
    "TeachingAndLearning": "Course offerings, sections, instruction time, learning objectives — the heart of classroom data.",
}

# TPDM (extension) domain
_TPDM_DOMAINS: dict[str, str] = {
    "TeacherPreparation": "TPDM extension — candidate teacher preparation, performance evaluations, certification.",
}


def load_domain_catalog(
    manifest: IngestionManifest,
    *,
    overrides_path: Path | None = None,
) -> DomainCatalog:
    """Build a DomainCatalog by inspecting the ApiModel(s) in the manifest, then
    optionally overlaying YAML overrides.

    The YAML override format:
        domains:
          - name: MyDomain
            description: "..."
            seed_entities: [Table1, Table2]
    """
    seen: dict[str, list[str]] = {}    # domain -> list of representative entity names
    descriptions = {**_DS610_DOMAIN_DESCRIPTIONS, **_TPDM_DOMAINS}

    for art in manifest.artifacts:
        data = json.loads(art.api_model_path.read_text(encoding="utf-8"))
        for ent in data.get("entityDefinitions", []):
            for d in ent.get("domains", []) or []:
                seen.setdefault(d, []).append(ent["name"])

    domains: list[Domain] = []
    for name, members in seen.items():
        # pick top-3 most common members as seed entities (deterministic, name-stable)
        top = [n for n, _ in Counter(members).most_common(3)]
        domains.append(
            Domain(
                name=name,
                description=descriptions.get(name, f"{name} domain (auto-discovered).") ,
                seed_entities=tuple(top),
            )
        )
    domains.sort(key=lambda d: d.name)

    if overrides_path and overrides_path.exists():
        raw = yaml.safe_load(overrides_path.read_text(encoding="utf-8")) or {}
        existing = {d.name: d for d in domains}
        for entry in raw.get("domains", []):
            existing[entry["name"]] = Domain(
                name=entry["name"],
                description=entry.get("description", ""),
                seed_entities=tuple(entry.get("seed_entities", [])),
            )
        domains = sorted(existing.values(), key=lambda d: d.name)

    return DomainCatalog(domains=domains)
