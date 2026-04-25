# LLM head-to-head benchmark

| Provider | Validated | Executed | Avg latency (ms) | Repair-loop fires |
|---|---|---|---|---|
| `azure-gpt-4o` | 11/12 | 11/12 | 8362 | 2 |
| `openrouter-glm-5.1` | 3/12 | 3/12 | 42041 | 0 |

## `cross_attendance_demographic`
> _How many Hispanic students were absent at least once this term?_

### ✅ azure-gpt-4o  validated=True executed=True rows=1 repairs=0 latency=8941ms
```sql
SELECT COUNT(DISTINCT s.studentusi) AS hispanic_students_absent_count FROM edfi.student s JOIN edfi.studenteducationorganizationassociation seoa ON s.studentusi = seoa.studentusi JOIN edfi.descriptor d ON seoa.oldethnicitydescriptorid = d.descriptorid JOIN edfi.studentschoolattendanceevent ssae ON s.studentusi = ssae.studentusi JOIN edfi.session ses ON ssae.schoolid = ses.schoolid AND ssae.schoolyear = ses.schoolyear AND ssae.sessionname = ses.sessionname WHERE d.codevalue = 'Hispanic' AND ssae.attendanceeventreason IS NOT NULL AND ses.termdescriptorid = (SELECT termdescriptorid FROM edfi.descriptor WHERE codevalue = 'Current Term')
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=0 latency=33250ms
- error: `Expecting value: line 1 column 1 (char 0)`

## `discipline_special_ed`
> _Which schools had the most disciplinary incidents involving students with IEPs last school year?_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=7320ms
```sql
SELECT eo.nameofinstitution AS school, COUNT(DISTINCT di.incidentidentifier) AS incident_count FROM edfi.disciplineincident di JOIN edfi.school s ON di.schoolid = s.schoolid JOIN edfi.educationorganization eo ON s.schoolid = eo.educationorganizationid JOIN edfi.studentdisciplineincidentbehaviorassociation sdiba ON di.incidentidentifier = sdiba.incidentidentifier AND di.schoolid = sdiba.schoolid JOIN edfi.studentspecialeducationprogramassociation ssepa ON sdiba.studentusi = ssepa.studentusi WHERE ssepa.ideaeligibility = TRUE AND di.incidentdate >= '2022-08-01' AND di.incidentdate <= '2023-07-31' GROUP BY eo.nameofinstitution ORDER BY incident_count DESC LIMIT 1000
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=0 latency=31721ms
- error: `Expecting value: line 1 column 1 (char 0)`

## `gpa_per_grade`
> _Show average GPA broken down by grade level for the senior class_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=6471ms
```sql
SELECT d.codevalue AS grade_level, AVG(sar.cumulativegradepointaverage) AS average_gpa FROM edfi.studentschoolassociation ssa JOIN edfi.descriptor d ON ssa.entrygradeleveldescriptorid = d.descriptorid JOIN edfi.studentacademicrecord sar ON ssa.studentusi = sar.studentusi AND ssa.schoolyear = sar.schoolyear WHERE ssa.classofschoolyear = sar.schoolyear GROUP BY d.codevalue ORDER BY average_gpa DESC
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=0 latency=30992ms
- error: `Expecting value: line 1 column 1 (char 0)`

## `section_credentialed`
> _List sections currently taught by staff who hold a math teaching credential_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=6636ms
```sql
SELECT DISTINCT sec.localcoursecode, sec.sectionidentifier, sec.sessionname, st.firstname || ' ' || st.lastsurname AS teacher_name FROM edfi.section sec JOIN edfi.staffsectionassociation ssa ON sec.localcoursecode = ssa.localcoursecode AND sec.schoolid = ssa.schoolid AND sec.schoolyear = ssa.schoolyear AND sec.sectionidentifier = ssa.sectionidentifier AND sec.sessionname = ssa.sessionname JOIN edfi.staff st ON ssa.staffusi = st.staffusi JOIN edfi.staffeducationorganizationemploymentassociation seo ON st.staffusi = seo.staffusi JOIN edfi.credential cr ON seo.credentialidentifier = cr.credentialidentifier JOIN edfi.credentialacademicsubject cas ON cr.credentialidentifier = cas.credentialidentifier AND cr.stateofissuestateabbreviationdescriptorid = cas.stateofissuestateabbreviationdescriptorid JOIN edfi.academicsubjectdescriptor asd ON cas.academicsubjectdescriptorid = asd.academicsubjectdescriptorid JOIN edfi.descriptor d ON asd.academicsubjectdescriptorid = d.descriptorid WHERE d.codevalue = 'Mathematics' LIMIT 1000
```
### ✅ openrouter-glm-5.1  validated=True executed=True rows=0 repairs=0 latency=24124ms
```sql
SELECT DISTINCT sec.localcoursecode, sec.sectionidentifier, sec.sectionname, st.firstname, st.lastsurname FROM edfi.section sec JOIN edfi.staffsectionassociation ssa ON sec.localcoursecode = ssa.localcoursecode AND sec.schoolid = ssa.schoolid AND sec.schoolyear = ssa.schoolyear AND sec.sectionidentifier = ssa.sectionidentifier AND sec.sessionname = ssa.sessionname JOIN edfi.staff st ON ssa.staffusi = st.staffusi JOIN edfi.staffeducationorganizationemploymentassociation seoea ON st.staffusi = seoea.staffusi JOIN edfi.credential c ON seoea.credentialidentifier = c.credentialidentifier AND seoea.stateofissuestateabbreviationdescriptorid = c.stateofissuestateabbreviationdescriptorid JOIN edfi.credentialacademicsubject cas ON c.credentialidentifier = cas.credentialidentifier AND c.stateofissuestateabbreviationdescriptorid = cas.stateofissuestateabbreviationdescriptorid JOIN edfi.academicsubjectdescriptor asd ON cas.academicsubjectdescriptorid = asd.academicsubjectdescriptorid JOIN edfi.descriptor d ON asd.academicsubjectdescriptorid = d.descriptorid WHERE d.codevalue ILIKE '%Math%'
```

## `not_graduated_passing`
> _Students who did NOT graduate but earned passing grades in every course_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=7997ms
```sql
SELECT DISTINCT s.studentusi, s.firstname, s.lastsurname FROM edfi.student s JOIN edfi.studentacademicrecord sar ON s.studentusi = sar.studentusi LEFT JOIN edfi.studentschoolassociation ssa ON s.studentusi = ssa.studentusi AND ssa.exitwithdrawtypedescriptorid IS NOT NULL LEFT JOIN edfi.descriptor d ON ssa.exitwithdrawtypedescriptorid = d.descriptorid WHERE d.codevalue != 'Graduated' AND NOT EXISTS (SELECT 1 FROM edfi.coursetranscript ct WHERE ct.studentusi = s.studentusi AND ct.finallettergradeearned IN ('F', 'D')) LIMIT 1000
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=0 latency=51258ms
- error: `Expecting value: line 1 column 1 (char 0)`

## `descriptor_ambiguity_levels`
> _Top 5 grade levels by enrollment for Title I schools_

### ✅ azure-gpt-4o  validated=True executed=True rows=5 repairs=0 latency=9328ms
```sql
SELECT d.codevalue AS grade_level, COUNT(DISTINCT ssa.studentusi) AS enrollment_count FROM edfi.studentschoolassociation ssa JOIN edfi.school s ON ssa.schoolid = s.schoolid JOIN edfi.descriptor d ON ssa.entrygradeleveldescriptorid = d.descriptorid WHERE s.titleipartaschooldesignationdescriptorid IS NOT NULL GROUP BY d.codevalue ORDER BY enrollment_count DESC LIMIT 5
```
### ✅ openrouter-glm-5.1  validated=True executed=True rows=0 repairs=0 latency=48562ms
```sql
SELECT d.codevalue AS grade_level, COUNT(DISTINCT ssa.studentusi) AS enrollment_count FROM edfi.studentschoolassociation ssa JOIN edfi.school s ON ssa.schoolid = s.schoolid JOIN edfi.descriptor d_title ON s.titleipartaschooldesignationdescriptorid = d_title.descriptorid JOIN edfi.descriptor d ON ssa.entrygradeleveldescriptorid = d.descriptorid WHERE d_title.codevalue LIKE 'Title I%' GROUP BY d.codevalue ORDER BY enrollment_count DESC LIMIT 5
```

## `free_lunch_intervention`
> _Free-and-reduced-lunch students who received reading interventions, by school_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=7883ms
```sql
SELECT "edfi"."school"."schoolid", "edfi"."educationorganization"."nameofinstitution" AS school_name, COUNT(DISTINCT "edfi"."studentschoolassociation"."studentusi") AS student_count FROM "edfi"."studentschoolassociation" JOIN "edfi"."school" ON "edfi"."studentschoolassociation"."schoolid" = "edfi"."school"."schoolid" JOIN "edfi"."educationorganization" ON "edfi"."school"."schoolid" = "edfi"."educationorganization"."educationorganizationid" JOIN "edfi"."intervention" ON "edfi"."intervention"."educationorganizationid" = "edfi"."educationorganization"."educationorganizationid" JOIN "edfi"."interventionpopulationserved" ON "edfi"."interventionpopulationserved"."educationorganizationid" = "edfi"."intervention"."educationorganizationid" AND "edfi"."interventionpopulationserved"."interventionidentificationcode" = "edfi"."intervention"."interventionidentificationcode" JOIN "edfi"."populationserveddescriptor" ON "edfi"."interventionpopulationserved"."populationserveddescriptorid" = "edfi"."populationserveddescriptor"."populationserveddescriptorid" JOIN "edfi"."descriptor" ON "edfi"."populationserveddescriptor"."populationserveddescriptorid" = "edfi"."descriptor"."descriptorid" WHERE "edfi"."descriptor"."codevalue" = 'FreeAndReducedLunch' GROUP BY "edfi"."school"."schoolid", "edfi"."educationorganization"."nameofinstitution" ORDER BY student_count DESC LIMIT 1000
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=0 latency=42963ms
- error: `Expecting value: line 1 column 1 (char 0)`

## `candidate_field_hours`
> _Teacher-prep candidates who completed at least 100 hours of field experience in 2023_

### ❌ azure-gpt-4o  validated=False executed=False rows=None repairs=2 latency=12954ms
- error: `explain error: (psycopg.errors.UndefinedTable) relation "edfi.candidate" does not exist
LINE 1: ...idate"."candidateidentifier" AS candidate_id FROM "edfi"."ca...
                                                             ^
[SQL: EXPLAIN SELECT DISTINCT "edfi"."candidate"."candidateidentifier" AS `
```sql
SELECT DISTINCT "edfi"."candidate"."candidateidentifier" AS candidate_id FROM "edfi"."candidate" JOIN "edfi"."candidateeducatorpreparationprogramassociation" ON "edfi"."candidateeducatorpreparationprogramassociation"."candidateidentifier" = "edfi"."candidate"."candidateidentifier" JOIN "edfi"."fieldworkexperience" ON "edfi"."fieldworkexperience"."educationorganizationid" = "edfi"."candidateeducatorpreparationprogramassociation"."educationorganizationid" AND "edfi"."fieldworkexperience"."programname" = "edfi"."candidateeducatorpreparationprogramassociation"."programname" AND "edfi"."fieldworkexperience"."programtypedescriptorid" = "edfi"."candidateeducatorpreparationprogramassociation"."programtypedescriptorid" WHERE "edfi"."fieldworkexperience"."totalhours" >= 100 AND EXTRACT(YEAR FROM "edfi"."fieldworkexperience"."enddate") = 2023 LIMIT 1000
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=0 latency=39179ms
- error: `Expecting value: line 1 column 1 (char 0)`

## `assessment_advanced`
> _Hispanic ELL students who scored Advanced on the state ELA assessment with 90%+ attendance_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=10498ms
```sql
SELECT DISTINCT s.studentusi, s.firstname, s.lastsurname FROM edfi.student s JOIN edfi.studentschoolassociation ssa ON s.studentusi = ssa.studentusi JOIN edfi.descriptor d_ethnicity ON ssa.entrygradeleveldescriptorid = d_ethnicity.descriptorid JOIN edfi.studentassessment sa ON s.studentusi = sa.studentusi JOIN edfi.studentassessmentitem sai ON sa.assessmentidentifier = sai.assessmentidentifier AND sa.namespace = sai.namespace AND sa.studentassessmentidentifier = sai.studentassessmentidentifier AND sa.studentusi = sai.studentusi JOIN edfi.performancebaseconversiondescriptor pbc ON sai.responseindicatordescriptorid = pbc.performancebaseconversiondescriptorid JOIN edfi.descriptor d_pbc ON pbc.performancebaseconversiondescriptorid = d_pbc.descriptorid WHERE d_ethnicity.codevalue = 'Hispanic' AND d_pbc.codevalue = 'Advanced' AND ssa.fulltimeequivalency >= 0.9 LIMIT 1000
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=0 latency=54259ms
- error: `Expecting value: line 1 column 1 (char 0)`

## `course_no_takers`
> _Courses offered this year that have zero enrolled students_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=7122ms
```sql
SELECT co.localcoursecode, co.localcoursetitle FROM edfi.courseoffering co LEFT JOIN edfi.section s ON co.localcoursecode = s.localcoursecode AND co.schoolid = s.schoolid AND co.schoolyear = s.schoolyear AND co.sessionname = s.sessionname LEFT JOIN edfi.studentsectionassociation ssa ON s.localcoursecode = ssa.localcoursecode AND s.schoolid = ssa.schoolid AND s.schoolyear = ssa.schoolyear AND s.sectionidentifier = ssa.sectionidentifier AND s.sessionname = ssa.sessionname WHERE co.schoolyear = (SELECT schoolyear FROM edfi.schoolyeartype WHERE currentschoolyear = TRUE LIMIT 1) GROUP BY co.localcoursecode, co.localcoursetitle HAVING COUNT(ssa.studentusi) = 0
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=0 latency=53472ms
- error: `Expecting value: line 1 column 1 (char 0)`

## `staff_retention`
> _Staff who left between 2022 and 2023 — by school and reason_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=5669ms
```sql
SELECT eo.nameofinstitution AS school, sl.reason AS leave_reason, COUNT(*) AS staff_count FROM edfi.staffleave sl JOIN edfi.staff s ON sl.staffusi = s.staffusi JOIN edfi.staffeducationorganizationemploymentassociation seo ON s.staffusi = seo.staffusi JOIN edfi.educationorganization eo ON seo.educationorganizationid = eo.educationorganizationid WHERE sl.begindate >= '2022-01-01' AND sl.enddate <= '2023-12-31' GROUP BY eo.nameofinstitution, sl.reason ORDER BY staff_count DESC
```
### ✅ openrouter-glm-5.1  validated=True executed=True rows=6 repairs=0 latency=56173ms
```sql
SELECT eo.nameofinstitution AS school, sl.reason, COUNT(*) AS staff_leave_count FROM edfi.staffleave sl JOIN edfi.staff st ON sl.staffusi = st.staffusi JOIN edfi.staffeducationorganizationassignmentassociation seoa ON seoa.staffusi = st.staffusi JOIN edfi.educationorganization eo ON seoa.educationorganizationid = eo.educationorganizationid JOIN edfi.school sc ON sc.schoolid = eo.educationorganizationid WHERE sl.begindate >= '2022-01-01' AND sl.begindate <= '2023-12-31' GROUP BY eo.nameofinstitution, sl.reason ORDER BY staff_leave_count DESC
```

## `bell_schedule_overlap`
> _Sections that share the same bell-schedule period across two different teachers_

### ✅ azure-gpt-4o  validated=True executed=True rows=20 repairs=0 latency=9532ms
```sql
SELECT DISTINCT s1.localcoursecode, s1.sectionidentifier, s2.localcoursecode AS other_localcoursecode, s2.sectionidentifier AS other_sectionidentifier FROM edfi.sectionclassperiod scp1 JOIN edfi.section s1 ON scp1.localcoursecode = s1.localcoursecode AND scp1.schoolid = s1.schoolid AND scp1.schoolyear = s1.schoolyear AND scp1.sectionidentifier = s1.sectionidentifier AND scp1.sessionname = s1.sessionname JOIN edfi.sectionclassperiod scp2 ON scp1.classperiodname = scp2.classperiodname AND scp1.schoolid = scp2.schoolid JOIN edfi.section s2 ON scp2.localcoursecode = s2.localcoursecode AND scp2.schoolid = s2.schoolid AND scp2.schoolyear = s2.schoolyear AND scp2.sectionidentifier = s2.sectionidentifier AND scp2.sessionname = s2.sessionname WHERE s1.sectionidentifier <> s2.sectionidentifier LIMIT 1000
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=0 latency=38546ms
- error: `Expecting value: line 1 column 1 (char 0)`
