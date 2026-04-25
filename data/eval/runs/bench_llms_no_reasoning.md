# LLM head-to-head benchmark

| Provider | Validated | Executed | Avg latency (ms) | Repair-loop fires |
|---|---|---|---|---|
| `azure-gpt-4o` | 9/12 | 9/12 | 8698 | 7 |
| `openrouter-glm-5.1` | 7/12 | 7/12 | 19945 | 10 |

## `cross_attendance_demographic`
> _How many Hispanic students were absent at least once this term?_

### ✅ azure-gpt-4o  validated=True executed=True rows=1 repairs=0 latency=7463ms
```sql
SELECT COUNT(DISTINCT s.studentusi) AS hispanic_students_absent_count FROM edfi.student s JOIN edfi.studenteducationorganizationassociation seoa ON s.studentusi = seoa.studentusi JOIN edfi.descriptor d ON seoa.oldethnicitydescriptorid = d.descriptorid JOIN edfi.studentschoolattendanceevent ssae ON s.studentusi = ssae.studentusi JOIN edfi.session ses ON ssae.schoolid = ses.schoolid AND ssae.schoolyear = ses.schoolyear AND ssae.sessionname = ses.sessionname WHERE d.codevalue = 'Hispanic' AND ssae.attendanceeventreason IS NOT NULL AND ses.schoolyear = (SELECT schoolyear FROM edfi.schoolyeartype WHERE currentschoolyear = TRUE)
```
### ✅ openrouter-glm-5.1  validated=True executed=True rows=1 repairs=0 latency=14277ms
```sql
SELECT COUNT(DISTINCT st.studentusi) AS hispanic_absent_count FROM edfi.student st JOIN edfi.studenteducationorganizationassociation seoa ON seoa.studentusi = st.studentusi JOIN edfi.descriptor d_eth ON seoa.oldethnicitydescriptorid = d_eth.descriptorid JOIN edfi.studentschoolattendanceevent ssae ON ssae.studentusi = st.studentusi JOIN edfi.session sess ON ssae.schoolid = sess.schoolid AND ssae.schoolyear = sess.schoolyear AND ssae.sessionname = sess.sessionname JOIN edfi.descriptor d_att ON ssae.attendanceeventcategorydescriptorid = d_att.descriptorid WHERE d_eth.codevalue = 'Hispanic' AND d_att.codevalue = 'Absent' AND CURRENT_DATE BETWEEN sess.begindate AND sess.enddate
```

## `discipline_special_ed`
> _Which schools had the most disciplinary incidents involving students with IEPs last school year?_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=7144ms
```sql
SELECT eo.nameofinstitution AS school, COUNT(DISTINCT di.incidentidentifier) AS incident_count FROM edfi.disciplineincident di JOIN edfi.school s ON di.schoolid = s.schoolid JOIN edfi.educationorganization eo ON s.schoolid = eo.educationorganizationid JOIN edfi.studentdisciplineincidentbehaviorassociation sdiba ON di.incidentidentifier = sdiba.incidentidentifier AND di.schoolid = sdiba.schoolid JOIN edfi.studentspecialeducationprogramassociation ssepa ON sdiba.studentusi = ssepa.studentusi WHERE ssepa.ideaeligibility = TRUE AND di.incidentdate >= '2022-08-01' AND di.incidentdate <= '2023-07-31' GROUP BY eo.nameofinstitution ORDER BY incident_count DESC LIMIT 1000
```
### ✅ openrouter-glm-5.1  validated=True executed=True rows=0 repairs=0 latency=12528ms
```sql
SELECT eo.nameofinstitution AS school_name, COUNT(DISTINCT di.incidentidentifier) AS incident_count FROM edfi.disciplineaction da JOIN edfi.disciplineincident di ON da.responsibilityschoolid = di.schoolid JOIN edfi.studentdisciplineincidentbehaviorassociation sdiba ON di.incidentidentifier = sdiba.incidentidentifier AND di.schoolid = sdiba.schoolid AND da.studentusi = sdiba.studentusi JOIN edfi.school s ON di.schoolid = s.schoolid JOIN edfi.educationorganization eo ON s.schoolid = eo.educationorganizationid WHERE da.iepplacementmeetingindicator = true AND da.disciplinedate >= '2022-07-01' AND da.disciplinedate < '2023-07-01' GROUP BY eo.nameofinstitution ORDER BY incident_count DESC
```

## `gpa_per_grade`
> _Show average GPA broken down by grade level for the senior class_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=6498ms
```sql
SELECT d.codevalue AS grade_level, AVG(sar.cumulativegradepointaverage) AS average_gpa FROM edfi.studentschoolassociation ssa JOIN edfi.descriptor d ON ssa.entrygradeleveldescriptorid = d.descriptorid JOIN edfi.studentacademicrecord sar ON ssa.studentusi = sar.studentusi WHERE d.codevalue = 'Senior' GROUP BY d.codevalue
```
### ✅ openrouter-glm-5.1  validated=True executed=True rows=0 repairs=0 latency=14908ms
```sql
SELECT d.codevalue AS grade_level, AVG(sargpa.gradepointaveragevalue) AS average_gpa FROM edfi.studentschoolassociation ssa JOIN edfi.descriptor d ON ssa.entrygradeleveldescriptorid = d.descriptorid JOIN edfi.studentacademicrecordgradepointaverage sargpa ON sargpa.studentusi = ssa.studentusi WHERE d.codevalue = '12th grade' GROUP BY d.codevalue
```

## `section_credentialed`
> _List sections currently taught by staff who hold a math teaching credential_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=5801ms
```sql
SELECT DISTINCT sec.localcoursecode, sec.sectionidentifier, sec.sessionname, st.firstname || ' ' || st.lastsurname AS teacher_name FROM edfi.section sec JOIN edfi.staffsectionassociation ssa ON sec.localcoursecode = ssa.localcoursecode AND sec.schoolid = ssa.schoolid AND sec.schoolyear = ssa.schoolyear AND sec.sectionidentifier = ssa.sectionidentifier AND sec.sessionname = ssa.sessionname JOIN edfi.staff st ON ssa.staffusi = st.staffusi JOIN edfi.staffeducationorganizationemploymentassociation seo ON st.staffusi = seo.staffusi JOIN edfi.credential cr ON seo.credentialidentifier = cr.credentialidentifier JOIN edfi.credentialacademicsubject cas ON cr.credentialidentifier = cas.credentialidentifier AND cr.stateofissuestateabbreviationdescriptorid = cas.stateofissuestateabbreviationdescriptorid JOIN edfi.academicsubjectdescriptor asd ON cas.academicsubjectdescriptorid = asd.academicsubjectdescriptorid JOIN edfi.descriptor d ON asd.academicsubjectdescriptorid = d.descriptorid WHERE d.codevalue = 'Mathematics' LIMIT 1000
```
### ✅ openrouter-glm-5.1  validated=True executed=True rows=0 repairs=0 latency=12340ms
```sql
SELECT DISTINCT sec.localcoursecode, sec.sectionidentifier, sec.schoolyear, st.firstname, st.lastsurname FROM edfi.section sec JOIN edfi.staffsectionassociation ssa ON sec.localcoursecode = ssa.localcoursecode AND sec.schoolid = ssa.schoolid AND sec.schoolyear = ssa.schoolyear AND sec.sectionidentifier = ssa.sectionidentifier AND sec.sessionname = ssa.sessionname JOIN edfi.staff st ON ssa.staffusi = st.staffusi JOIN edfi.staffeducationorganizationemploymentassociation seoea ON st.staffusi = seoea.staffusi JOIN edfi.credential cr ON seoea.credentialidentifier = cr.credentialidentifier AND seoea.stateofissuestateabbreviationdescriptorid = cr.stateofissuestateabbreviationdescriptorid JOIN edfi.credentialacademicsubject cas ON cr.credentialidentifier = cas.credentialidentifier AND cr.stateofissuestateabbreviationdescriptorid = cas.stateofissuestateabbreviationdescriptorid JOIN edfi.academicsubjectdescriptor asd ON cas.academicsubjectdescriptorid = asd.academicsubjectdescriptorid JOIN edfi.descriptor d ON asd.academicsubjectdescriptorid = d.descriptorid JOIN edfi.teachingcredentialdescriptor tcd ON cr.teachingcredentialdescriptorid = tcd.teachingcredentialdescriptorid JOIN edfi.descriptor d2 ON tcd.teachingcredentialdescriptorid = d2.descriptorid WHERE d.codevalue = 'Mathematics' AND d2.codevalue = 'Teaching'
```

## `not_graduated_passing`
> _Students who did NOT graduate but earned passing grades in every course_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=7868ms
```sql
SELECT DISTINCT s.studentusi, s.firstname, s.lastsurname FROM edfi.student s JOIN edfi.studentacademicrecord sar ON s.studentusi = sar.studentusi LEFT JOIN edfi.studentschoolassociation ssa ON s.studentusi = ssa.studentusi AND ssa.exitwithdrawtypedescriptorid IS NOT NULL LEFT JOIN edfi.descriptor d ON ssa.exitwithdrawtypedescriptorid = d.descriptorid WHERE d.codevalue != 'Graduated' AND NOT EXISTS (SELECT 1 FROM edfi.coursetranscript ct WHERE ct.studentusi = s.studentusi AND ct.finallettergradeearned IN ('F', 'D')) LIMIT 1000
```
### ✅ openrouter-glm-5.1  validated=True executed=True rows=0 repairs=0 latency=14101ms
```sql
SELECT DISTINCT s.studentusi, s.firstname, s.lastsurname FROM edfi.student s JOIN edfi.coursetranscript ct ON ct.studentusi = s.studentusi JOIN edfi.courseattemptresultdescriptor card ON ct.courseattemptresultdescriptorid = card.courseattemptresultdescriptorid JOIN edfi.descriptor d ON card.courseattemptresultdescriptorid = d.descriptorid JOIN edfi.studentschoolassociation ssa ON ssa.studentusi = s.studentusi LEFT JOIN edfi.descriptor ed ON ssa.exitwithdrawtypedescriptorid = ed.descriptorid LEFT JOIN edfi.descriptor gd ON ssa.graduationplantypedescriptorid = gd.descriptorid WHERE ssa.exitwithdrawdate IS NOT NULL AND (ed.descriptorid IS NULL OR ed.codevalue NOT IN ('Graduated')) AND (gd.descriptorid IS NULL OR gd.codevalue NOT IN ('Graduated')) AND d.codevalue IN ('Pass', 'Passed', 'Completed') AND NOT EXISTS ( SELECT 1 FROM edfi.coursetranscript ct2 JOIN edfi.courseattemptresultdescriptor card2 ON ct2.courseattemptresultdescriptorid = card2.courseattemptresultdescriptorid JOIN edfi.descriptor d2 ON card2.courseattemptresultdescriptorid = d2.descriptorid WHERE ct2.studentusi = s.studentusi AND d2.codevalue NOT IN ('Pass', 'Passed', 'Completed')) ORDER BY s.lastsurname, s.firstname LIMIT 1000
```

## `descriptor_ambiguity_levels`
> _Top 5 grade levels by enrollment for Title I schools_

### ✅ azure-gpt-4o  validated=True executed=True rows=5 repairs=0 latency=6928ms
```sql
SELECT d.codevalue AS grade_level, COUNT(DISTINCT ssa.studentusi) AS enrollment_count FROM edfi.studentschoolassociation ssa JOIN edfi.school s ON ssa.schoolid = s.schoolid JOIN edfi.descriptor d ON ssa.entrygradeleveldescriptorid = d.descriptorid WHERE s.titleipartaschooldesignationdescriptorid IS NOT NULL GROUP BY d.codevalue ORDER BY enrollment_count DESC LIMIT 5
```
### ✅ openrouter-glm-5.1  validated=True executed=True rows=0 repairs=0 latency=13925ms
```sql
SELECT d.codevalue AS grade_level, COUNT(DISTINCT ssa.studentusi) AS enrollment_count FROM edfi.studentschoolassociation ssa JOIN edfi.school s ON ssa.schoolid = s.schoolid JOIN edfi.descriptor t1 ON s.titleipartaschooldesignationdescriptorid = t1.descriptorid JOIN edfi.descriptor d ON ssa.entrygradeleveldescriptorid = d.descriptorid WHERE t1.codevalue = 'Title I' GROUP BY d.codevalue ORDER BY enrollment_count DESC LIMIT 5
```

## `free_lunch_intervention`
> _Free-and-reduced-lunch students who received reading interventions, by school_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=9266ms
```sql
SELECT "edfi"."school"."schoolid", "edfi"."educationorganization"."nameofinstitution" AS school_name, COUNT(DISTINCT "edfi"."studentschoolassociation"."studentusi") AS student_count
FROM "edfi"."studentschoolassociation"
JOIN "edfi"."school" ON "edfi"."studentschoolassociation"."schoolid" = "edfi"."school"."schoolid"
JOIN "edfi"."educationorganization" ON "edfi"."school"."schoolid" = "edfi"."educationorganization"."educationorganizationid"
JOIN "edfi"."intervention" ON "edfi"."intervention"."educationorganizationid" = "edfi"."educationorganization"."educationorganizationid"
JOIN "edfi"."interventionpopulationserved" ON "edfi"."interventionpopulationserved"."educationorganizationid" = "edfi"."intervention"."educationorganizationid" AND "edfi"."interventionpopulationserved"."interventionidentificationcode" = "edfi"."intervention"."interventionidentificationcode"
JOIN "edfi"."populationserveddescriptor" ON "edfi"."populationserveddescriptor"."populationserveddescriptorid" = "edfi"."interventionpopulationserved"."populationserveddescriptorid"
JOIN "edfi"."descriptor" ON "edfi"."descriptor"."descriptorid" = "edfi"."populationserveddescriptor"."populationserveddescriptorid"
WHERE "edfi"."descriptor"."codevalue" = 'FreeAndReducedLunch' AND "edfi"."intervention"."interventionclassdescriptorid" IN (SELECT "descriptorid" FROM "edfi"."descriptor" WHERE "codevalue" = 'Reading')
GROUP BY "edfi"."school"."schoolid", "edfi"."educationorganization"."nameofinstitution"
ORDER BY student_count DESC
LIMIT 1000
```
### ✅ openrouter-glm-5.1  validated=True executed=True rows=0 repairs=0 latency=11922ms
```sql
SELECT eo.nameofinstitution AS school, COUNT(DISTINCT ssa.studentusi) AS student_count FROM edfi.intervention i JOIN edfi.interventionpopulationserved ips ON i.educationorganizationid = ips.educationorganizationid AND i.interventionidentificationcode = ips.interventionidentificationcode JOIN edfi.descriptor d ON ips.populationserveddescriptorid = d.descriptorid JOIN edfi.school s ON i.educationorganizationid = s.schoolid JOIN edfi.educationorganization eo ON s.schoolid = eo.educationorganizationid JOIN edfi.studentschoolassociation ssa ON ssa.schoolid = s.schoolid JOIN edfi.descriptor d2 ON ssa.entrygradeleveldescriptorid = d2.descriptorid WHERE d.codevalue = 'Free and Reduced Lunch' AND d2.codevalue = 'Reading' GROUP BY eo.nameofinstitution ORDER BY student_count DESC
```

## `candidate_field_hours`
> _Teacher-prep candidates who completed at least 100 hours of field experience in 2023_

### ❌ azure-gpt-4o  validated=False executed=False rows=None repairs=2 latency=11844ms
- error: `explain error: (psycopg.errors.UndefinedTable) relation "edfi.candidate" does not exist
LINE 1: ...NCT "edfi"."candidate"."candidateidentifier" FROM "edfi"."ca...
                                                             ^
[SQL: EXPLAIN SELECT DISTINCT "edfi"."candidate"."candidateidentifier" FRO`
```sql
SELECT DISTINCT "edfi"."candidate"."candidateidentifier" FROM "edfi"."candidate" JOIN "edfi"."candidateeducatorpreparationprogramassociation" ON "edfi"."candidateeducatorpreparationprogramassociation"."candidateidentifier" = "edfi"."candidate"."candidateidentifier" JOIN "edfi"."fieldworkexperience" ON "edfi"."fieldworkexperience"."educationorganizationid" = "edfi"."candidateeducatorpreparationprogramassociation"."educationorganizationid" AND "edfi"."fieldworkexperience"."programname" = "edfi"."candidateeducatorpreparationprogramassociation"."programname" AND "edfi"."fieldworkexperience"."programtypedescriptorid" = "edfi"."candidateeducatorpreparationprogramassociation"."programtypedescriptorid" WHERE "edfi"."fieldworkexperience"."totalhours" >= 100 AND EXTRACT(YEAR FROM "edfi"."fieldworkexperience"."enddate") = 2023 LIMIT 1000
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=2 latency=26616ms
- error: `explain error: (psycopg.errors.UndefinedTable) relation "edfi.candidate" does not exist
LINE 1: ...XPLAIN SELECT DISTINCT c.candidateidentifier FROM edfi.candi...
                                                             ^
[SQL: EXPLAIN SELECT DISTINCT c.candidateidentifier FROM edfi.candidate c `
```sql
SELECT DISTINCT c.candidateidentifier FROM edfi.candidate c JOIN "edfi"."candidateeducatorpreparationprogramassociation" ON "edfi"."candidateeducatorpreparationprogramassociation"."candidateidentifier" = c.candidateidentifier JOIN "edfi"."educatorpreparationprogram" ON "edfi"."candidateeducatorpreparationprogramassociation"."educationorganizationid" = "edfi"."educatorpreparationprogram"."educationorganizationid" AND "edfi"."candidateeducatorpreparationprogramassociation"."programname" = "edfi"."educatorpreparationprogram"."programname" AND "edfi"."candidateeducatorpreparationprogramassociation"."programtypedescriptorid" = "edfi"."educatorpreparationprogram"."programtypedescriptorid" JOIN "edfi"."fieldworkexperience" ON "edfi"."fieldworkexperience"."educationorganizationid" = "edfi"."educatorpreparationprogram"."educationorganizationid" AND "edfi"."fieldworkexperience"."programname" = "edfi"."educatorpreparationprogram"."programname" AND "edfi"."fieldworkexperience"."programtypedescriptorid" = "edfi"."educatorpreparationprogram"."programtypedescriptorid" WHERE "edfi"."fieldworkexperience"."hours" >= 100 AND EXTRACT(YEAR FROM "edfi"."fieldworkexperience"."begindate") = 2023 LIMIT 1000
```

## `assessment_advanced`
> _Hispanic ELL students who scored Advanced on the state ELA assessment with 90%+ attendance_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=1 latency=11130ms
```sql
SELECT DISTINCT s.studentusi, s.firstname, s.lastsurname FROM edfi.student s JOIN edfi.studentschoolassociation ssa ON s.studentusi = ssa.studentusi JOIN edfi.descriptor d_ethnicity ON ssa.entrygradeleveldescriptorid = d_ethnicity.descriptorid JOIN edfi.studentassessment sa ON s.studentusi = sa.studentusi JOIN edfi.studentassessmentitem sai ON sa.studentassessmentidentifier = sai.studentassessmentidentifier JOIN edfi.descriptor d_performance ON sai.assessmentitemresultdescriptorid = d_performance.descriptorid JOIN edfi.studentschoolattendanceevent ssae ON s.studentusi = ssae.studentusi WHERE d_ethnicity.codevalue = 'Hispanic' AND d_performance.codevalue = 'Advanced' AND ssae.eventduration >= 0.9 GROUP BY s.studentusi, s.firstname, s.lastsurname LIMIT 1000
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=2 latency=50353ms
- error: `explain error: (psycopg.errors.GroupingError) subquery uses ungrouped column "ssae.schoolid" from outer query
LINE 1: ...se2.studentusi = s.studentusi AND sse2.schoolid = ssae.schoo...
                                                             ^
[SQL: EXPLAIN SELECT s.studentusi, s.firstname, s.la`
```sql
SELECT s.studentusi, s.firstname, s.lastsurname FROM edfi.student s JOIN edfi.studenteducationorganizationassociation seoa ON s.studentusi = seoa.studentusi JOIN edfi.descriptor d_eth ON seoa.oldethnicitydescriptorid = d_eth.descriptorid JOIN edfi.studentassessment sa ON s.studentusi = sa.studentusi JOIN edfi.assessment a ON sa.assessmentidentifier = a.assessmentidentifier AND sa.namespace = a.namespace JOIN edfi.studentassessmentitem sai ON sai.assessmentidentifier = sa.assessmentidentifier AND sai.namespace = sa.namespace AND sai.studentassessmentidentifier = sa.studentassessmentidentifier AND sai.studentusi = sa.studentusi JOIN edfi.descriptor d_perf ON sai.assessmentitemresultdescriptorid = d_perf.descriptorid JOIN edfi.performancebaseconversiondescriptor pbcd ON pbcd.performancebaseconversiondescriptorid = d_perf.descriptorid JOIN edfi.studentschoolassociation ssa ON s.studentusi = ssa.studentusi JOIN edfi.studentschoolattendanceevent ssae ON ssa.studentusi = ssae.studentusi AND ssa.schoolid = ssae.schoolid JOIN edfi.attendanceeventcategorydescriptor aecd ON ssae.attendanceeventcategorydescriptorid = aecd.attendanceeventcategorydescriptorid JOIN edfi.descriptor d_att ON aecd.attendanceeventcategorydescriptorid = d_att.descriptorid WHERE d_eth.codevalue = 'Hispanic' AND a.assessmenttitle ILIKE '%ELA%' AND d_perf.codevalue = 'Advanced' AND d_att.codevalue = 'In Attendance' GROUP BY s.studentusi, s.firstname, s.lastsurname HAVING SUM(ssae.eventduration) / NULLIF((SELECT SUM(sse2.eventduration) FROM edfi.studentschoolattendanceevent sse2 WHERE sse2.studentusi = s.studentusi AND sse2.schoolid = ssae.schoolid AND sse2.schoolyear = ssae.schoolyear AND sse2.sessionname = ssae.sessionname), 0) >= 0.9 ORDER BY s.lastsurname LIMIT 1000
```

## `course_no_takers`
> _Courses offered this year that have zero enrolled students_

### ✅ azure-gpt-4o  validated=True executed=True rows=0 repairs=0 latency=6659ms
```sql
SELECT co.localcoursecode, co.localcoursetitle FROM edfi.courseoffering co LEFT JOIN edfi.section s ON co.localcoursecode = s.localcoursecode AND co.schoolid = s.schoolid AND co.schoolyear = s.schoolyear AND co.sessionname = s.sessionname LEFT JOIN edfi.studentsectionassociation ssa ON s.localcoursecode = ssa.localcoursecode AND s.schoolid = ssa.schoolid AND s.schoolyear = ssa.schoolyear AND s.sectionidentifier = ssa.sectionidentifier AND s.sessionname = ssa.sessionname WHERE co.schoolyear = (SELECT schoolyear FROM edfi.schoolyeartype WHERE currentschoolyear = TRUE) AND ssa.studentusi IS NULL
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=2 latency=24774ms
- error: `explain error: (psycopg.OperationalError) connection failed: connection to server at "127.0.0.1", port 5432 failed: could not receive data from server: Connection refused
(Background on this error at: https://sqlalche.me/e/20/e3q8)`
```sql
SELECT c.coursetitle, co.localcoursecode, eo.nameofinstitution AS school_name FROM edfi.courseoffering co JOIN edfi.course c ON co.coursecode = c.coursecode AND co.educationorganizationid = c.educationorganizationid JOIN edfi.educationorganization eo ON co.schoolid = eo.educationorganizationid JOIN edfi.schoolyeartype syt ON co.schoolyear = syt.schoolyear LEFT JOIN edfi.section s ON s.localcoursecode = co.localcoursecode AND s.schoolid = co.schoolid AND s.schoolyear = co.schoolyear AND s.sessionname = co.sessionname LEFT JOIN edfi.studentsectionassociation ssa ON ssa.localcoursecode = s.localcoursecode AND ssa.schoolid = s.schoolid AND ssa.schoolyear = s.schoolyear AND ssa.sectionidentifier = s.sectionidentifier AND ssa.sessionname = s.sessionname WHERE syt.currentschoolyear = TRUE GROUP BY c.coursetitle, co.localcoursecode, eo.nameofinstitution HAVING COUNT(ssa.studentusi) = 0
```

## `staff_retention`
> _Staff who left between 2022 and 2023 — by school and reason_

### ❌ azure-gpt-4o  validated=False executed=False rows=None repairs=2 latency=9971ms
- error: `explain error: (psycopg.OperationalError) connection failed: connection to server at "127.0.0.1", port 5432 failed: could not receive data from server: Connection refused
(Background on this error at: https://sqlalche.me/e/20/e3q8)`
```sql
SELECT "edfi"."school"."nameofinstitution" AS "SchoolName", "edfi"."staffleave"."reason" AS "LeaveReason", COUNT("edfi"."staffleave"."id") AS "LeaveCount" FROM "edfi"."staffleave" JOIN "edfi"."staffschoolassociation" ON "edfi"."staffleave"."staffusi" = "edfi"."staffschoolassociation"."staffusi" JOIN "edfi"."school" ON "edfi"."staffschoolassociation"."schoolid" = "edfi"."school"."schoolid" WHERE "edfi"."staffleave"."begindate" >= '2022-01-01' AND "edfi"."staffleave"."enddate" <= '2023-12-31' GROUP BY "edfi"."school"."nameofinstitution", "edfi"."staffleave"."reason" LIMIT 1000
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=2 latency=18053ms
- error: `explain error: (psycopg.OperationalError) connection failed: connection to server at "127.0.0.1", port 5432 failed: could not receive data from server: Connection refused
(Background on this error at: https://sqlalche.me/e/20/e3q8)`
```sql
SELECT eo.nameofinstitution AS school, sl.reason, s.staffusi, s.firstname, s.lastsurname FROM edfi.staffleave sl JOIN edfi.staff s ON sl.staffusi = s.staffusi JOIN edfi.staffschoolassociation ssa ON ssa.staffusi = s.staffusi JOIN edfi.school sch ON sch.schoolid = ssa.schoolid JOIN edfi.educationorganization eo ON eo.educationorganizationid = sch.schoolid WHERE sl.begindate >= '2022-01-01' AND sl.begindate <= '2023-12-31' ORDER BY eo.nameofinstitution, sl.reason
```

## `bell_schedule_overlap`
> _Sections that share the same bell-schedule period across two different teachers_

### ❌ azure-gpt-4o  validated=False executed=False rows=None repairs=2 latency=13805ms
- error: `explain error: (psycopg.OperationalError) connection failed: connection to server at "127.0.0.1", port 5432 failed: could not receive data from server: Connection refused
(Background on this error at: https://sqlalche.me/e/20/e3q8)`
```sql
SELECT DISTINCT scp1.localcoursecode, scp1.schoolid, scp1.schoolyear, scp1.sectionidentifier, scp1.sessionname, scp1.classperiodname FROM "edfi"."sectionclassperiod" AS scp1 JOIN "edfi"."sectionclassperiod" AS scp2 ON scp1.classperiodname = scp2.classperiodname AND scp1.schoolid = scp2.schoolid AND scp1.schoolyear = scp2.schoolyear AND scp1.sessionname = scp2.sessionname AND scp1.sectionidentifier <> scp2.sectionidentifier LIMIT 1000
```
### ❌ openrouter-glm-5.1  validated=False executed=False rows=None repairs=2 latency=25549ms
- error: `explain error: (psycopg.OperationalError) connection failed: connection to server at "127.0.0.1", port 5432 failed: could not receive data from server: Connection refused
(Background on this error at: https://sqlalche.me/e/20/e3q8)`
```sql
SELECT s1.sectionidentifier AS section_identifier_1, s2.sectionidentifier AS section_identifier_2, scp.classperiodname AS class_period_name FROM edfi.section s1 JOIN edfi.sectionclassperiod scp ON scp.localcoursecode = s1.localcoursecode AND scp.schoolid = s1.schoolid AND scp.schoolyear = s1.schoolyear AND scp.sectionidentifier = s1.sectionidentifier AND scp.sessionname = s1.sessionname JOIN edfi.section s2 ON scp.localcoursecode = s2.localcoursecode AND scp.schoolid = s2.schoolid AND scp.schoolyear = s2.schoolyear AND scp.sectionidentifier = s2.sectionidentifier AND scp.sessionname = s2.sessionname WHERE s1.sectionidentifier < s2.sectionidentifier LIMIT 1000
```
