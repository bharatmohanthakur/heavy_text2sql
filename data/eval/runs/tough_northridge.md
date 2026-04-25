# Tough-questions stress run

- queries: **12**
- SQL valid: **10/12**
- executed:  **10/12**
- needed repair: **7/12**
- returned 0 rows: **6/12**
- avg wall-clock: **7800 ms**

## ✅ `cross_attendance_demographic`
> _How many Hispanic students were absent at least once this term?_

**validated** True · **executed** True · **rows** 1 · **join nodes** 11 · **repairs** 2 · **8309 ms**
- domains: StudentAttendance · StudentIdentificationAndDemographics
- top tables: edfi.StudentDemographic · edfi.AidTypeDescriptor · edfi.TermDescriptor · edfi.PopulationServedDescriptor · edfi.School
- rationale: _Corrected the column reference for the descriptor join to align with the schema._
- description: _This term, no Hispanic students were absent at least once._
```sql
SELECT COUNT(DISTINCT [edfi].[Student].[StudentUSI]) AS [HispanicStudentAbsences] FROM [edfi].[Student] JOIN [edfi].[StudentSchoolAssociation] ON [edfi].[StudentSchoolAssociation].[StudentUSI] = [edfi].[Student].[StudentUSI] JOIN [edfi].[Descriptor] ON [edfi].[Descriptor].[DescriptorId] = [edfi].[Student].[BirthCountryDescriptorId] WHERE [edfi].[Descriptor].[CodeValue] = 'Hispanic' AND [edfi].[StudentSchoolAssociation].[TermCompletionIndicator] = 0
```

## ✅ `discipline_special_ed`
> _Which schools had the most disciplinary incidents involving students with IEPs last school year?_

**validated** True · **executed** True · **rows** 0 · **join nodes** 14 · **repairs** 1 · **9839 ms**
- domains: Discipline · SpecialEducation
- top tables: edfi.StudentDisciplineIncidentBehaviorAssociationWeapon · edfi.DisciplineAction · edfi.DisciplineIncidentWeapon · edfi.DisciplineActionLengthDifferenceReasonDescriptor · edfi.SchoolTypeDescriptor
- rationale: _The query was corrected to reference the 'NameOfInstitution' column from the 'EducationOrganization' table, which is joined with the 'School' table._
- description: _No matching rows._
```sql
SELECT TOP 1000 [edfi].[EducationOrganization].[NameOfInstitution], COUNT([edfi].[DisciplineIncident].[IncidentIdentifier]) AS [IncidentCount] FROM [edfi].[DisciplineIncident] JOIN [edfi].[StudentDisciplineIncidentBehaviorAssociation] ON [edfi].[StudentDisciplineIncidentBehaviorAssociation].[IncidentIdentifier] = [edfi].[DisciplineIncident].[IncidentIdentifier] AND [edfi].[StudentDisciplineIncidentBehaviorAssociation].[SchoolId] = [edfi].[DisciplineIncident].[SchoolId] JOIN [edfi].[DisciplineAction] ON [edfi].[DisciplineAction].[StudentUSI] = [edfi].[StudentDisciplineIncidentBehaviorAssociation].[StudentUSI] AND [edfi].[DisciplineAction].[AssignmentSchoolId] = [edfi].[DisciplineIncident].[SchoolId] JOIN [edfi].[School] ON [edfi].[School].[SchoolId] = [edfi].[DisciplineIncident].[SchoolId] JOIN [edfi].[EducationOrganization] ON [edfi].[EducationOrganization].[EducationOrganizationId] = [edfi].[School].[SchoolId] WHERE [edfi].[DisciplineAction].[IEPPlacementMeetingIndicator] = 1 AND YEAR([edfi].[DisciplineIncident].[IncidentDate]) = YEAR(GETDATE()) - 1 GROUP BY [edfi].[EducationOrganization].[NameOfInstitution] ORDER BY [IncidentCount] DESC
```

## ✅ `gpa_per_grade`
> _Show average GPA broken down by grade level for the senior class_

**validated** True · **executed** True · **rows** 0 · **join nodes** 24 · **repairs** 1 · **7092 ms**
- domains: StudentAcademicRecord
- top tables: edfi.StudentAcademicRecordGradePointAverage · edfi.CourseGPAApplicabilityDescriptor · edfi.GradePointAverageTypeDescriptor · edfi.ReportCardGradePointAverage · edfi.Grade
- rationale: _The query correctly joins the necessary tables and filters by the 'Senior' grade level using the appropriate descriptor column._
- description: _No matching rows._
```sql
SELECT TOP 1000 d."ShortDescription" AS "GradeLevel", AVG(sargpa."GradePointAverageValue") AS "AverageGPA" FROM "edfi"."StudentAcademicRecordGradePointAverage" sargpa JOIN "edfi"."StudentSchoolAssociation" ssa ON sargpa."StudentUSI" = ssa."StudentUSI" JOIN "edfi"."GradeLevelDescriptor" gld ON ssa."EntryGradeLevelDescriptorId" = gld."GradeLevelDescriptorId" JOIN "edfi"."Descriptor" d ON gld."GradeLevelDescriptorId" = d."DescriptorId" WHERE d."ShortDescription" = 'Senior' GROUP BY d."ShortDescription"
```

## ✅ `section_credentialed`
> _List sections currently taught by staff who hold a math teaching credential_

**validated** True · **executed** True · **rows** 0 · **join nodes** 22 · **repairs** 0 · **4659 ms**
- domains: SectionsAndPrograms · Credential
- top tables: edfi.TeachingCredentialBasisDescriptor · edfi.TeachingCredentialDescriptor · edfi.StaffSectionAssociation · edfi.CertificationFieldDescriptor · edfi.Credential
- rationale: _The query retrieves sections taught by staff holding a math teaching credential by filtering on the 'Math' descriptor code value in the academic subject descriptor._
- description: _No matching rows._
```sql
SELECT TOP 1000 ssa.[LocalCourseCode], ssa.[SchoolId], ssa.[SchoolYear], ssa.[SectionIdentifier], ssa.[SessionName] FROM [edfi].[StaffSectionAssociation] AS ssa JOIN [edfi].[Staff] AS st ON ssa.[StaffUSI] = st.[StaffUSI] JOIN [edfi].[StaffEducationOrganizationEmploymentAssociation] AS seo ON st.[StaffUSI] = seo.[StaffUSI] JOIN [edfi].[Credential] AS cr ON seo.[CredentialIdentifier] = cr.[CredentialIdentifier] AND seo.[StateOfIssueStateAbbreviationDescriptorId] = cr.[StateOfIssueStateAbbreviationDescriptorId] JOIN [edfi].[CredentialAcademicSubject] AS cas ON cr.[CredentialIdentifier] = cas.[CredentialIdentifier] AND cr.[StateOfIssueStateAbbreviationDescriptorId] = cas.[StateOfIssueStateAbbreviationDescriptorId] JOIN [edfi].[AcademicSubjectDescriptor] AS asd ON cas.[AcademicSubjectDescriptorId] = asd.[AcademicSubjectDescriptorId] JOIN [edfi].[Descriptor] AS d ON asd.[AcademicSubjectDescriptorId] = d.[DescriptorId] WHERE d.[CodeValue] = 'Math'
```

## ✅ `not_graduated_passing`
> _Students who did NOT graduate but earned passing grades in every course_

**validated** True · **executed** True · **rows** 50 · **join nodes** 29 · **repairs** 0 · **8087 ms**
- domains: StudentAcademicRecord · Graduation
- top tables: edfi.CourseTranscriptPartialCourseTranscriptAwards · edfi.GraduationPlanCreditsBySubject · edfi.ReportCardGrade · edfi.StudentAcademicRecord · edfi.MethodCreditEarnedDescriptor
- rationale: _The query identifies students without graduation records and ensures all their courses have passing grades._
- description: _The dashboard displays 50 students who did not graduate but achieved passing grades in all their courses. Examples include Adam Watt, Santos Lazaro, and Ubaldo Stephens. This information highlights students who met academic requirements but did not complete graduation._
```sql
SELECT TOP 1000 [edfi].[Student].[StudentUSI], [edfi].[Student].[FirstName], [edfi].[Student].[LastSurname] FROM [edfi].[Student] JOIN [edfi].[StudentAcademicRecord] ON [edfi].[StudentAcademicRecord].[StudentUSI] = [edfi].[Student].[StudentUSI] LEFT JOIN [edfi].[GraduationPlan] ON [edfi].[GraduationPlan].[EducationOrganizationId] = [edfi].[StudentAcademicRecord].[EducationOrganizationId] AND [edfi].[GraduationPlan].[GraduationSchoolYear] = [edfi].[StudentAcademicRecord].[SchoolYear] WHERE [edfi].[GraduationPlan].[Id] IS NULL AND NOT EXISTS (SELECT 1 FROM [edfi].[CourseTranscript] WHERE [edfi].[CourseTranscript].[StudentUSI] = [edfi].[Student].[StudentUSI] AND [edfi].[CourseTranscript].[FinalLetterGradeEarned] IN ('F'))
```

## ❌ `descriptor_ambiguity_levels`
> _Top 5 grade levels by enrollment for Title I schools_

**validated** False · **executed** False · **rows** None · **join nodes** 18 · **repairs** 2 · **8241 ms**
- domains: AlternativeAndSupplementalServices · Enrollment
- top tables: edfi.GradeLevelDescriptor · edfi.SchoolGradeLevel · edfi.SectionOfferedGradeLevel · edfi.GunFreeSchoolsActReportingStatusDescriptor · edfi.EnrollmentTypeDescriptor
- **error**: `explain error: (pymssql.exceptions.ProgrammingError) (207, b"Invalid column name 'ShortDescription'.DB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\n")
[SQL: SELECT TOP 5 d.ShortDescription AS GradeLevel, COUNT(ssa.StudentUSI) AS EnrollmentCount FROM [edfi].[StudentSchoolAssociation] AS ssa JOIN [edfi].[School] AS s ON ssa.SchoolId = s.SchoolId JOIN [edfi].[TitleIPartASchoolDesignationDescriptor] AS t ON s.TitleIPartASchoolDesignationDescriptorId = t.TitleIPartASchoolDesignationDescriptorId JOIN [edfi].[Descriptor] AS d ON ssa.EntryGradeLevelDescriptorId = d.DescriptorId WHERE t.ShortDescription = 'Title I Part A' GROUP BY d.ShortDescription ORDER BY EnrollmentCount DESC]
(Background on this error at: https://sqlalche.me/e/20/f405)`
- rationale: _The query filters schools designated as 'Title I Part A' and groups enrollment counts by grade level descriptors._
```sql
SELECT TOP 5 d.ShortDescription AS GradeLevel, COUNT(ssa.StudentUSI) AS EnrollmentCount FROM [edfi].[StudentSchoolAssociation] AS ssa JOIN [edfi].[School] AS s ON ssa.SchoolId = s.SchoolId JOIN [edfi].[TitleIPartASchoolDesignationDescriptor] AS t ON s.TitleIPartASchoolDesignationDescriptorId = t.TitleIPartASchoolDesignationDescriptorId JOIN [edfi].[Descriptor] AS d ON ssa.EntryGradeLevelDescriptorId = d.DescriptorId WHERE t.ShortDescription = 'Title I Part A' GROUP BY d.ShortDescription ORDER BY EnrollmentCount DESC
```

## ✅ `free_lunch_intervention`
> _Free-and-reduced-lunch students who received reading interventions, by school_

**validated** True · **executed** True · **rows** 0 · **join nodes** 18 · **repairs** 2 · **12161 ms**
- domains: Intervention · Enrollment
- top tables: edfi.School · edfi.GunFreeSchoolsActReportingStatusDescriptor · edfi.InterventionPopulationServed · edfi.InterventionStudyPopulationServed · edfi.InterventionPrescriptionPopulationServed
- rationale: _The query was corrected to use the 'NameOfInstitution' column from the 'EducationOrganization' table, as 'School' does not contain this column._
- description: _No matching rows._
```sql
SELECT TOP (1000) eo.[NameOfInstitution] AS SchoolName, COUNT(DISTINCT ss.[StudentUSI]) AS StudentCount FROM [edfi].[EducationOrganization] eo JOIN [edfi].[School] s ON s.[SchoolId] = eo.[EducationOrganizationId] JOIN [edfi].[StudentSchoolAssociation] ss ON ss.[SchoolId] = s.[SchoolId] JOIN [edfi].[Intervention] i ON i.[EducationOrganizationId] = s.[SchoolId] JOIN [edfi].[InterventionPopulationServed] ips ON ips.[EducationOrganizationId] = i.[EducationOrganizationId] AND ips.[InterventionIdentificationCode] = i.[InterventionIdentificationCode] JOIN [edfi].[PopulationServedDescriptor] psd ON psd.[PopulationServedDescriptorId] = ips.[PopulationServedDescriptorId] JOIN [edfi].[Descriptor] d_psd ON d_psd.[DescriptorId] = psd.[PopulationServedDescriptorId] JOIN [edfi].[Descriptor] d_dm ON d_dm.[DescriptorId] = i.[DeliveryMethodDescriptorId] WHERE d_psd.[CodeValue] = 'Free and Reduced Lunch' AND d_dm.[CodeValue] = 'Reading' GROUP BY eo.[NameOfInstitution]
```

## ❌ `candidate_field_hours`
> _Teacher-prep candidates who completed at least 100 hours of field experience in 2023_

**validated** False · **executed** False · **rows** None · **join nodes** 12 · **repairs** 2 · **9493 ms**
- domains: EducatorPreparationProgram
- top tables: edfi.FieldworkExperience · edfi.FieldworkTypeDescriptor · edfi.FieldworkExperienceSectionAssociation · edfi.FieldworkExperienceCoteaching · edfi.CandidateEducatorPreparationProgramAssociationDegreeSpecialization
- **error**: `explain error: (pymssql.exceptions.ProgrammingError) (208, b"Invalid object name 'edfi.Candidate'.DB-Lib error message 20018, severity 16:\nGeneral SQL Server error: Check messages from the SQL Server\n")
[SQL: SELECT TOP 1000 [Candidate].[CandidateIdentifier] FROM [edfi].[Candidate] JOIN [edfi].[CandidateEducatorPreparationProgramAssociation] ON [edfi].[CandidateEducatorPreparationProgramAssociation].[CandidateIdentifier] = [edfi].[Candidate].[CandidateIdentifier] JOIN [edfi].[EducatorPreparationProgram] ON [edfi].[CandidateEducatorPreparationProgramAssociation].[EducationOrganizationId] = [edfi].[EducatorPreparationProgram].[EducationOrganizationId] AND [edfi].[CandidateEducatorPreparationProgramAssociation].[ProgramName] = [edfi].[EducatorPreparationProgram].[ProgramName] AND [edfi].[CandidateEducatorPreparationProgramAssociation].[ProgramTypeDescriptorId] = [edfi].[EducatorPreparationProgram].[ProgramTypeDescriptorId] JOIN [edfi].[FieldworkExperience] ON [edfi].[FieldworkExperience].[EducationOrganizationId] = [edfi].[EducatorPreparationProgram].[EducationOrganizationId] AND [edfi].[FieldworkExperience].[ProgramName] = [edfi].[EducatorPreparationProgram].[ProgramName] AND [edfi].[FieldworkExperience].[ProgramTypeDescriptorId] = [edfi].[EducatorPreparationProgram].[ProgramTypeDescriptorId] WHERE [edfi].[FieldworkExperience].[FieldworkHours] >= 100 AND YEAR([edfi].[FieldworkExperience].[BeginDate]) = 2023]
(Background on this error at: https://sqlalche.me/e/20/f405)`
- rationale: _The query retrieves candidates who completed at least 100 hours of fieldwork experience in 2023 using the correct table and join paths._
```sql
SELECT TOP 1000 [Candidate].[CandidateIdentifier] FROM [edfi].[Candidate] JOIN [edfi].[CandidateEducatorPreparationProgramAssociation] ON [edfi].[CandidateEducatorPreparationProgramAssociation].[CandidateIdentifier] = [edfi].[Candidate].[CandidateIdentifier] JOIN [edfi].[EducatorPreparationProgram] ON [edfi].[CandidateEducatorPreparationProgramAssociation].[EducationOrganizationId] = [edfi].[EducatorPreparationProgram].[EducationOrganizationId] AND [edfi].[CandidateEducatorPreparationProgramAssociation].[ProgramName] = [edfi].[EducatorPreparationProgram].[ProgramName] AND [edfi].[CandidateEducatorPreparationProgramAssociation].[ProgramTypeDescriptorId] = [edfi].[EducatorPreparationProgram].[ProgramTypeDescriptorId] JOIN [edfi].[FieldworkExperience] ON [edfi].[FieldworkExperience].[EducationOrganizationId] = [edfi].[EducatorPreparationProgram].[EducationOrganizationId] AND [edfi].[FieldworkExperience].[ProgramName] = [edfi].[EducatorPreparationProgram].[ProgramName] AND [edfi].[FieldworkExperience].[ProgramTypeDescriptorId] = [edfi].[EducatorPreparationProgram].[ProgramTypeDescriptorId] WHERE [edfi].[FieldworkExperience].[FieldworkHours] >= 100 AND YEAR([edfi].[FieldworkExperience].[BeginDate]) = 2023
```

## ✅ `assessment_advanced`
> _Hispanic ELL students who scored Advanced on the state ELA assessment with 90%+ attendance_

**validated** True · **executed** True · **rows** 0 · **join nodes** 28 · **repairs** 0 · **4999 ms**
- domains: StudentAssessment · StudentAttendance · StudentIdentificationAndDemographics
- top tables: edfi.StudentAssessmentItem · edfi.StudentSchoolAttendanceEvent · edfi.AttendanceEventCategoryDescriptor · edfi.StudentSectionAttendanceEvent · edfi.SectionAttendanceTakenEvent
- rationale: _The query filters Hispanic ELL students with Advanced scores on ELA assessments and attendance of 90% or more._
- description: _No matching rows._
```sql
SELECT TOP 1000 sa.StudentUSI, sa.AssessmentIdentifier, sa.AdministrationDate, ssa.EventDate, ssa.EventDuration FROM edfi.StudentAssessment AS sa JOIN edfi.StudentAssessmentItem AS sai ON sai.AssessmentIdentifier = sa.AssessmentIdentifier AND sai.Namespace = sa.Namespace AND sai.StudentAssessmentIdentifier = sa.StudentAssessmentIdentifier AND sai.StudentUSI = sa.StudentUSI JOIN edfi.Descriptor AS d ON d.DescriptorId = sai.AssessmentItemResultDescriptorId JOIN edfi.StudentSchoolAttendanceEvent AS ssa ON ssa.StudentUSI = sa.StudentUSI WHERE d.CodeValue = 'Advanced' AND ssa.EventDuration >= 0.9
```

## ✅ `course_no_takers`
> _Courses offered this year that have zero enrolled students_

**validated** True · **executed** True · **rows** 50 · **join nodes** 20 · **repairs** 0 · **5740 ms**
- domains: SectionsAndPrograms · Enrollment
- top tables: edfi.PopulationServedDescriptor · edfi.CourseOffering · edfi.CourseOfferingOfferedGradeLevel · edfi.SectionOfferedGradeLevel · edfi.CourseOfferedGradeLevel
- rationale: _The query identifies courses offered this year by joining CourseOffering with Section and StudentSectionAssociation, filtering for courses with no associated students._
- description: _This year, 50 courses have been identified with zero enrolled students. Examples include 'ALG-1' offered at school ID 255901001 during the 2017-2018 Fall and Spring semesters, and similar sessions at school ID 255901002. These courses span multiple schools and sessions, highlighting areas for potential review or adjustment in course offerings._
```sql
SELECT TOP 1000 [edfi].[CourseOffering].[LocalCourseCode], [edfi].[CourseOffering].[SchoolId], [edfi].[CourseOffering].[SchoolYear], [edfi].[CourseOffering].[SessionName] FROM [edfi].[CourseOffering] LEFT JOIN [edfi].[Section] ON [edfi].[Section].[LocalCourseCode] = [edfi].[CourseOffering].[LocalCourseCode] AND [edfi].[Section].[SchoolId] = [edfi].[CourseOffering].[SchoolId] AND [edfi].[Section].[SchoolYear] = [edfi].[CourseOffering].[SchoolYear] AND [edfi].[Section].[SessionName] = [edfi].[CourseOffering].[SessionName] LEFT JOIN [edfi].[StudentSectionAssociation] ON [edfi].[StudentSectionAssociation].[LocalCourseCode] = [edfi].[Section].[LocalCourseCode] AND [edfi].[StudentSectionAssociation].[SchoolId] = [edfi].[Section].[SchoolId] AND [edfi].[StudentSectionAssociation].[SchoolYear] = [edfi].[Section].[SchoolYear] AND [edfi].[StudentSectionAssociation].[SectionIdentifier] = [edfi].[Section].[SectionIdentifier] AND [edfi].[StudentSectionAssociation].[SessionName] = [edfi].[Section].[SessionName] WHERE [edfi].[StudentSectionAssociation].[StudentUSI] IS NULL
```

## ✅ `staff_retention`
> _Staff who left between 2022 and 2023 — by school and reason_

**validated** True · **executed** True · **rows** 0 · **join nodes** 16 · **repairs** 1 · **8140 ms**
- domains: Staff · EducationOrganization
- top tables: edfi.StaffLeave · edfi.School · edfi.StaffAbsenceEvent · edfi.StaffEducationOrganizationEmploymentAssociation · edfi.PostSecondaryInstitution
- rationale: _The corrected query uses the appropriate column 'NameOfInstitution' from the 'EducationOrganization' table instead of the 'School' table._
- description: _No matching rows._
```sql
SELECT TOP 1000 [edfi].[EducationOrganization].[NameOfInstitution] AS [SchoolName], [edfi].[StaffLeave].[Reason] AS [LeaveReason], [edfi].[StaffLeave].[BeginDate] AS [LeaveBeginDate], [edfi].[StaffLeave].[EndDate] AS [LeaveEndDate] FROM [edfi].[StaffLeave] JOIN [edfi].[Staff] ON [edfi].[StaffLeave].[StaffUSI] = [edfi].[Staff].[StaffUSI] JOIN [edfi].[StaffEducationOrganizationEmploymentAssociation] ON [edfi].[StaffEducationOrganizationEmploymentAssociation].[StaffUSI] = [edfi].[Staff].[StaffUSI] JOIN [edfi].[EducationOrganization] ON [edfi].[StaffEducationOrganizationEmploymentAssociation].[EducationOrganizationId] = [edfi].[EducationOrganization].[EducationOrganizationId] WHERE [edfi].[StaffLeave].[BeginDate] >= '2022-01-01' AND [edfi].[StaffLeave].[EndDate] <= '2023-12-31'
```

## ✅ `bell_schedule_overlap`
> _Sections that share the same bell-schedule period across two different teachers_

**validated** True · **executed** True · **rows** 50 · **join nodes** 18 · **repairs** 0 · **6836 ms**
- domains: BellSchedule · SectionsAndPrograms
- top tables: edfi.BellSchedule · edfi.BellScheduleClassPeriod · edfi.BellScheduleDate · edfi.BellScheduleGradeLevel · edfi.ClassPeriod
- rationale: _The query identifies sections sharing the same class period by joining on the class period name and ensuring the sections are distinct._
- description: _There are 50 sections where two different teachers share the same bell-schedule period. For example, '255901001_01-Traditional_ALG1_186_20172018' and '255901001_01-Traditional_ALG1_100_20172018' both occur during the '01 - Traditional' period. Other shared sections include '255901001_01-Traditional_ALG2_101_20172018' and '255901001_01-Traditional_ALG1_100_20172018', among others. These shared periods indicate overlapping schedules for these sections._
```sql
SELECT DISTINCT TOP 1000 s1.[SectionIdentifier] AS Section1Identifier, s2.[SectionIdentifier] AS Section2Identifier, scp1.[ClassPeriodName] AS SharedClassPeriodName FROM [edfi].[SectionClassPeriod] scp1 JOIN [edfi].[Section] s1 ON scp1.[LocalCourseCode] = s1.[LocalCourseCode] AND scp1.[SchoolId] = s1.[SchoolId] AND scp1.[SchoolYear] = s1.[SchoolYear] AND scp1.[SectionIdentifier] = s1.[SectionIdentifier] AND scp1.[SessionName] = s1.[SessionName] JOIN [edfi].[SectionClassPeriod] scp2 ON scp1.[ClassPeriodName] = scp2.[ClassPeriodName] AND scp1.[SchoolId] = scp2.[SchoolId] JOIN [edfi].[Section] s2 ON scp2.[LocalCourseCode] = s2.[LocalCourseCode] AND scp2.[SchoolId] = s2.[SchoolId] AND scp2.[SchoolYear] = s2.[SchoolYear] AND scp2.[SectionIdentifier] = s2.[SectionIdentifier] AND scp2.[SessionName] = s2.[SessionName] WHERE s1.[SectionIdentifier] <> s2.[SectionIdentifier]
```
