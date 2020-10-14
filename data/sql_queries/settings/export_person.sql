  -- Prevent the count from showing up in the text file results
SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

:OUT C:\Users\Jay\Desktop\cohd_covid\person_q2r2.txt
SELECT person_id, gender_concept_id, race_concept_id, ethnicity_concept_id
FROM person;
