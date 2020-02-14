#Functions
CREATE OR REPLACE FUNCTION populate_gdelt() RETURNS void AS $$
BEGIN

UPDATE staging
SET person_id = persons.id
FROM persons
WHERE staging.person = persons.person;

UPDATE staging
SET theme_id = themes.id
FROM themes
WHERE staging.theme = themes.theme;

UPDATE staging
SET organization_id = organizations.id
FROM organizations
WHERE staging.organization = organizations.organization;

INSERT INTO persons(person)
SELECT DISTINCT person FROM staging
WHERE person_id is NULL;

INSERT INTO organizations(organization)
SELECT DISTINCT organization FROM staging
WHERE organization_id is NULL;

UPDATE staging
SET person_id = persons.id
FROM persons
WHERE staging.person = persons.person AND person_id IS NULL;

UPDATE staging
SET organization_id = organizations.id
FROM organizations
WHERE staging.organization = organizations.organization AND organization_id IS NULL;

INSERT INTO gdelt 
SELECT DISTINCT file_date, publication_id, 
source_collection_id, person_id, theme_id, organization_id 
FROM staging;

TRUNCATE TABLE staging;

END; $$ 
LANGUAGE plpgsql;



###################


UPDATE elections_prod
SET person = 'Bernie Sanders'
WHERE person like '%bernie sanders%';

UPDATE elections_prod
SET person = 'Joe Biden'
WHERE person like '%joe biden%';

UPDATE elections_prod
SET person = 'Pete Buttigieg'
WHERE person like '%pete buttigieg%';

UPDATE elections_prod
SET person = 'Michael Bloomberg'
WHERE person like '%michael bloomberg%';

UPDATE elections_prod
SET person = 'Amy Klobuchar'
WHERE person like '%amy klobuchar%';

UPDATE elections_prod
SET person = 'Tom Steyer'
WHERE person like '%tom steyer%';

UPDATE elections_prod
SET person = 'Donald Trump'
WHERE person like '%donald trump%';

UPDATE elections_prod
SET person = 'Elizabeth Warren'
WHERE person like '%elizabeth warren%';

DELETE FROM elections_prod WHERE person NOT IN ('Bernie Sanders', 'Joe Biden', 'Pete Buttigieg', 'Michael Bloomberg', 'Amy Klobuchar', 'Tom Steyer', 'Donald Trump');

#query for tableau data source
SELECT file_date, person, AVG(ave_tone) as tone, AVG(postivie_score) as postivie_score, AVG(negative_score) as negative_score, AVG(polarity) as polarity, count(*) as rec_count
FROM elections_prod
GROUP BY file_date, person;

INSERT INTO elections_prod (file_date, person, theme, ave_tone, postivie_score, negative_score, polarity)
SELECT to_timestamp(file_date::text, 'YYYYMMDDHH24MISS') as file_date, person, theme, ave_tone, postivie_score, negative_score, polarity 
FROM elections WHERE person like '%elizabeth warren%';

