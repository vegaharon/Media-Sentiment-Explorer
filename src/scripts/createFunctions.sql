--Functions
CREATE OR REPLACE FUNCTION populate_gdelt() RETURNS void AS $$
BEGIN

--update person_id in the staging table
UPDATE staging
SET person_id = persons.id
FROM persons
WHERE staging.person = persons.person;

--update theme_id in the staging table
UPDATE staging
SET theme_id = themes.id
FROM themes
WHERE staging.theme = themes.theme;

--update organization_id in the staging table
UPDATE staging
SET organization_id = organizations.id
FROM organizations
WHERE staging.organization = organizations.organization;

--insert persons that are new into persons table, they will acuire an id
INSERT INTO persons(person)
SELECT DISTINCT person FROM staging
WHERE person_id is NULL;

--insert organizations that are new into organizations table, they will acuire an id
INSERT INTO organizations(organization)
SELECT DISTINCT organization FROM staging
WHERE organization_id is NULL;

--update person_id in the staging table for the new persons
UPDATE staging
SET person_id = persons.id
FROM persons
WHERE staging.person = persons.person AND person_id IS NULL;

--update organization_id in the staging table for the new organizations
UPDATE staging
SET organization_id = organizations.id
FROM organizations
WHERE staging.organization = organizations.organization AND organization_id IS NULL;

--insert the the normalized columns into gdelt table 
INSERT INTO gdelt 
SELECT DISTINCT file_date, publication_id, 
source_collection_id, person_id, theme_id, organization_id 
FROM staging;

--after loading a file the staging table empty the staging table 
TRUNCATE TABLE staging;

END; $$ 
LANGUAGE plpgsql;



###################

CREATE OR REPLACE FUNCTION populate_elections() RETURNS void AS $$
BEGIN

--copy elections related records from elctions table to elections_production table and convert filedate to time stamp.  
INSERT INTO elections_prod (file_date, person, theme, ave_tone, postivie_score, negative_score, polarity)
SELECT to_timestamp(file_date::text, 'YYYYMMDDHH24MISS') as file_date, person, theme, ave_tone, postivie_score, negative_score, polarity 
FROM elections WHERE (person LIKE '%bernie sanders%' OR person LIKE '%joe biden%' OR 
person LIKE '%pete buttigieg%' OR person LIKE '%michael bloomberg%' OR 
person LIKE '%amy klobuchar%' OR person LIKE '%elizabeth warren%' OR 
person LIKE '%tom steyer%' OR person LIKE '%donald trump%') ;

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

--delete irrelavant records just in case, there should not be any.
DELETE FROM elections_prod WHERE person NOT IN ('Bernie Sanders', 'Joe Biden', 'Pete Buttigieg', 'Elizabeth Warren', 'Michael Bloomberg', 'Amy Klobuchar', 'Tom Steyer', 'Donald Trump');

END; $$ 
LANGUAGE plpgsql;

--query for tableau as data source. 
SELECT file_date, person, AVG(ave_tone) as tone, AVG(postivie_score) as postivie_score, AVG(negative_score) as negative_score, AVG(polarity) as polarity, count(*) as rec_count
FROM elections_prod
GROUP BY file_date, person;
