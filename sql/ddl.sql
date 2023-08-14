DROP TABLE IF EXISTS ehr_db.encounters;
DROP TABLE IF EXISTS ehr_db.conditions;
DROP TABLE IF EXISTS ehr_db.patients;
DROP TABLE IF EXISTS ehr_db.medications;


CREATE TABLE ehr_db.encounters (
    id VARCHAR(40) NOT NULL PRIMARY KEY,
    startTime TIMESTAMP,
    endTime TIMESTAMP,
    status VARCHAR(255),
    patientId VARCHAR(40),
    encounterType_system VARCHAR(255),
    encounterType_code VARCHAR(15),
    encounterType_text VARCHAR(255),
    practitioner_npi VARCHAR(15),
    practitioner_name VARCHAR(255),
    provider_id VARCHAR(40),
    provider_name VARCHAR(255),
    data_dt DATE
);

CREATE TABLE ehr_db.medications (
    id VARCHAR(40) NOT NULL PRIMARY KEY,
    dateWritten TIMESTAMP,
    status VARCHAR(255),
    patientId VARCHAR(40),
    encounterId VARCHAR(40), 
    prescriber_npi VARCHAR(15),
    prescriber_name VARCHAR(255),
    medication_system VARCHAR(255),
    medication_code VARCHAR(15),
    medication_text VARCHAR(255),
    data_dt DATE
);

CREATE TABLE ehr_db.patients (
    id VARCHAR(40) PRIMARY KEY NOT NULL,
    gender VARCHAR(8),
    birthDate DATE,
    deceasedDateTime TIMESTAMP, 
    name_given VARCHAR(100),
    name_family VARCHAR(100),
    name_use VARCHAR(15),
    maritalStatus VARCHAR(15),
    race VARCHAR(100),
    ethnicity VARCHAR(100),
    mothersMaidenName VARCHAR(100),
    birthSex VARCHAR(8),
    disabilityAdjustedLifeYears FLOAT,
    qualityAdjustedLifeYears FLOAT, 
    birthPlace_city VARCHAR(100),
    birthPlace_state VARCHAR(100),
    birthPlace_country VARCHAR(100),
    isDeceased VARCHAR(8),
    data_dt DATE
);

CREATE TABLE ehr_db.conditions (
    id VARCHAR(40) NOT NULL PRIMARY KEY,
    patientId VARCHAR(40),
    encounterId VARCHAR(40),
    dateRecorded DATE, 
    code_system VARCHAR(255),
    code_code VARCHAR(15),
    code_text VARCHAR(255),
    category_system  VARCHAR(255),
    category_code VARCHAR(15),
    category_text VARCHAR(255),
    clinicalStatus VARCHAR(100),
    verificationStatus VARCHAR(100),
    onsetDateTime TIMESTAMP, 
    abatementDateTime TIMESTAMP,
    data_dt DATE
);