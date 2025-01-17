DROP TABLE IF EXISTS brainpop.student_activities;
DROP TABLE IF EXISTS public.stg_student_activities;

-- For the purpose of this exercise, I wil not be creating a separate schema for staging data, since there will only be 4-5 tables in total.

--This table is in the default Public Schema as a Staging Table

CREATE TABLE public.stg_student_activities (
    ID BIGINT NOT NULL, -- Becuase IDs are duplicated from the API call, this staging table will not be auto-incremented
    STUDENT_ID BIGINT NOT NULL,
    ACCOUNT_ID BIGINT NOT NULL,
    RESOURCE_TYPE VARCHAR(10) NOT NULL,
    SCORE INT,
    QUIZ_ID INT,
    ETL_LAST_UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--This table is in the Brainpop Schema as the Final Table

CREATE TABLE brainpop.student_activities (
    ID BIGINT NOT NULL, -- Auto-incremented ID to ensure uniqueness
    STUDENT_ID BIGINT NOT NULL,
    ACCOUNT_ID BIGINT NOT NULL,
    RESOURCE_TYPE VARCHAR(10) NOT NULL,
    SCORE INT,
    QUIZ_ID INT,
    ETL_LAST_UPDATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



