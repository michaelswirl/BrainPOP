DROP TABLE IF EXISTS public.google_trends;

-- This table  is in the default Public Schema since it is only being used to assist in the generation of reasonable synthetic data.

CREATE TABLE public.google_trends (
    DATE DATE NOT NULL,
    FREQUENCY INT NOT NULL,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
