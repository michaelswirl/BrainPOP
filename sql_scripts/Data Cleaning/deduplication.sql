-- Use a CTE to rank rows within the staging table
WITH partitioned_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ID
            ORDER BY ETL_LAST_UPDATED_TS ASC, ACCOUNT_ID ASC
        ) AS row_num
    FROM stg_student_activities
),
-- Filter to get the top-ranked rows for each ID
deduplicated_data AS (
    SELECT *
    FROM partitioned_data
    WHERE row_num = 1
),

-- Filtering out records already present in the target table by ID
new_data_to_insert AS (
    SELECT *
    FROM deduplicated_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM brainpop.student_activities AS target
        WHERE target.ID = deduplicated_data.ID
    )
)

-- Insert only the new records into the target table
INSERT INTO brainpop.student_activities
SELECT id, student_id, account_id, resource_type, score, quiz_id, CURRENT_TIMESTAMP
FROM new_data_to_insert;
