CREATE TABLE regional_user (
    job_id bigint NOT NULL,
    user_id varchar(255) NOT NULL,
    presense_count bigint NOT NULL,
    CONSTRAINT pk_regional_user PRIMARY KEY (job_id, user_id),
    CONSTRAINT ck_regional_user_presense_count CHECK (presense_count >= 0)
);
