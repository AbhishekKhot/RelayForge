ALTER TABLE delivery_work ADD COLUMN processing_deadline_at timestamptz;
ALTER TABLE deliveries ADD COLUMN processing_deadline_at timestamptz;
