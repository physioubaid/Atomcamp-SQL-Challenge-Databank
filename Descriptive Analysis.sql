describe customer_nodes;

SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = 'data_bank';

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'data_bank' AND table_type = 'BASE TABLE';
describe customer_nodes;
describe customer_transactions;
describe regions;
describe web_events;

SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'data_bank' AND table_name = 'customer_nodes';

