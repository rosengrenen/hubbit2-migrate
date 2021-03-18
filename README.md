# hubbit2-migrate

This is a simple tool to import hubbit (v1) data to hubbit2, extracting all relevant data from a db dump.

## Usage

1. Place `hubbit-backup.sql` in the root
2. Run `yarn` or `npm install` to install dependencies
3. Run `yarn parse` to transform the sql dump to a json file
4. Now make sure that your database is running and has up to date schema
5. Fill in the env vars by copying `.env.example` to `.env` (adding a Gamma api key will map cids to gamma user ids correctly)
6. Run `yarn sync` to add the data to the database (WARNING: this will remove all existing data in the `mac_addresses`, `sessions` and `user_sessions` tables)

## Todo

- Currently created and updated at fields are not imported
