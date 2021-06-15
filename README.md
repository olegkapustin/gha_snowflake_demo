# Github Archive >> Snowflake demo

## Infrastructure

1. Get a Snowflake demo account
2. Create Azure Data Lake storage account
3. Create ADLS filesystem/container called "lake"
4. Create folder `gha` in your "lake"

## Dataset

1. Get some Github event archives from [here](https://www.gharchive.org/). Note that archive names have `YYYY-MM-DD-HH.json.gz` names.
2. Upload archives as-is to your `gha` folder in ADLS.

## Demo

Go through SQL scripts in this repo. You can use Snowflake UI or
VS Code with [this extension](https://marketplace.visualstudio.com/items?itemName=koszti.snowflake-driver-for-sqltools).

Please note that you will need to adjust lines 22-23 in [`gha_snowflake_part1.sql`](./gha_snowflake_part1.sql):

- replace `<YOUR ACCOUNT NAME>` with the name of your storage account;
- replace `<YOUR SAS TOKEN>` with your SAS token (grant access to read and list blobs)
