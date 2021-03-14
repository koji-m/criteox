# criteox

Criteo Data Extractor

## usage

Set parameters in environment variables and execute. The extracted data is output to standard output in JSON format.

```shell
BASE_DATE='YYYY-mm-dd' \
LOOKBACK_WINDOW=3 \ # number of days back from the base date
CRITEO_CLIENT_ID='...' \
CRITEO_CLIENT_SECRET='...' \
cargo run
```
