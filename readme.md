# Setup

1. Edit `env` to put your credentials and other variables
1. Create database using `setup_db.sql`
1. Copy `service.key`, `service.cert` and `ca.pem` next to `setup.sh`
1. Run `setup.sh`
1. Run `run_tests.sh` and if everything is OK, proceed to next step
1. Run `run_producer.sh`, `run_consumer.sh`

# Code

* Tests are in `tests.py`
* Producer code is in `producer.py`
* Consumer code is in `consumer.py`
* DB setup code is in `setup_db.sql`
