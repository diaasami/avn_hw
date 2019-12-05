# Setup

1. Edit `env` to put your credentials and other variables
2. Copy `service.key`, `service.cert` and `ca.pem` next to `setup.sh`
3. Run `setup.sh`
4. Run `run_tests.sh` and if everything is OK, proceed to next step
5. Run `run_producer.sh`, `run_consumer.sh`

# Code

* Tests are in `tests.py`
* Producer code is in `producer.py`
* Consumer code is in `consumer.py`
* DB setup code is in `setup_db.sql`
