import db as db_init
import load_data as db_load
import test_queries as db_testing

def main():
	db_init.init_db()
	db_load.load_data()
	db_testing.main()