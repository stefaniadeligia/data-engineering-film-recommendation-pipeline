#Task1 Stage1 > stage1.py

#Task1 Stage2 > stage2.py

#Task1 pipeline > pipeline.py

#To run Task 1 Luigi pipeline:
luigi --module pipeline ProcessGenres --local-scheduler

#Task3 > query_films.py

#To run the Task 3 test:
pytest test_films.py