 Repo provides a docker compose setup, which will create two containers: the python app, postgres database
 to run in **one go**:
 
 ``docker build --no-cache -t zeal-python . && docker compose up --exit-code-from app``

That is sufficient to run the whole app, which will spit out the results to the console when done. After, the app will shutdown and stop the containers.

***Gotcha***

The current (somewhat unfortunate) implementation takes a while to ingest all data. Expect 30 to 40 mins of database grunting.



To keep the db alive after data load (test sql statements or play with the data), just remove the exit code flag as such:
``docker build --no-cache -t zeal-python . && docker compose up``

Access the DB via

``docker exec -it zeal-cricket-db-1 bash``

and the DB via

``psql -U postgres``

all credentials are in the .env-vars file, and defaults -> not to used in any other scenario, or god forbid production

To build the python image as a seperate step (not necessary if step above has run):

``docker build --no-cache -t zeal-python .``

To start containers as a single step:

 ``docker compose up --exit-code-from app``

 OR

 ``docker compose up``
 
 to keep the db alive after the pipeline is done.