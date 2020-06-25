from google.cloud.spanner import Client


class EmsSpannerClient:
    def __init__(self, project_id: str, instance_id, db_id):
        self.project_id = project_id
        self.__db = Client(project_id).instance(instance_id).database(db_id)

    def execute_sql(self, query: str):
        with self.__db.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
            for row in results:
                yield row

    def execute_update(self, query: str):
        def run_query(transaction):
            transaction.execute_update(query)
        self.__db.run_in_transaction(run_query)