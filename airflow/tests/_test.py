from airflow.models import DagBag

def test_dag_imports():
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
