from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.session import NEW_SESSION, provide_session

from typing import TYPE_CHECKING, Any, Generator, Iterable, cast, overload

import json
import uuid
import os
import csv

class CustomXComBackend(BaseXCom):
    S3_CONN_ID = 'my_s3'
    BUCKET_NAME = 'wjzawsbucket'

    @staticmethod
    def serialize_value(value, *, key = None, task_id = None, dag_id = None, run_id = None, map_index = None, **kwargs):
        hook = S3Hook(aws_conn_id=CustomXComBackend.S3_CONN_ID)

        with open('/Users/jiazhenwang/Downloads/tmp/1.txt','a+') as f:
            f.write(f'########{type(value)}########')
            f.close()

        if isinstance(value,list) or isinstance(value,tuple):
            filename = "data_" + str(uuid.uuid4()) + ".csv"
            s3_key = f'{run_id}/{task_id}/{filename}'
            with open(filename,'a+') as f:
                writer = csv.writer(f)
                writer.writerows(value)
                f.close()
        else:
            filename = "data_" + str(uuid.uuid4()) + ".json"
            s3_key = f'{run_id}/{task_id}/{filename}'
            with open(filename,'a+') as f:
                json.dump(value ,f)
        hook.load_file(
            filename=filename,
            key=s3_key,
            bucket_name=CustomXComBackend.BUCKET_NAME,
            replace=True
        )
        os.remove(filename)

        reference_string = s3_key

        return BaseXCom.serialize_value(value=reference_string)

    @staticmethod
    def deserialize_value(result):
        s3_key = BaseXCom.deserialize_value(result)

        hook = S3Hook(aws_conn_id=CustomXComBackend.S3_CONN_ID)

        filename = hook.download_file(
            key=s3_key,
            bucket_name=CustomXComBackend.BUCKET_NAME,
            local_path="/Users/jiazhenwang/Downloads/tmp"
        )

        # added deserialization option to convert a CSV back to a dataframe
        if s3_key.split(".")[-1] == "csv":
            output =[]
            with open(filename) as f:
                reader = csv.reader(f)
                for row in reader:
                    output.append(row)
        # if the key does not end in 'csv' use JSON deserialization
        else:
            with open(filename, 'r') as f:
                output = json.load(f)

        # remove the local temporary file
        os.remove(filename)

        return output


    @classmethod
    @provide_session
    def clear(
        cls,
        execution_date = None,
        dag_id = None,
        task_id =  None,
        session = NEW_SESSION,
        *,
        run_id = None,
        map_index = None,
    ) -> None:

        from airflow.models import DagRun
        from airflow.utils.helpers import exactly_one
        import warnings
        from airflow.exceptions import RemovedInAirflow3Warning

        if dag_id is None:
            raise TypeError("clear() missing required argument: dag_id")
        if task_id is None:
            raise TypeError("clear() missing required argument: task_id")

        if not exactly_one(execution_date is not None, run_id is not None):
            raise ValueError(
                f"Exactly one of run_id or execution_date must be passed. "
                f"Passed execution_date={execution_date}, run_id={run_id}"
            )

        if execution_date is not None:
            message = "Passing 'execution_date' to 'XCom.clear()' is deprecated. Use 'run_id' instead."
            warnings.warn(message, RemovedInAirflow3Warning, stacklevel=3)
            run_id = (
                session.query(DagRun.run_id)
                .filter(DagRun.dag_id == dag_id, DagRun.execution_date == execution_date)
                .scalar()
            )

        #### Customization start

        # get the reference string from the Airflow metadata database
        if map_index is not None:
            reference_string = session.query(cls.value).filter_by(
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index
            ).scalar()
        else:
            reference_string = session.query(cls.value).filter_by(
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id
            ).scalar()

        if reference_string is not None:

            # decode the XCom binary to UTF-8
            reference_string = reference_string.decode('utf-8')
            
            hook = S3Hook(aws_conn_id="s3_xcom_backend_conn")
            key = reference_string

            # use the reference string to delete the object from the S3 bucket
            hook.delete_objects(
                bucket=CustomXComBackend.BUCKET_NAME,
                keys=json.loads(key)
            )

        # retrieve the XCom record from the metadata database containing the reference string
        query = session.query(cls).filter_by(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id
        )
        if map_index is not None:
            query = query.filter_by(map_index=map_index)

        # delete the XCom containing the reference string from metadata database
        query.delete()