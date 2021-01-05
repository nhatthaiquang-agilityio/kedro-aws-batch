from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import Any, Dict, Set

import boto3

from kedro.io import DataCatalog
from kedro.pipeline.pipeline import Pipeline, Node
from kedro.runner import ThreadRunner


class AWSBatchRunner(ThreadRunner):
    def __init__(
        self,
        max_workers: int = None,
        job_queue: str = None,
        job_definition: str = None,
        is_async: bool = False,
    ):
        super().__init__(max_workers, is_async=is_async)
        self._job_queue = job_queue
        self._job_definition = job_definition
        self._client = boto3.client("batch")

    def create_default_data_set(self, ds_name: str):
        raise NotImplementedError(
            "All datasets must be defined in the catalog")

    def _get_required_workers_count(self, pipeline: Pipeline):
        if self._max_workers is not None:
            return self._max_workers

        return super()._get_required_workers_count(pipeline)

    def _run(  # pylint: disable=too-many-locals,useless-suppression
        self, pipeline: Pipeline, catalog: DataCatalog, run_id: str = None
    ) -> None:
        nodes = pipeline.nodes
        node_dependencies = pipeline.node_dependencies
        todo_nodes = set(node_dependencies.keys())
        node_to_job = dict()
        done_nodes = set()  # type: Set[Node]
        futures = set()
        max_workers = self._get_required_workers_count(pipeline)

        self._logger.info("Max workers: %d", max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            while True:
                # Process the nodes that have completed, i.e. jobs that reached
                # FAILED or SUCCEEDED state
                done = {fut for fut in futures if fut.done()}
                futures -= done
                for future in done:
                    try:
                        node = future.result()
                    except Exception:
                        self._suggest_resume_scenario(pipeline, done_nodes)
                        raise
                    done_nodes.add(node)
                    self._logger.info(
                        "Completed %d out of %d jobs",
                        len(done_nodes),
                        len(nodes)
                    )

                # A node is ready to be run if all
                # its upstream dependencies have been
                # submitted to Batch, i.e.
                # all node dependencies were assigned a job ID
                ready = {
                    n for n in todo_nodes if node_dependencies[n] <= node_to_job.keys()
                }
                todo_nodes -= ready
                # Asynchronously submit Batch jobs
                for node in ready:
                    future = pool.submit(
                        self._submit_job,
                        node,
                        node_to_job,
                        node_dependencies[node],
                        run_id,
                    )
                    futures.add(future)

                # If no more nodes left to run,
                # ensure the entire pipeline was run
                if not futures:
                    assert not todo_nodes, (todo_nodes, done_nodes, ready, done)
                    break

    def _submit_job(
        self,
        node: Node,
        node_to_job: Dict[Node, str],
        node_dependencies: Set[Node],
        run_id: str,
    ) -> Node:
        self._logger.info("Submitting the job for node: %s", str(node))

        job_name = f"kedro_{run_id}_{node.name}".replace(".", "-")
        depends_on = [{"jobId": node_to_job[dep]} for dep in node_dependencies]
        command = ["kedro", "run", "--node", node.name]

        response = self._client.submit_job(
            jobName=job_name,
            jobQueue=self._job_queue,
            jobDefinition=self._job_definition,
            dependsOn=depends_on,
            containerOverrides={"command": command},
        )

        job_id = response["jobId"]
        node_to_job[node] = job_id

        _track_batch_job(job_id, self._client)  # make sure the job finishes

        return node


# Helper Function
def _track_batch_job(job_id: str, client: Any) -> None:
    """
    Continuously poll the Batch client for a job's status,
    given the job ID. If it ends in FAILED state, raise an exception
    and log the reason. Return if successful.
    """
    while True:
        # we don't want to bombard AWS with the requests
        # to not get throttled
        sleep(1.0)

        jobs = client.describe_jobs(jobs=[job_id])["jobs"]
        if not jobs:
            raise ValueError(f"Job ID {job_id} not found.")

        job = jobs[0]
        status = job["status"]

        if status == "FAILED":
            reason = job["statusReason"]
            raise Exception(
                f"Job {job_id} has failed with the following reason: {reason}"
            )

        if status == "SUCCEEDED":
            return
