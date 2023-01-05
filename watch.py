import argparse
import pprint
import sys
import time
from functools import partial
from threading import Thread
from typing import List, Optional

from kubernetes import client, config, watch

config.load_kube_config()

# CoreV1API is the "default" k8s API with access to pods, namespaces, etc.
core_client = client.CoreV1Api()
# BatchV1Api provides access to Jobs https://kubernetes.io/docs/concepts/workloads/controllers/job/
batch_client = client.BatchV1Api()
# Watch supports keeping a request open via long-polling
watch_client = watch.Watch()


class WatcherException(Exception):
    pass


class TimeoutError(WatcherException):
    pass


class ActiveJobTimeout(TimeoutError):
    pass


class ContainerLogTimeout(TimeoutError):
    pass


class JobError(WatcherException):
    pass


class NoActiveJob(JobError):
    pass


class PodNotFound(JobError):
    pass


class PodUnavailable(JobError):
    pass


class JobWatcher:
    def __init__(
        self,
        namespace,
        name,
        job_active_wait_secs=1,
        job_active_wait_timeout=100,
        default_container="default",
    ):
        self.namespace = namespace
        self.name = name
        self.job_active_wait_secs = job_active_wait_secs
        self.job_active_wait_timeout = job_active_wait_timeout
        self.job_object = None
        self.default_container = default_container

    @property
    def job(self):
        """Singleton for the associated v1_job.V1Job k8s API object"""
        if self.job_object:
            return self.job_object
        self.job_object = batch_client.read_namespaced_job(self.name, self.namespace)
        return self.job_object
        return self._fetch_job()

    def wait_for_active_job(self) -> "client.models.v1_job.V1Job":
        """Blocks until the job is active
        If timeout is exceeded, raises ActiveJobTimeout
        """
        timeout = time.time() + self.job_active_wait_timeout

        job = self.job

        if job.status.failed or job.status.succeeded:
            raise NoActiveJob("This job has already run!")

        # Keep refreshing the job with calls to the k8s API
        # until the timeout is reached. Occasionally a job
        # will stay in the pending state until there are
        # more resources available in the cluster
        while not job.status.active:
            if time.time() > timeout:
                raise ActiveJobTimeout()

            time.sleep(self.job_active_wait_secs)
            job = batch_client.read_namespaced_job(self.name, self.namespace)

        return job

    @property
    def pods(self):
        """Returns the pod(s) spawned by a Job.
        Often called to refresh the pod object while waiting
        for a Pod's status to go from Pending to Active.

        raises PodNotFound if a pod with the label-selector job-name=<Job.name>
        cannot be found
        """
        pods = core_client.list_namespaced_pod(
            namespace=self.namespace, label_selector="job-name={}".format(self.name)
        ).items

        if not pods:
            raise PodNotFound(
                f"Pod with label-selector job-name={self.name} not found!"
            )

        return pods

    def _refresh_pods(self):
        """Fetch the Pod object from the kubernetes API. Called when waiting on a status change."""
        return core_client.list_namespaced_pod(
            namespace=self.namespace, label_selector="job-name={}".format(self.name)
        ).items

    @property
    def containers(self) -> List["kubernetes.client.models.v1_container.V1Container"]:
        """Helper to fetch the container spec(s) from a V1Job"""
        return self.job.spec.template.spec.containers

    @property
    def init_containers(
        self,
    ) -> Optional[List["kubernetes.client.models.v1_container.V1Container"]]:
        """Helper to fetch the initContainer spec(s) from a V1Job"""
        return self.job.spec.template.spec.init_containers

    def print_container_logs(
        self,
        container="step",
        is_init_container=False,
        wait_secs=10,
        timeout_secs=30,
        pod_availability_retries=10,
    ):
        """Prints the logs of the container to stdout then follows new logs until the container is terminated

        raises ContainerLogTimeout if logs cannot be fetched within timeout"""
        timeout = time.time() + timeout_secs

        pod = self.pods[0]
        # check_pod_running(pod)

        container_status = None
        retries = pod_availability_retries
        while not container_status and retries >= 0:
            try:
                container_status = self.fetch_container_status(
                    container, is_init_container=is_init_container
                )
            except PodUnavailable as e:
                if retries == 0:
                    raise e
                retries -= 1
                time.sleep(1)

        # We only want to tail logs when the container is running and exit after the container has terminated.
        # The container can be in a pending state waiting for resources or for an
        # initContainer to run.
        exit_thread = Thread(
            target=wait_for_termination, args=[self, container, is_init_container]
        )
        exit_thread.start()
        while not (container_status.state.running or container_status.state.terminated):
            if time.time() > timeout:
                raise ContainerLogTimeout(
                    "Timeout exceeded, container did not enter running or terminated state"
                )

            print(
                f"Waiting on container to be in running or terminated state. Current state: {container_status.state}"
            )
            time.sleep(wait_secs)
            container_status = self.fetch_container_status(
                container, is_init_container=is_init_container
            )

        for e in watch_client.stream(
            core_client.read_namespaced_pod_log,
            name=pod.metadata.name,
            namespace=self.namespace,
            container=container,
            since_seconds=1000000,
        ):
            print(e)
        exit_thread.join()

    def fetch_container_status(self, container_name, is_init_container=False):
        """Helper to fetch the container status attribute for a given container name within a Pod"""
        pod = self._refresh_pods()[0]
        status_field = (
            "init_container_statuses" if is_init_container else "container_statuses"
        )
        if not pod.status:
            raise PodUnavailable("Pod was unavailable")

        return list(
            filter(
                lambda x: x.name == container_name, getattr(pod.status, status_field)
            )
        )[0]

    def watch(self, watched_containers=[], log_init_containers=None) -> int:
        """
        Watch all specified Containers and initContainers scheduled in a Pod by a Job.
        Returns the first nonzero exit code found from a container.

        Containers are iterated through in the order they are specified in their manifests
        as kubernetes does not guarantee a start order (other than initContainers preceeding Containers)
        """
        exit_statuses = {"init_containers": {}, "containers": {}}
        if self.init_containers and log_init_containers:
            print("printing init container logs")
            for ic in self.init_containers:
                print(f"---- initContainer: {ic.name}")
                self.print_container_logs(container=ic.name, is_init_container=True)
                exit_statuses["init_containers"][ic.name] = self.fetch_container_status(
                    ic.name,
                    is_init_container=True,
                )

        if self.containers:
            # Only print logs for selected containers
            for c in filter(lambda c: c.name in watched_containers, self.containers):
                print(f"------ container logs for container {c.name} ------")
                self.print_container_logs(container=c.name)
                exit_statuses["containers"][c.name] = self.fetch_container_status(
                    c.name,
                    is_init_container=False,
                )

        print("------ Container Statuses ------")
        # Print last known container states
        pprint.pprint(exit_statuses)

        exit_codes = {
            "init_containers": {
                init_container: status.state.terminated.exit_code
                for (init_container, status) in exit_statuses["init_containers"].items()
            },
            "containers": {
                container: status.state.terminated.exit_code
                for (container, status) in exit_statuses["containers"].items()
            },
        }
        print("------ Container Exit Codes ------")
        pprint.pprint(exit_codes)

        if watched_containers:
            non_zero = [
                ret_code
                for (container_name, ret_code) in exit_codes["containers"].items()
                if ret_code != 0 and container_name in watched_containers
            ]
            if non_zero:
                return non_zero.pop(0)
        else:
            non_zero = [
                ret_code
                for (_, ret_code) in exit_codes["containers"].items()
                if ret_code != 0
            ]
            if non_zero:
                return non_zero.pop(0)
        return 0


def check_pod_running(pod):
    """Rasies if a pod is not available (Unschedulable or stuck in Unknown, Pending phases"""
    last_condition = pod.status.conditions.pop()
    # TODO: Update this with more failure scenarios, potentially wait on pending?
    if last_condition.reason in ["Unschedulable"]:
        raise PodUnavailable(
            f"Pod could not be scheduled. Message: {last_condition.message}"
        )

    # TODO: Do we care if the pod has already succeeded?
    # Should probably wait if the pod is pending.
    if pod.status.phase in ["Unknown", "Pending"]:
        raise PodUnavailable(f"Pod is not running. Pod phase: {pod.status.phase}")


def wait_for_termination(watcher, container, is_init_container=False):
    """Wait until a container has exited"""
    while True:
        container_status = watcher.fetch_container_status(
            container, is_init_container=is_init_container
        )
        if container_status.state.terminated:
            break
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job_name", type=str, help="Name of Kubernetes Job to watch")
    parser.add_argument(
        "-n",
        "--namespace",
        help="Namespace for Kubernetes job",
        default="default",
        type=str,
    )
    parser.add_argument(
        "-c",
        "--containers",
        type=str,
        help="Name of container(s) to tail logs from",
        default=["step"],
        nargs="+",
    )
    parser.add_argument(
        "-i",
        "--init-logs",
        help="Follow logs for all initContainers",
        action="store_true",
        default=False,
    )
    parsed_args = parser.parse_args()
    namespace = parsed_args.namespace
    job_name = parsed_args.job_name
    containers = parsed_args.containers
    init_logs = parsed_args.init_logs

    watcher = JobWatcher(namespace, job_name)
    try:
        return_code = watcher.watch(
            watched_containers=containers, log_init_containers=init_logs
        )
    except WatcherException as e:
        print(f"Error occurred while watching job: {e}")
        sys.exit(1)
    else:
        sys.exit(return_code)


if __name__ == "__main__":
    main()
