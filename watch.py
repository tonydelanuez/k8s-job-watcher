import argparse
import pprint
import sys
import time
from typing import List, Optional

from kubernetes import client, config, watch

config.load_kube_config()

# Create the Kubernetes client
batch_client = client.BatchV1Api()
core_client = client.CoreV1Api()
events_client = client.EventsV1Api()

w = watch.Watch()


class TimeoutError(Exception):
    pass


class ActiveJobTimeout(TimeoutError):
    pass


class ContainerLogTimeout(TimeoutError):
    pass


class JobError(Exception):
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
        if self.job_object:
            return self.job_object
        return self._fetch_job()

    def _fetch_job(self):
        self.job_object = batch_client.read_namespaced_job(self.name, self.namespace)
        return self.job_object

    def wait_for_active_job(self) -> "client.models.v1_job.V1Job":
        """Blocks until the job is active
        If timeout is exceeded, raises ActiveJobTimeout
        """
        timeout = time.time() + self.job_active_wait_timeout

        job = self._fetch_job()

        if job.status.failed or job.status.succeeded:
            raise NoActiveJob("This job has already run!")

        while not job.status.active:
            if time.time() > timeout:
                raise ActiveJobTimeout()

            time.sleep(self.job_active_wait_secs)
            job = batch_client.read_namespaced_job(self.name, self.namespace)

        return job

    @property
    def pods(self):
        """Returns the pod(s) spawned by a Job. Helpful to refresh the pod object"""
        pods = core_client.list_namespaced_pod(
            namespace=self.namespace, label_selector="job-name={}".format(self.name)
        ).items

        if not pods:
            raise PodNotFound(
                f"Pod with label-selector job-name={self.name} not found!"
            )

        return pods

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
    ):
        """Prints the logs of the container to stdout, following new logs
        If timeout is exceeded, raises ContainerLogTimeout"""
        timeout = time.time() + timeout_secs

        pod = self.pods[0]
        check_pod_running(pod)

        container_status = fetch_container_status(
            pod, container, is_init_container=is_init_container
        )

        while not (container_status.state.running or container_status.state.terminated):
            if time.time() > timeout:
                raise ContainerLogTimeout()

            print("Waiting on container to be in running or terminated state")
            time.sleep(wait_secs)
            container_status = fetch_container_status(
                pod, container, is_init_container=is_init_container
            )

        # Create the log stream for the pod
        # should probably use since=(time since container was started or infinity)
        for e in w.stream(
            core_client.read_namespaced_pod_log,
            name=pod.metadata.name,
            namespace=self.namespace,
            container=container,
            since_seconds=1000000,
        ):
            print(e)

    def watch(self, watched_containers=[], log_init_containers=None):
        exit_statuses = {"init_containers": {}, "containers": {}}
        if self.init_containers and log_init_containers:
            print("printing init container logs")
            for ic in self.init_containers:
                print(f"---- initContainer: {ic.name}")
                self.print_container_logs(container=ic.name, is_init_container=True)
                exit_statuses["init_containers"][ic.name] = fetch_container_status(
                    self.pods[0], ic.name, is_init_container=True
                )

        if self.containers:
            # Only print logs for selected containers
            for c in filter(lambda c: c.name in watched_containers, self.containers):
                print(f"------ container logs for container {c.name} ------")
                self.print_container_logs(container=c.name)
                exit_statuses["containers"][c.name] = fetch_container_status(
                    self.pods[0], c.name, is_init_container=False
                )

        print('------ Container Statuses -----')
        # Print last known container states
        pprint.pprint(exit_statuses)

        exit_codes = {
            'init_containers': {
                init_container: status.state.terminated.exit_code for (init_container, status) in exit_statuses["init_containers"].items()
            },
            'containers': {
                container: status.state.terminated.exit_code for (container, status) in exit_statuses["containers"].items()
            }
        }
        print('------ Container Exit Codes-----')
        pprint.pprint(exit_codes)

        if watched_containers:
            non_zero = [ret_code for (container_name, ret_code) in exit_codes['containers'].items() if ret_code != 0 and container_name in watched_containers]
            if non_zero:
                return non_zero.pop(0)
        else:
            non_zero = [ret_code for (_, ret_code) in exit_codes['containers'].items() if ret_code != 0]
            if non_zero:
                return non_zero.pop(0)
        return 0


def check_pod_running(pod):
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


def fetch_container_status(pod, container_name, is_init_container=False):
    """Helper to fetch the container status attribute for a given container name within a Pod"""
    # Need to check either container_statuses or init_container_statuses depending on container type
    status_field = (
        "init_container_statuses" if is_init_container else "container_statuses"
    )
    if not pod.status:
        raise PodUnavailable()

    return list(
        filter(lambda x: x.name == container_name, getattr(pod.status, status_field))
    )[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job_name", type=str, help="Name of Kubernetes Job")
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
        help="Print logs for all initContainers",
        action="store_true",
        default=False,
    )
    parsed_args = parser.parse_args()
    namespace = parsed_args.namespace
    job_name = parsed_args.job_name
    containers = parsed_args.containers
    init_logs = parsed_args.init_logs

    watcher = JobWatcher(namespace, job_name)
    return_code = watcher.watch(watched_containers=containers, log_init_containers=init_logs)
    sys.exit(return_code)



if __name__ == "__main__":
    main()
