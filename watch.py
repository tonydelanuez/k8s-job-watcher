import argparse
import time
from typing import List, Optional

from kubernetes import client, config, watch

config.load_kube_config()

# Create the Kubernetes client
batch_client = client.BatchV1Api()
core_client = client.CoreV1Api()
w = watch.Watch()


class TimeoutError(Exception):
    pass


class ActiveJobTimeout(TimeoutError):
    pass


class ContainerLogTimeout(TimeoutError):
    pass


def get_containers_for_job(
    job_object,
) -> List['kubernetes.client.models.v1_container.V1Container']:
    """Helper to fetch the container spec(s) from a V1Job"""
    return job_object.spec.template.spec.containers


def get_init_containers_for_job(
    job_object,
) -> Optional[List['kubernetes.client.models.v1_container.V1Container']]:
    """Helper to fetch the initContainer spec(s) from a V1Job"""
    return job_object.spec.template.spec.init_containers


def get_job_pod(namespace, job_name):
    """Returns the pod spawned by a Job. Helpful to refresh the pod object"""
    return core_client.list_namespaced_pod(
        namespace=namespace, label_selector='job-name={}'.format(job_name)
    ).items[0]


def wait_for_active_job(
    namespace, job_name, wait_secs=1, timeout_secs=100
) -> 'client.models.v1_job.V1Job':
    """Blocks until the job is active
    If timeout is exceeded, raises ActiveJobTimeout
    """
    timeout = time.time() + timeout_secs

    job = batch_client.read_namespaced_job(job_name, namespace)

    while not job.status.active:
        if time.time() > timeout:
            raise ActiveJobTimeout

        time.sleep(wait_secs)
        job = batch_client.read_namespaced_job(job_name, namespace)

    print(f'Job is active: {job.status.active}')
    return job


def fetch_container_status(pod, container_name, status_field):
    """Helper to fetch the container status attribute for a given container name within a Pod"""
    return list(
        filter(lambda x: x.name == container_name, getattr(pod.status, status_field))
    )[0]


def tail_container_logs(
    namespace,
    job_name,
    container='step',
    is_init_container=False,
    wait_secs=10,
    timeout_secs=30,
):
    """Tails the logs of the step container and print thems to stdout
    If timeout is exceeded, raised ContainerLogTimeout"""
    timeout = time.time() + timeout_secs

    # Need to check either container_statuses or init_container_statuses depending on container type
    status_field = (
        'init_container_statuses' if is_init_container else 'container_statuses'
    )
    pod = get_job_pod(namespace, job_name)
    container_status = fetch_container_status(pod, container, status_field)

    while not container_status.ready:
        if time.time() > timeout:
            raise ContainerLogTimeout

        print('waiting for container to start running')
        time.sleep(wait_secs)
        print('refreshing container status')
        container_status = fetch_container_status(
            get_job_pod(namespace, job_name), container, status_field
        )

    # Create the log stream for the pod
    # should probably use since=(time since container was started or infinity)
    for e in w.stream(
        core_client.read_namespaced_pod_log,
        name=pod.metadata.name,
        namespace=namespace,
        container=container,
        since_seconds=1000000,
    ):
        print(e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('job_name', type=str, help='Name of Kubernetes Job')
    parser.add_argument(
        '-n',
        '--namespace',
        help='Namespace for Kubernetes job',
        default='default',
        type=str,
    )
    parser.add_argument(
        '-c',
        '--containers',
        type=str,
        help='Name of container(s) to tail logs from',
        default=['step'],
        nargs='+',
    )
    parser.add_argument(
        '-i',
        '--init-logs',
        help='Print logs for all initContainers',
        action='store_true',
        default=False,
    )
    parsed_args = parser.parse_args()
    namespace = parsed_args.namespace
    job_name = parsed_args.job_name
    containers = parsed_args.containers
    init_logs = parsed_args.init_logs

    # Wait for the job to be active before fetching container logs
    job = wait_for_active_job(namespace, job_name)

    # Some Jobs have initContainers, log them too.
    job_init_containers = get_init_containers_for_job(job)
    job_containers = get_containers_for_job(job)

    if job_init_containers and init_logs:
        print('printing init container logs')
        for ic in job_init_containers:
            print(f'---- initContainer: {ic.name}')
            tail_container_logs(
                namespace, job_name, container=ic.name, is_init_container=True
            )

    if containers:
        # Only print logs for selected containers
        for c in filter(lambda c: c.name in containers, job_containers):
            tail_container_logs(namespace, job_name, container=c.name)


if __name__ == '__main__':
    main()
