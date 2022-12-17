import argparse
import time
import threading

from typing import List, Optional
from kubernetes import client, config, watch

config.load_kube_config()

# Create the Kubernetes client
batch_client = client.BatchV1Api()
core_client = client.CoreV1Api()
w = watch.Watch()

def get_containers_for_job(job_object) -> List['kubernetes.client.models.v1_container.V1Container']:
    return job_object.spec.template.spec.containers

def get_init_containers_for_job(job_object) -> Optional[List['kubernetes.client.models.v1_container.V1Container']]:
    return job_object.spec.template.spec.init_containers

# Function to watch the status of the job and print updates
def watch_job_status(v1, namespace, job_name):
  w = v1.read_namespaced_job_status(job_name, namespace, watch=True)
  for event in w:
    status = event['status']['conditions'][-1]['type']
    print(f"Job status: {status}")
    if status == "Failed":
      print("Job failed. Exiting.")
      exit(1)


def get_job_pod(namespace, job_name):
    """Returns the pod spawned by a Job. Helpful to refresh the pod object"""
    return core_client.list_namespaced_pod(namespace=namespace, label_selector='job-name={}'.format(job_name)).items[0]

def wait_for_active_job(namespace, job_name) -> 'kubernetes.client.models.v1_job.V1Job':
  """Blocks until the job is active"""
  job = batch_client.read_namespaced_job(job_name, namespace)
  while not job.status.active:
    time.sleep(1)
    job = batch_client.read_namespaced_job(job_name, namespace)

  print(f'Job is active: {job.status.active}')
  return job


def fetch_container_status(pod, container_name, status_field):
    return list(filter(lambda x: x.name == container_name, getattr(pod.status, status_field)))[0]

# Function to tail the logs of the step container and print them
def tail_container_logs(namespace, job_name, container="step", is_init_container=False):
  # Get the name of the pod for the job
  # pod_name = job.status.active[0].name
  status_field = 'init_container_statuses' if is_init_container else 'container_statuses'
  pod = get_job_pod(namespace, job_name)
  container_status = fetch_container_status(pod, container, status_field)

  while not container_status.ready:
    print('waiting for container to start running')
    time.sleep(1)
    print('refreshing container status')
    container_status = fetch_container_status(get_job_pod(namespace, job_name), container, status_field)
  
  # Create the log stream for the pod
  # should probably use since=(time since container was started or infinity)
  for e in w.stream(core_client.read_namespaced_pod_log, name=pod.metadata.name, namespace=namespace, container=container, since_seconds=1000000):
      print(e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'job_name', type=str, help='Name of Kubernetes Job'
    )
    parser.add_argument(
        '-n',
        '--namespace',
        help='Namespace for Kubernetes job',
        default='default',
        type=str
    )
    parser.add_argument(
        '-c',
        '--containers',
        type=str,
        help='Name of container(s) to tail logs from',
        default=['step'],
        nargs='+'
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

    # Start the job status watcher in a separate thread
    threading.Thread(target=watch_job_status, args=(batch_client, namespace, job_name)).start()

    # Tail the container logs in the main thread
    job = wait_for_active_job(namespace, job_name)
    job_init_containers = get_init_containers_for_job(job)
    job_containers = get_containers_for_job(job)

    if job_init_containers and init_logs:
        print('printing init container logs')
        for ic in job_init_containers:
            print(f'---- initContainer: {ic.name}')
            tail_container_logs(namespace, job_name, container=ic.name, is_init_container=True)


    if containers:
        for c in filter(lambda c: c.name in containers, job_containers):
          tail_container_logs(namespace, job_name, container=c.name)

if __name__ == '__main__':
    main()