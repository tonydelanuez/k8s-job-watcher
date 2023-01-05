# k8s Job watcher

CLI for watching the logs and container exit statuses from Kubernetes [Job](https://kubernetes.io/docs/concepts/workloads/controllers/job/) resources.

Uses the the [official Python client for Kubernetes](https://github.com/kubernetes-client/python) to make requests to the k8s API server. Tails logs to stdout pretty prints the exitStatuses of all watched containers.

```bash
usage: watch.py [-h] [-n NAMESPACE] [-c CONTAINERS [CONTAINERS ...]] [-i] job_name

positional arguments:
  job_name              Name of Kubernetes Job to watch

options:
  -h, --help            show this help message and exit
  -n NAMESPACE, --namespace NAMESPACE
                        Namespace for Kubernetes job
  -c CONTAINERS [CONTAINERS ...], --containers CONTAINERS [CONTAINERS ...]
                        Name of container(s) to tail logs from
  -i, --init-logs       Follow logs for all initContainers
```

Example Job manifest:

```python
apiVersion: batch/v1
kind: Job
metadata:
  name: pi
spec:
  template:
    spec:
      initContainers:
      - name: bootstrap
        image: chentex/random-logger
        args: ["10", "40", "60"]
      containers:
      - name: step
        image: chentex/random-logger
        args: ["100", "400", "100"]
      restartPolicy: Never
  backoffLimit: 4
```

Watching the Job named “pi” in the “default” namespace, specifically the “step” container, tailing initContainer logs:

```python
python watch.py pi -c step -n default -i

printing init container logs
---- initContainer: bootstrap
Waiting on container to be in running or terminated state. Current state: {'running': None,
 'terminated': None,
 'waiting': {'message': None, 'reason': 'PodInitializing'}}
2023-01-05T02:25:36+0000 ERROR An error is usually an exception that has been caught and not handled.
2023-01-05T02:25:37+0000 DEBUG This is a debug log that shows a log that can be ignored.
...
2023-01-05T02:25:38+0000 WARN A warning that should be ignored is usually at this level and should be actionable.
------ container logs for container step ------
2023-01-05T02:25:40+0000 INFO This is less important than debug log and is often used to provide context in the current task.
...
2023-01-05T02:26:03+0000 ERROR An error is usually an exception that has been caught and not handled.
2023-01-05T02:26:03+0000 DEBUG This is a debug log that shows a log that can be ignored.
------ Container Statuses ------
{'containers': {'step': {'container_id': 'containerd://bb468d3e4369e16b709a235236c6a3eb87a2516fc3d04ea21cbc9241ccfd4a7f',
 'image': 'docker.io/chentex/random-logger:latest',
 'image_id': 'docker.io/chentex/random-logger@sha256:7cae589926ce903c65a853c22b4e2923211cc19966ac8f8cc533bbcff335ca39',
 'last_state': {'running': None, 'terminated': None, 'waiting': None},
 'name': 'step',
 'ready': False,
 'restart_count': 0,
 'started': False,
 'state': {'running': None,
           'terminated': {'container_id': 'containerd://bb468d3e4369e16b709a235236c6a3eb87a2516fc3d04ea21cbc9241ccfd4a7f',
                          'exit_code': 0,
                          'finished_at': datetime.datetime(2023, 1, 5, 2, 26, 3, tzinfo=tzutc()),
                          'message': None,
                          'reason': 'Completed',
                          'signal': None,
                          'started_at': datetime.datetime(2023, 1, 5, 2, 25, 40, tzinfo=tzutc())},
           'waiting': None}}},
 'init_containers': {'bootstrap': {'container_id': 'containerd://dcff2d9708112620734a198cff0bc052545b43f6b13fab2772c4847438fdf3e9',
 'image': 'docker.io/chentex/random-logger:latest',
 'image_id': 'docker.io/chentex/random-logger@sha256:7cae589926ce903c65a853c22b4e2923211cc19966ac8f8cc533bbcff335ca39',
 'last_state': {'running': None, 'terminated': None, 'waiting': None},
 'name': 'bootstrap',
 'ready': True,
 'restart_count': 0,
 'started': None,
 'state': {'running': None,
           'terminated': {'container_id': 'containerd://dcff2d9708112620734a198cff0bc052545b43f6b13fab2772c4847438fdf3e9',
                          'exit_code': 0,
                          'finished_at': datetime.datetime(2023, 1, 5, 2, 25, 38, tzinfo=tzutc()),
                          'message': None,
                          'reason': 'Completed',
                          'signal': None,
                          'started_at': datetime.datetime(2023, 1, 5, 2, 25, 36, tzinfo=tzutc())},
           'waiting': None}}}}
------ Container Exit Codes ------
{'containers': {'step': 0}, 'init_containers': {'bootstrap': 0}}
```
