# Background
see: https://github.com/skypilot-org/skypilot/discussions/4226#discussion-7399363

# Proposed Approaches
## Approach 0: The Ideal
**Claim**: Users understand dependencies, not necessarily dags.
So the ideal dag has **implicit** edges.
```python
@Task
def preprocess():
  # preprocessing ...

@Task
def train_a():
  preprocess()
  # do training

@Task
def train_b():
  preprocess()
  # do training

launch(train_b)
launch(train_a)
```
Other examples of this approach, [regent/legion](https://regent-lang.org/tutorial/01_tasks_and_futures/), [parsl](https://parsl.readthedocs.io/en/stable/userguide/joins.html), [dask](https://distributed.dask.org/en/stable/task-launch.html)

### Pros:
* Implicit dag definition
* Each function is an encoding of the DAG
### Cons:
* Costly to implement
## Approach 1: Reasonable
Inspired by Approach 2 in [andy's proposal](https://github.com/skypilot-org/skypilot/discussions/4226)

In yaml dependencies should be specified within the task defintion
```yaml
name: preprocess

resources:
  cloud: aws

setup: |
  pip install -r requirements.txt

run: |
  python3 preprocess.py
```

```yaml
name: train_a

resources:
  cloud: aws

setup: |
  pip install -r requirements.txt

dependson:
  preprocess:
    - train.csv

run: |
  python3 train_a.py
```
```yaml
name: train_b

resources:
  cloud: aws

setup: |
  pip install -r requirements.txt

dependson:
  preprocess:
    - train.csv

run: |
  python3 train_b.py
```

Similarly, in the Python API, task dependencies should be specified within the task definition
```python
preprocess = Task(name="preprocess", run="python3 preprocess.py")
train_a = Task(name="train_a", run="python3 train_a.py", depends_on=["preprocess:/data/train_a"])
train_b = Task(name="train_b", run="python3 train_a.py", depends_on=["preprocess:/data/train_b"])
```
