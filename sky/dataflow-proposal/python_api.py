# ideal
preprocess = Task(name="preprocess", run="python3 preprocess.py")
train_a = Task(name="train_a", run="python3 train_a.py", depends_on=[preprocess])
train_b = Task(name="train_b", run="python3 train_a.py", depends_on=[preprocess])

# realistic
preprocess = Task(name="preprocess", run="python3 preprocess.py")
train_a = Task(name="train_a", run="python3 train_a.py", depends_on=["preprocess:/data/train_a"])
train_b = Task(name="train_b", run="python3 train_a.py", depends_on=["preprocess:/data/train_b"])

